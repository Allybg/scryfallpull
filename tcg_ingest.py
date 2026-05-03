"""
TCGPlayer price ingest via mp-search-api (no API key required).

Two-tier fetch:
  Tier 1 — Product sweep: paginates all MTG products via the search endpoint,
            capturing market/median/low prices per product.
  Tier 2 — Listings sweep: fetches every individual seller listing per product,
            capturing per-condition prices, seller info, and quantities.

Run Tier 1 alone for a fast daily market-price snapshot (~minutes).
Run Tier 2 after Tier 1 for full listing depth (~hours, rate-limit aware).

Usage:
    python tcg_ingest.py            # Tier 1 only
    python tcg_ingest.py --listings # Tier 1 + Tier 2

Requires:
    pip install curl_cffi psycopg python-dotenv
"""

import argparse
import asyncio
import datetime
import math
import os
import time
from pathlib import Path

import psycopg
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    raise RuntimeError("SUPABASE_DB_URL not set — check your .env file")

SEARCH_URL = "https://mp-search-api.tcgplayer.com/v1/search/request"
LISTINGS_URL = "https://mp-search-api.tcgplayer.com/v1/product/{product_id}/listings"
LISTINGS_PARAMS = {"mpfev": "5106"}

PAGE_SIZE = 24
LISTINGS_PAGE_SIZE = 100  # try higher than the observed 10; reduce if errors
SEARCH_CONCURRENCY = 8
LISTINGS_CONCURRENCY = 20  # reduce to 5-10 if you see 429s

SEARCH_PAYLOAD = {
    "algorithm": "sales_dismax",
    "from": 0,
    "size": PAGE_SIZE,
    "filters": {
        "term": {"productLineName": ["magic"]},
        "range": {},
        "match": {},
    },
    "listingSearch": {
        "context": {"cart": {"packages": {}}},
        "filters": {
            "term": {"sellerStatus": "Live", "channelId": 0, "language": ["English"]},
            "range": {"quantity": {"gte": 1}},
            "exclude": {"channelExclusion": 0},
        },
    },
    "context": {"shippingCountry": "US", "cart": {"packages": {}}, "userProfile": {}},
    "settings": {"useFuzzySearch": True, "didYouMean": {}},
    "sort": {},
}

LISTINGS_PAYLOAD = {
    "filters": {
        "term": {"sellerStatus": "Live", "channelId": 0, "language": ["English"]},
        "range": {"quantity": {"gte": 1}},
        "exclude": {"channelExclusion": 0},
    },
    "from": 0,
    "size": LISTINGS_PAGE_SIZE,
    "sort": {"field": "price+shipping", "order": "asc"},
    "context": {"shippingCountry": "US", "cart": {"packages": {}}},
    "aggregations": [],
}

HEADERS = {
    "Content-Type": "application/json",
    "Referer": "https://www.tcgplayer.com/",
}


def parse_price(val) -> float | None:
    try:
        f = float(val)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Tier 1 — product sweep (per-set to avoid Elasticsearch 10k offset limit)
# ---------------------------------------------------------------------------


def _set_payload(offset: int, set_name: str) -> dict:
    term = {**SEARCH_PAYLOAD["filters"]["term"]}
    if set_name:
        term["setName"] = [set_name]
    return {
        **SEARCH_PAYLOAD,
        "from": offset,
        "filters": {**SEARCH_PAYLOAD["filters"], "term": term},
    }


async def _fetch_page(
    session: AsyncSession,
    offset: int,
    set_name: str,
    sem: asyncio.Semaphore,
    retries: int = 3,
) -> dict:
    last_exc: Exception = RuntimeError("no attempts made")
    async with sem:
        for attempt in range(retries):
            try:
                resp = await session.post(
                    SEARCH_URL,
                    params={"q": "", "isList": "false"},
                    json=_set_payload(offset, set_name),
                    headers=HEADERS,
                    timeout=60,
                )
                resp.raise_for_status()
                return resp.json().get("results", [{}])[0]
            except Exception as e:
                last_exc = e
                if attempt < retries - 1:
                    wait = 2**attempt  # 1s, 2s
                    print(
                        f"  [retry {attempt + 1}/{retries - 1} set={set_name} offset={offset}] {e} — waiting {wait}s"
                    )
                    await asyncio.sleep(wait)
    raise last_exc


async def fetch_set_products(
    session: AsyncSession, set_name: str, sem: asyncio.Semaphore
) -> list[dict]:
    first = await _fetch_page(session, 0, set_name, sem)
    total = first.get("totalResults", 0)
    all_products = list(first.get("results", []))

    if total > PAGE_SIZE:
        offsets = [i * PAGE_SIZE for i in range(1, math.ceil(total / PAGE_SIZE))]
        pages = await asyncio.gather(
            *[_fetch_page(session, o, set_name, sem) for o in offsets],
            return_exceptions=True,
        )
        for r in pages:
            if isinstance(r, dict):
                all_products.extend(r.get("results", []))
            elif isinstance(r, BaseException):
                print(f"  [error set={set_name}] {r}")

    return all_products


async def fetch_all_products(conn) -> int:
    sem = asyncio.Semaphore(SEARCH_CONCURRENCY)
    total_written = 0

    async with AsyncSession(impersonate="chrome120") as session:
        # Lightweight request just to get set names from aggregations
        first = await _fetch_page(session, 0, "", sem)
        set_names = [
            s["urlValue"] for s in first.get("aggregations", {}).get("setName", [])
        ]
        print(f"Found {len(set_names):,} sets to fetch")

        pending = list(enumerate(set_names))  # (original_index, set_name)
        attempts: dict[str, int] = {}
        max_attempts = 5

        while pending:
            next_round: list[tuple[int, str]] = []
            for i, set_name in pending:
                attempts[set_name] = attempts.get(set_name, 0) + 1
                try:
                    products = await fetch_set_products(session, set_name, sem)
                    rows = build_product_rows(products)
                    upsert_products(rows, conn)
                    total_written += len(rows)
                    attempt_tag = (
                        f" (attempt {attempts[set_name]})"
                        if attempts[set_name] > 1
                        else ""
                    )
                    print(
                        f"  [{i + 1:,}/{len(set_names):,}] {set_name}: {len(rows)} products | {total_written:,} total in DB{attempt_tag}"
                    )
                except Exception as e:
                    if attempts[set_name] < max_attempts:
                        next_round.append((i, set_name))
                    else:
                        print(
                            f"  [gave up after {max_attempts} attempts] {set_name}: {e}"
                        )

            if next_round:
                wait = min(60, 5 * len(next_round))
                print(f"\n  {len(next_round)} sets failed — retrying in {wait}s...")
                await asyncio.sleep(wait)
            pending = next_round

    print(f"Product sweep complete: {total_written:,} total rows written")
    return total_written


def build_product_rows(products: list[dict]) -> list[tuple]:
    rows = []
    for p in products:
        tcg_id = p.get("productId")
        if tcg_id is None:
            continue
        rows.append(
            (
                int(tcg_id),
                p.get("productName", ""),
                p.get("setName", ""),
                int(p["setId"]) if p.get("setId") else None,
                p.get("rarityName", ""),
                p.get("foilOnly", False),
                p.get("productUrlName", ""),
                parse_price(p.get("marketPrice")),
                parse_price(p.get("lowestPrice")),
                parse_price(p.get("lowestPriceWithShipping")),
                parse_price(p.get("medianPrice")),
            )
        )
    return rows


def upsert_products(rows: list[tuple], conn):
    # Deduplicate by tcg_id — same product can appear on multiple search pages
    rows = list({row[0]: row for row in rows}.values())

    start = time.time()
    today = datetime.date.today()
    now = datetime.datetime.now(datetime.timezone.utc)

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tcg_products_staging")
        cur.execute(
            "CREATE TEMP TABLE tcg_products_staging (LIKE tcg_products INCLUDING DEFAULTS)"
        )

        print(f"  Streaming {len(rows):,} product rows via COPY...")
        with cur.copy("""
            COPY tcg_products_staging (
                tcg_id, name, set_name, set_id, rarity, foil_only, url_name,
                market_price, low_price, low_price_with_shipping, median_price,
                snapshot_date, snapshot_at
            ) FROM STDIN
        """) as copy:
            for row in rows:
                copy.write_row((*row, today, now))

        cur.execute("""
            INSERT INTO tcg_products (
                tcg_id, name, set_name, set_id, rarity, foil_only, url_name,
                market_price, low_price, low_price_with_shipping, median_price,
                snapshot_date, snapshot_at
            )
            SELECT
                tcg_id, name, set_name, set_id, rarity, foil_only, url_name,
                market_price, low_price, low_price_with_shipping, median_price,
                snapshot_date, snapshot_at
            FROM tcg_products_staging
            ON CONFLICT (tcg_id, snapshot_date) DO UPDATE SET
                market_price               = EXCLUDED.market_price,
                low_price                  = EXCLUDED.low_price,
                low_price_with_shipping    = EXCLUDED.low_price_with_shipping,
                median_price               = EXCLUDED.median_price
        """)
        cur.execute("DROP TABLE IF EXISTS tcg_products_staging")
        conn.commit()

    print(f"Products upserted in {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# Tier 2 — listings sweep
# ---------------------------------------------------------------------------


async def fetch_product_listings(
    session: AsyncSession, product_id: int, sem: asyncio.Semaphore
) -> list[dict]:
    all_listings: list[dict] = []
    offset = 0

    async with sem:
        while True:
            payload = {**LISTINGS_PAYLOAD, "from": offset}
            resp = await session.post(
                LISTINGS_URL.format(product_id=product_id),
                params=LISTINGS_PARAMS,
                json=payload,
                headers=HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            result = data.get("results", [{}])[0]
            page = result.get("results", [])
            all_listings.extend(page)

            total = result.get("totalResults", 0)
            offset += LISTINGS_PAGE_SIZE
            if offset >= total or not page:
                break

    return all_listings


async def fetch_all_listings(product_ids: list[int]) -> list[dict]:
    sem = asyncio.Semaphore(LISTINGS_CONCURRENCY)
    all_listings: list[dict] = []

    async with AsyncSession(impersonate="chrome120") as session:
        tasks = [fetch_product_listings(session, pid, sem) for pid in product_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"  Listings error for product {product_ids[i]}: {result}")
            continue
        all_listings.extend(result)

    print(f"Listings sweep complete: {len(all_listings):,} listings")
    return all_listings


def build_listing_rows(listings: list[dict]) -> list[tuple]:
    rows = []
    now = datetime.datetime.now(datetime.timezone.utc)
    for l in listings:
        listing_id = l.get("listingId")
        if listing_id is None:
            continue
        rows.append(
            (
                int(listing_id),
                int(l["productId"]),
                l.get("sellerId", ""),
                l.get("sellerName", ""),
                l.get("sellerRating"),
                l.get("sellerSales", ""),
                l.get("goldSeller", False),
                l.get("verifiedSeller", False),
                l.get("directSeller", False),
                l.get("condition", ""),
                int(l["conditionId"]) if l.get("conditionId") else None,
                l.get("printing", ""),
                l.get("language", ""),
                parse_price(l.get("sellerPrice")),
                parse_price(l.get("shippingPrice")),
                parse_price(l.get("score")),
                int(l["quantity"]) if l.get("quantity") else None,
                now,
            )
        )
    return rows


def upsert_listings(rows: list[tuple], conn):
    start = time.time()

    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS tcg_listings_staging")
        cur.execute(
            "CREATE TEMP TABLE tcg_listings_staging (LIKE tcg_listings INCLUDING DEFAULTS)"
        )

        print(f"  Streaming {len(rows):,} listing rows via COPY...")
        with cur.copy("""
            COPY tcg_listings_staging (
                listing_id, product_id, seller_id, seller_name,
                seller_rating, seller_sales, gold_seller, verified_seller, direct_seller,
                condition, condition_id, printing, language,
                price, shipping_price, ranked_price, quantity, snapshot_at
            ) FROM STDIN
        """) as copy:
            for row in rows:
                copy.write_row(row)

        cur.execute("""
            INSERT INTO tcg_listings (
                listing_id, product_id, seller_id, seller_name,
                seller_rating, seller_sales, gold_seller, verified_seller, direct_seller,
                condition, condition_id, printing, language,
                price, shipping_price, ranked_price, quantity, snapshot_at
            )
            SELECT
                listing_id, product_id, seller_id, seller_name,
                seller_rating, seller_sales, gold_seller, verified_seller, direct_seller,
                condition, condition_id, printing, language,
                price, shipping_price, ranked_price, quantity, snapshot_at
            FROM tcg_listings_staging
            ON CONFLICT (listing_id) DO UPDATE SET
                price          = EXCLUDED.price,
                shipping_price = EXCLUDED.shipping_price,
                ranked_price   = EXCLUDED.ranked_price,
                quantity       = EXCLUDED.quantity,
                snapshot_at    = EXCLUDED.snapshot_at
        """)
        cur.execute("DROP TABLE IF EXISTS tcg_listings_staging")
        conn.commit()

    print(f"Listings upserted in {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--listings", action="store_true", help="Also fetch per-product listings (slow)"
    )
    args = parser.parse_args()

    assert DB_URL
    print("Connecting to Supabase...")
    with psycopg.connect(DB_URL, prepare_threshold=None) as conn:
        print("=== Tier 1: product sweep ===")
        asyncio.run(fetch_all_products(conn))

        if args.listings:
            print("\n=== Tier 2: listings sweep ===")
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT tcg_id FROM tcg_products")
                product_ids = [row[0] for row in cur.fetchall()]
            print(f"Fetching listings for {len(product_ids):,} products...")
            listings = asyncio.run(fetch_all_listings(product_ids))
            listing_rows = build_listing_rows(listings)
            upsert_listings(listing_rows, conn)


if __name__ == "__main__":
    main()
