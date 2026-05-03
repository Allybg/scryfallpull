"""
Ingest Card Kingdom price list into Supabase.

Fetches the CK v2 pricelist and upserts into `ck_prices`.
scryfall_id is provided directly by the CK API.

Usage:
    python ck_ingest.py
"""

import os
import time
from pathlib import Path

import httpx
import psycopg
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    raise RuntimeError("SUPABASE_DB_URL not set — check your .env file")

CK_PRICELIST_URL = "https://api.cardkingdom.com/api/v2/pricelist"
BATCH_SIZE = 1000
HEADERS = {"User-Agent": "bzaar-pipeline/1.0 (github.com/DaichiOS/bzaar)"}

UPSERT_SQL = """
INSERT INTO ck_prices (
    ck_id, sku, name, variation, edition,
    is_foil, price_retail, price_buy,
    qty_retail, qty_buying,
    nm_price, nm_qty, ex_price, ex_qty,
    vg_price, vg_qty, g_price, g_qty,
    scryfall_id, snapshot_date, snapshot_at
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s, %s, %s,
    %s, %s, %s, %s,
    %s, CURRENT_DATE, NOW()
)
ON CONFLICT (ck_id, is_foil, snapshot_date)
DO UPDATE SET
    price_retail = EXCLUDED.price_retail,
    price_buy    = EXCLUDED.price_buy,
    qty_retail   = EXCLUDED.qty_retail,
    qty_buying   = EXCLUDED.qty_buying,
    nm_price     = EXCLUDED.nm_price,
    nm_qty       = EXCLUDED.nm_qty,
    ex_price     = EXCLUDED.ex_price,
    ex_qty       = EXCLUDED.ex_qty,
    vg_price     = EXCLUDED.vg_price,
    vg_qty       = EXCLUDED.vg_qty,
    g_price      = EXCLUDED.g_price,
    g_qty        = EXCLUDED.g_qty,
    scryfall_id  = EXCLUDED.scryfall_id
"""


def fetch_pricelist() -> list[dict]:
    print("Fetching Card Kingdom pricelist...")
    resp = httpx.get(CK_PRICELIST_URL, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    payload = resp.json()
    items = payload.get("data", payload) if isinstance(payload, dict) else payload
    print(f"Fetched {len(items):,} items")
    return items


def parse_price(val) -> float | None:
    try:
        f = float(val)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def build_rows(items: list[dict]) -> list[tuple]:
    rows = []
    for item in items:
        ck_id = item.get("id")
        if ck_id is None:
            continue

        is_foil = item.get("is_foil", "false") == "true"
        cv = item.get("condition_values") or {}

        rows.append(
            (
                ck_id,
                item.get("sku"),
                item.get("name", ""),
                item.get("variation", ""),
                item.get("edition", ""),
                is_foil,
                parse_price(item.get("price_retail")),
                parse_price(item.get("price_buy")),
                item.get("qty_retail"),
                item.get("qty_buying"),
                parse_price(cv.get("nm_price")),
                cv.get("nm_qty"),
                parse_price(cv.get("ex_price")),
                cv.get("ex_qty"),
                parse_price(cv.get("vg_price")),
                cv.get("vg_qty"),
                parse_price(cv.get("g_price")),
                cv.get("g_qty"),
                item.get("scryfall_id"),
            )
        )
    return rows


def upsert_prices(rows: list[tuple], conn):
    total = 0
    start = time.time()

    with conn.cursor() as cur:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            for row in batch:
                cur.execute(UPSERT_SQL, row)
            conn.commit()
            total += len(batch)
            elapsed = time.time() - start
            print(
                f"\r  {total:,}/{len(rows):,} rows upserted ({elapsed:.0f}s)",
                end="",
                flush=True,
            )

    elapsed = time.time() - start
    print(f"\nDone. {total:,} CK price rows upserted in {elapsed:.1f}s")


def main():
    items = fetch_pricelist()
    rows = build_rows(items)
    print(f"Parsed {len(rows):,} price rows")

    print("Connecting to Supabase...")
    with psycopg.connect(DB_URL, prepare_threshold=None) as conn:
        upsert_prices(rows, conn)


if __name__ == "__main__":
    main()
