"""
Append-only Card Kingdom price ingest.

Same as ck_ingest.py, but INSERTs new rows instead of upserting. Use this
to build a time-series price history (every run = new snapshot rows).

NOTE: requires the unique index `ck_prices_daily_uniq` to be dropped if you
want to run multiple times per day:
    DROP INDEX IF EXISTS ck_prices_daily_uniq;

Usage:
    python ck_ingest_append.py
"""

import datetime
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
HEADERS = {"User-Agent": "bzaar-pipeline/1.0 (github.com/DaichiOS/bzaar)"}


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


def append_prices(rows: list[tuple], conn):
    start = time.time()
    today = datetime.date.today()
    now = datetime.datetime.now(datetime.timezone.utc)

    with conn.cursor() as cur:
        print(f"  Streaming {len(rows):,} rows via COPY (append-only)...")
        with cur.copy("""
            COPY ck_prices (
                ck_id, sku, name, variation, edition,
                is_foil, price_retail, price_buy,
                qty_retail, qty_buying,
                nm_price, nm_qty, ex_price, ex_qty,
                vg_price, vg_qty, g_price, g_qty,
                scryfall_id, snapshot_date, snapshot_at
            ) FROM STDIN
        """) as copy:
            for row in rows:
                copy.write_row((*row, today, now))

        conn.commit()

    elapsed = time.time() - start
    print(f"Done. {len(rows):,} CK price rows inserted in {elapsed:.1f}s")


def main():
    items = fetch_pricelist()
    rows = build_rows(items)
    print(f"Parsed {len(rows):,} price rows")

    print("Connecting to Supabase...")
    with psycopg.connect(DB_URL, prepare_threshold=None) as conn:
        append_prices(rows, conn)


if __name__ == "__main__":
    main()
