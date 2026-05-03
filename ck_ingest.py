"""
Ingest Card Kingdom price list into Supabase.

Fetches the CK v2 pricelist and upserts into `ck_prices`, linking each row
to the `cards` table via card_kingdom_id where possible.

Usage:
    python ck_ingest.py
"""

import json
import os
import sys
import time

import httpx
import psycopg
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.environ["SUPABASE_DB_URL"]
CK_PRICELIST_URL = "https://api.cardkingdom.com/api/v2/pricelist"
BATCH_SIZE = 500
HEADERS = {"User-Agent": "bzaar-pipeline/1.0 (github.com/DaichiOS/bzaar)"}

UPSERT_SQL = """
INSERT INTO ck_prices (
    ck_id, name, edition, edition_code,
    is_foil, buy_price, sell_price,
    scryfall_id, snapshot_date, snapshot_at
)
VALUES (
    %s, %s, %s, %s,
    %s, %s, %s,
    (SELECT id FROM cards
     WHERE (NOT %s AND card_kingdom_id = %s)
        OR (%s AND card_kingdom_foil_id = %s)
     LIMIT 1),
    CURRENT_DATE, NOW()
)
ON CONFLICT (ck_id, is_foil, snapshot_date)
DO UPDATE SET
    buy_price   = EXCLUDED.buy_price,
    sell_price  = EXCLUDED.sell_price,
    scryfall_id = EXCLUDED.scryfall_id
"""


def fetch_pricelist() -> list[dict]:
    print(f"Fetching Card Kingdom pricelist...")
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
    """
    CK v2 pricelist items have fields like:
      id, name, edition, edition_code, is_foil, buy_price, sell_price, ...
    Non-foil and foil variants are separate line items with different ids.
    """
    rows = []
    for item in items:
        ck_id = item.get("id")
        if ck_id is None:
            continue

        is_foil = bool(item.get("is_foil", 0))
        buy_price = parse_price(
            item.get("buy_price") or item.get("prices", {}).get("buy")
            if isinstance(item.get("prices"), dict)
            else item.get("buy_price")
        )
        sell_price = parse_price(
            item.get("sell_price") or item.get("prices", {}).get("sell")
            if isinstance(item.get("prices"), dict)
            else item.get("sell_price")
        )

        rows.append(
            (
                ck_id,
                item.get("name", ""),
                item.get("edition") or item.get("set_name", ""),
                item.get("edition_code") or item.get("set", ""),
                is_foil,
                buy_price,
                sell_price,
                # subquery args: (NOT is_foil, ck_id, is_foil, ck_id)
                is_foil,
                ck_id,
                is_foil,
                ck_id,
            )
        )
    return rows


def upsert_prices(rows: list[tuple], conn):
    total = 0
    start = time.time()

    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        conn.cursor().executemany(UPSERT_SQL, batch)
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
