"""
Ingest Scryfall default_cards bulk data into Supabase.

Downloads the latest default_cards JSON from Scryfall (English, one entry per
printing, ~150MB) and upserts all cards into the `cards` table.

Usage:
    python scryfall_ingest.py
    python scryfall_ingest.py --file ../all-cards-20260501092258.json
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import httpx
import ijson
import psycopg
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

DB_URL = os.environ.get("SUPABASE_DB_URL")
if not DB_URL:
    raise RuntimeError("SUPABASE_DB_URL not set — check your .env file")
SCRYFALL_BULK_API = "https://api.scryfall.com/bulk-data"
BULK_TYPE = "all_cards"
BATCH_SIZE = 500
HEADERS = {"User-Agent": "bzaar-pipeline/1.0 (github.com/DaichiOS/bzaar)"}

UPSERT_SQL = """
INSERT INTO cards (
    id, oracle_id, name, lang, released_at, layout,
    mana_cost, cmc, type_line, oracle_text, power, toughness, loyalty,
    colors, color_identity, keywords, rarity,
    set_code, set_name, set_type, collector_number, artist,
    border_color, frame, foil, nonfoil, reprint, digital,
    full_art, textless, promo, oversized, reserved,
    edhrec_rank, penny_rank,
    image_uris, legalities, prices,
    tcgplayer_id, cardmarket_id, card_kingdom_id, card_kingdom_foil_id,
    scryfall_uri, updated_at
) VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s, NOW()
)
ON CONFLICT (id) DO UPDATE SET
    oracle_id            = EXCLUDED.oracle_id,
    name                 = EXCLUDED.name,
    released_at          = EXCLUDED.released_at,
    mana_cost            = EXCLUDED.mana_cost,
    cmc                  = EXCLUDED.cmc,
    type_line            = EXCLUDED.type_line,
    oracle_text          = EXCLUDED.oracle_text,
    colors               = EXCLUDED.colors,
    color_identity       = EXCLUDED.color_identity,
    keywords             = EXCLUDED.keywords,
    rarity               = EXCLUDED.rarity,
    set_code             = EXCLUDED.set_code,
    set_name             = EXCLUDED.set_name,
    image_uris           = EXCLUDED.image_uris,
    legalities           = EXCLUDED.legalities,
    prices               = EXCLUDED.prices,
    tcgplayer_id         = EXCLUDED.tcgplayer_id,
    cardmarket_id        = EXCLUDED.cardmarket_id,
    card_kingdom_id      = EXCLUDED.card_kingdom_id,
    card_kingdom_foil_id = EXCLUDED.card_kingdom_foil_id,
    updated_at           = NOW()
"""


def get_bulk_download_url() -> str:
    print("Fetching bulk data manifest from Scryfall...")
    resp = httpx.get(SCRYFALL_BULK_API, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    for item in resp.json()["data"]:
        if item["type"] == BULK_TYPE:
            size_mb = item.get("size", 0) / 1_000_000
            print(f"Found {BULK_TYPE}: {size_mb:.0f} MB — {item['download_uri']}")
            return item["download_uri"]
    raise ValueError(f"Bulk type '{BULK_TYPE}' not found in Scryfall manifest")


def download_bulk(url: str) -> Path:
    out_path = Path(f"scryfall_{BULK_TYPE}.json")
    print(f"Downloading to {out_path}...")
    with httpx.stream("GET", url, headers=HEADERS, timeout=300, follow_redirects=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_bytes(chunk_size=1 << 20):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    print(f"\r  {downloaded/1e6:.1f}/{total/1e6:.1f} MB ({downloaded/total*100:.0f}%)", end="", flush=True)
    print()
    return out_path


def card_to_row(card: dict) -> tuple:
    return (
        card.get("id"),
        card.get("oracle_id"),
        card.get("name"),
        card.get("lang", "en"),
        card.get("released_at"),
        card.get("layout"),
        card.get("mana_cost"),
        card.get("cmc"),
        card.get("type_line"),
        card.get("oracle_text"),
        card.get("power"),
        card.get("toughness"),
        card.get("loyalty"),
        card.get("colors", []),
        card.get("color_identity", []),
        card.get("keywords", []),
        card.get("rarity"),
        card.get("set"),        # Scryfall field "set" → column set_code
        card.get("set_name"),
        card.get("set_type"),
        card.get("collector_number"),
        card.get("artist"),
        card.get("border_color"),
        card.get("frame"),
        card.get("foil"),
        card.get("nonfoil"),
        card.get("reprint"),
        card.get("digital"),
        card.get("full_art"),
        card.get("textless"),
        card.get("promo"),
        card.get("oversized"),
        card.get("reserved"),
        card.get("edhrec_rank"),
        card.get("penny_rank"),
        json.dumps(card.get("image_uris")),
        json.dumps(card.get("legalities", {})),
        json.dumps(card.get("prices", {})),
        card.get("tcgplayer_id"),
        card.get("cardmarket_id"),
        card.get("card_kingdom_id"),
        card.get("card_kingdom_foil_id"),
        card.get("scryfall_uri"),
    )


def stream_and_upsert(json_path: Path, conn):
    batch = []
    total = 0
    start = time.time()

    print(f"Streaming {json_path} into Supabase...")

    with open(json_path, "rb") as f:
        for card in ijson.items(f, "item"):
            batch.append(card_to_row(card))

            if len(batch) >= BATCH_SIZE:
                conn.cursor().executemany(UPSERT_SQL, batch)
                conn.commit()
                total += len(batch)
                elapsed = time.time() - start
                print(f"\r  {total:,} cards upserted ({elapsed:.0f}s)", end="", flush=True)
                batch.clear()

    if batch:
        conn.cursor().executemany(UPSERT_SQL, batch)
        conn.commit()
        total += len(batch)

    elapsed = time.time() - start
    print(f"\nDone. {total:,} cards upserted in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser(description="Ingest Scryfall card data into Supabase")
    parser.add_argument(
        "--file", type=Path, default=None,
        help="Path to a local Scryfall bulk JSON file. Downloads if not provided."
    )
    args = parser.parse_args()

    json_path = args.file
    if json_path is None:
        url = get_bulk_download_url()
        json_path = download_bulk(url)
    else:
        if not json_path.exists():
            print(f"Error: file not found: {json_path}", file=sys.stderr)
            sys.exit(1)
        print(f"Using local file: {json_path} ({json_path.stat().st_size / 1e6:.0f} MB)")

    print("Connecting to Supabase...")
    with psycopg.connect(DB_URL, prepare_threshold=None) as conn:
        stream_and_upsert(json_path, conn)


if __name__ == "__main__":
    main()
