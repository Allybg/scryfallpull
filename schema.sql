-- Run this in the Supabase SQL editor to set up the pipeline tables.

CREATE TABLE IF NOT EXISTS cards (
    id              TEXT PRIMARY KEY,       -- Scryfall UUID
    oracle_id       TEXT,
    name            TEXT NOT NULL,
    lang            TEXT DEFAULT 'en',
    released_at     DATE,
    layout          TEXT,
    mana_cost       TEXT,
    cmc             NUMERIC,
    type_line       TEXT,
    oracle_text     TEXT,
    power           TEXT,
    toughness       TEXT,
    loyalty         TEXT,
    colors          TEXT[],
    color_identity  TEXT[],
    keywords        TEXT[],
    rarity          TEXT,
    set_code        TEXT,
    set_name        TEXT,
    set_type        TEXT,
    collector_number TEXT,
    artist          TEXT,
    border_color    TEXT,
    frame           TEXT,
    foil            BOOLEAN,
    nonfoil         BOOLEAN,
    reprint         BOOLEAN,
    digital         BOOLEAN,
    full_art        BOOLEAN,
    textless        BOOLEAN,
    promo           BOOLEAN,
    oversized       BOOLEAN,
    reserved        BOOLEAN,
    edhrec_rank     INTEGER,
    penny_rank      INTEGER,
    image_uris      JSONB,
    legalities      JSONB,
    prices          JSONB,  -- Scryfall market price snapshot
    tcgplayer_id    INTEGER,
    cardmarket_id   INTEGER,
    card_kingdom_id INTEGER,
    card_kingdom_foil_id INTEGER,
    scryfall_uri    TEXT,
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cards_name_idx       ON cards (name);
CREATE INDEX IF NOT EXISTS cards_set_code_idx   ON cards (set_code);
CREATE INDEX IF NOT EXISTS cards_oracle_id_idx  ON cards (oracle_id);
CREATE INDEX IF NOT EXISTS cards_ck_id_idx      ON cards (card_kingdom_id);

-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ck_prices (
    id              BIGSERIAL PRIMARY KEY,
    ck_id           INTEGER NOT NULL,
    name            TEXT NOT NULL,
    edition         TEXT,
    edition_code    TEXT,
    is_foil         BOOLEAN DEFAULT FALSE,
    buy_price       NUMERIC(10, 2),
    sell_price      NUMERIC(10, 2),
    scryfall_id     TEXT REFERENCES cards (id),
    snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE,
    snapshot_at     TIMESTAMPTZ DEFAULT NOW()
);

-- One row per ck_id + foil variant per calendar day
CREATE UNIQUE INDEX IF NOT EXISTS ck_prices_daily_uniq
    ON ck_prices (ck_id, is_foil, snapshot_date);

CREATE INDEX IF NOT EXISTS ck_prices_scryfall_id_idx ON ck_prices (scryfall_id);
