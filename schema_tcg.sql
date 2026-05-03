-- TCGPlayer price tables. Run in the Supabase SQL editor before running tcg_ingest.py.
--
-- tcg_products  — one row per product per calendar day (market price snapshot)
-- tcg_listings  — individual seller listings, replaced each run
--
-- Join to the cards table via: cards.tcgplayer_id = tcg_products.tcg_id

-- -----------------------------------------------------------------------
-- Drop and recreate if you need to reset (dev only):
-- DROP TABLE IF EXISTS tcg_listings;
-- DROP TABLE IF EXISTS tcg_products;
-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tcg_products (
    id                      BIGSERIAL PRIMARY KEY,
    tcg_id                  INTEGER NOT NULL,
    name                    TEXT,
    set_name                TEXT,
    set_id                  INTEGER,
    rarity                  TEXT,
    foil_only               BOOLEAN DEFAULT FALSE,
    url_name                TEXT,
    market_price            NUMERIC(10, 2),
    low_price               NUMERIC(10, 2),
    low_price_with_shipping NUMERIC(10, 2),
    median_price            NUMERIC(10, 2),
    snapshot_date           DATE NOT NULL DEFAULT CURRENT_DATE,
    snapshot_at             TIMESTAMPTZ DEFAULT NOW()
);

-- One row per product per calendar day
CREATE UNIQUE INDEX IF NOT EXISTS tcg_products_daily_uniq
    ON tcg_products (tcg_id, snapshot_date);

CREATE INDEX IF NOT EXISTS tcg_products_name_idx    ON tcg_products (name);
CREATE INDEX IF NOT EXISTS tcg_products_set_idx     ON tcg_products (set_name);
CREATE INDEX IF NOT EXISTS tcg_products_date_idx    ON tcg_products (snapshot_date);

-- -----------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tcg_listings (
    id              BIGSERIAL PRIMARY KEY,
    listing_id      BIGINT NOT NULL,
    product_id      INTEGER NOT NULL,
    seller_id       TEXT,
    seller_name     TEXT,
    seller_rating   NUMERIC(5, 2),
    seller_sales    TEXT,
    gold_seller     BOOLEAN DEFAULT FALSE,
    verified_seller BOOLEAN DEFAULT FALSE,
    direct_seller   BOOLEAN DEFAULT FALSE,
    condition       TEXT,
    condition_id    INTEGER,
    printing        TEXT,
    language        TEXT,
    price           NUMERIC(10, 2),
    shipping_price  NUMERIC(10, 2),
    ranked_price    NUMERIC(10, 2),
    quantity        INTEGER,
    snapshot_at     TIMESTAMPTZ DEFAULT NOW()
);

-- One row per listing_id (upserted each run)
CREATE UNIQUE INDEX IF NOT EXISTS tcg_listings_listing_id_uniq
    ON tcg_listings (listing_id);

CREATE INDEX IF NOT EXISTS tcg_listings_product_id_idx ON tcg_listings (product_id);
CREATE INDEX IF NOT EXISTS tcg_listings_condition_idx  ON tcg_listings (condition);
CREATE INDEX IF NOT EXISTS tcg_listings_price_idx      ON tcg_listings (price);
