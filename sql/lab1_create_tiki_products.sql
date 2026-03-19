-- Lab 1 (DoD): Load Project 02 Tiki product JSON into PostgreSQL

CREATE TABLE IF NOT EXISTS tiki_product (
  id           BIGINT PRIMARY KEY,
  name         TEXT,
  url_key      TEXT,
  price        NUMERIC(12, 2),
  description  TEXT,
  images_url   JSONB,
  loaded_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tiki_product_price ON tiki_product(price);
