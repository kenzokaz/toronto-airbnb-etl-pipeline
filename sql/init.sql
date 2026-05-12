-- Create the data warehouse database
CREATE DATABASE airbnb_dw;

\connect airbnb_dw;

-- ─────────────────────────────────────────────
-- STAGING layer (raw ingest, no constraints)
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.listings_raw (
    id                  BIGINT,
    name                TEXT,
    host_id             BIGINT,
    host_name           TEXT,
    neighbourhood       TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    room_type           TEXT,
    price               TEXT,          -- raw, may contain '$' or commas
    minimum_nights      INTEGER,
    number_of_reviews   INTEGER,
    last_review         TEXT,
    reviews_per_month   TEXT,
    calculated_host_listings_count INTEGER,
    availability_365    INTEGER,
    loaded_at           TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- WAREHOUSE layer (clean, typed, analytics-ready)
-- ─────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_neighbourhood (
    neighbourhood_id    SERIAL PRIMARY KEY,
    neighbourhood       TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_room_type (
    room_type_id        SERIAL PRIMARY KEY,
    room_type           TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_host (
    host_id             BIGINT PRIMARY KEY,
    host_name           TEXT,
    total_listings      INTEGER
);

CREATE TABLE IF NOT EXISTS warehouse.fact_listings (
    listing_id                      BIGINT PRIMARY KEY,
    listing_name                    TEXT,
    host_id                         BIGINT REFERENCES warehouse.dim_host(host_id),
    neighbourhood_id                INTEGER REFERENCES warehouse.dim_neighbourhood(neighbourhood_id),
    room_type_id                    INTEGER REFERENCES warehouse.dim_room_type(room_type_id),
    latitude                        DOUBLE PRECISION,
    longitude                       DOUBLE PRECISION,
    price_usd                       NUMERIC(10,2),
    minimum_nights                  INTEGER,
    number_of_reviews               INTEGER,
    reviews_per_month               NUMERIC(5,2),
    availability_365                INTEGER,
    last_review_date                DATE,
    transformed_at                  TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────────
-- Analytical views (used by Streamlit dashboard)
-- ─────────────────────────────────────────────

CREATE OR REPLACE VIEW warehouse.vw_avg_price_by_neighbourhood AS
SELECT
    n.neighbourhood,
    rt.room_type,
    ROUND(AVG(f.price_usd), 2)      AS avg_price,
    COUNT(*)                         AS listing_count
FROM warehouse.fact_listings f
JOIN warehouse.dim_neighbourhood n  ON f.neighbourhood_id = n.neighbourhood_id
JOIN warehouse.dim_room_type rt     ON f.room_type_id     = rt.room_type_id
GROUP BY n.neighbourhood, rt.room_type
ORDER BY avg_price DESC;

CREATE OR REPLACE VIEW warehouse.vw_host_performance AS
SELECT
    h.host_id,
    h.host_name,
    h.total_listings,
    ROUND(AVG(f.price_usd), 2)          AS avg_price,
    SUM(f.number_of_reviews)            AS total_reviews,
    ROUND(AVG(f.availability_365), 1)   AS avg_availability
FROM warehouse.fact_listings f
JOIN warehouse.dim_host h ON f.host_id = h.host_id
GROUP BY h.host_id, h.host_name, h.total_listings
ORDER BY total_reviews DESC;

CREATE OR REPLACE VIEW warehouse.vw_availability_distribution AS
SELECT
    n.neighbourhood,
    ROUND(AVG(f.availability_365), 1)   AS avg_availability,
    MIN(f.price_usd)                     AS min_price,
    MAX(f.price_usd)                     AS max_price,
    ROUND(AVG(f.price_usd), 2)          AS avg_price,
    COUNT(*)                             AS total_listings
FROM warehouse.fact_listings f
JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
GROUP BY n.neighbourhood
ORDER BY avg_availability DESC;

CREATE OR REPLACE VIEW warehouse.vw_room_type_summary AS
SELECT
    rt.room_type,
    COUNT(*)                         AS total_listings,
    ROUND(AVG(f.price_usd), 2)      AS avg_price,
    ROUND(AVG(f.number_of_reviews), 1) AS avg_reviews,
    ROUND(AVG(f.availability_365), 1)  AS avg_availability
FROM warehouse.fact_listings f
JOIN warehouse.dim_room_type rt ON f.room_type_id = rt.room_type_id
GROUP BY rt.room_type
ORDER BY total_listings DESC;
