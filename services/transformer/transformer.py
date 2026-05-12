"""
transformer.py
--------------
Reads from staging.listings_raw, cleans and normalises,
then loads into the warehouse star schema.
Note: price column is empty in this dataset so we use
review scores and occupancy as the primary metrics.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s [TRANSFORMER] %(message)s")
log = logging.getLogger(__name__)

DB_CONN = {
    "host":     os.getenv("DB_HOST",     "postgres"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "airbnb_dw"),
    "user":     os.getenv("DB_USER",     "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
}


def read_staging():
    import sqlalchemy
    engine = sqlalchemy.create_engine(
        f"postgresql+psycopg2://{DB_CONN['user']}:{DB_CONN['password']}"
        f"@{DB_CONN['host']}:{DB_CONN['port']}/{DB_CONN['dbname']}"
    )
    df = pd.read_sql("SELECT * FROM staging.listings_raw", engine)
    log.info(f"Read {len(df):,} rows from staging")
    return df


def clean(df):
    # Drop rows with no id
    df = df.dropna(subset=["id"])

    # Use 0 as default price since column is empty in this dataset
    df["price_usd"] = 0.0

    # Numeric coercions
    df["reviews_per_month"]   = pd.to_numeric(df.get("reviews_per_month"),   errors="coerce").fillna(0)
    df["minimum_nights"]      = pd.to_numeric(df.get("minimum_nights"),      errors="coerce").fillna(1).astype(int)
    df["number_of_reviews"]   = pd.to_numeric(df.get("number_of_reviews"),   errors="coerce").fillna(0).astype(int)
    df["availability_365"]    = pd.to_numeric(df.get("availability_365"),    errors="coerce").fillna(0).astype(int)

    # Parse last_review date
    df["last_review_date"] = pd.to_datetime(df.get("last_review"), errors="coerce").dt.date

    # Strip whitespace from text fields
    for col in ["name", "host_name", "neighbourhood", "room_type"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().replace("nan", None)

    # Normalise neighbourhood capitalisation
    df["neighbourhood"] = df["neighbourhood"].str.title()

    # Drop rows missing neighbourhood or room_type
    df = df.dropna(subset=["neighbourhood", "room_type"])
    df = df[df["neighbourhood"] != "None"]
    df = df[df["room_type"] != "None"]

    log.info(f"After cleaning: {len(df):,} rows remain")
    return df


def load_dimensions(df, conn):
    with conn.cursor() as cur:
        # dim_neighbourhood
        hoods = df["neighbourhood"].dropna().unique().tolist()
        execute_values(
            cur,
            "INSERT INTO warehouse.dim_neighbourhood (neighbourhood) VALUES %s ON CONFLICT DO NOTHING",
            [(h,) for h in hoods],
        )
        cur.execute("SELECT neighbourhood_id, neighbourhood FROM warehouse.dim_neighbourhood")
        hood_map = {row[1]: row[0] for row in cur.fetchall()}

        # dim_room_type
        rtypes = df["room_type"].dropna().unique().tolist()
        execute_values(
            cur,
            "INSERT INTO warehouse.dim_room_type (room_type) VALUES %s ON CONFLICT DO NOTHING",
            [(r,) for r in rtypes],
        )
        cur.execute("SELECT room_type_id, room_type FROM warehouse.dim_room_type")
        rt_map = {row[1]: row[0] for row in cur.fetchall()}

        # dim_host
        host_df = (
            df.groupby(["host_id", "host_name"])
            .size()
            .reset_index(name="total_listings")
        )
        execute_values(
            cur,
            """
            INSERT INTO warehouse.dim_host (host_id, host_name, total_listings)
            VALUES %s
            ON CONFLICT (host_id) DO UPDATE
              SET host_name      = EXCLUDED.host_name,
                  total_listings = EXCLUDED.total_listings
            """,
            list(host_df.itertuples(index=False, name=None)),
        )

    conn.commit()
    log.info(f"Dimensions loaded: {len(hood_map)} neighbourhoods, {len(rt_map)} room types")
    return hood_map, rt_map


def load_facts(df, hood_map, rt_map, conn):
    df["neighbourhood_id"] = df["neighbourhood"].map(hood_map)
    df["room_type_id"]     = df["room_type"].map(rt_map)

    fact_df = df.dropna(subset=["neighbourhood_id", "room_type_id"])

    rows = []
    for r in fact_df.itertuples(index=False, name=None):
        row_dict = dict(zip(fact_df.columns, r))
        rows.append((
            row_dict["id"],
            row_dict.get("name"),
            row_dict["host_id"],
            row_dict["neighbourhood_id"],
            row_dict["room_type_id"],
            row_dict.get("latitude"),
            row_dict.get("longitude"),
            row_dict["price_usd"],
            row_dict["minimum_nights"],
            row_dict["number_of_reviews"],
            row_dict["reviews_per_month"],
            row_dict["availability_365"],
            None if pd.isnull(row_dict.get("last_review_date")) else row_dict.get("last_review_date"),
        ))

    with conn.cursor() as cur:
        cur.execute("TRUNCATE warehouse.fact_listings RESTART IDENTITY CASCADE")
        execute_values(
            cur,
            """
            INSERT INTO warehouse.fact_listings (
                listing_id, listing_name, host_id, neighbourhood_id, room_type_id,
                latitude, longitude, price_usd, minimum_nights,
                number_of_reviews, reviews_per_month, availability_365,
                last_review_date
            ) VALUES %s
            ON CONFLICT (listing_id) DO NOTHING
            """,
            rows,
            page_size=500,
        )
        cur.execute("SELECT COUNT(*) FROM warehouse.fact_listings")
        count = cur.fetchone()[0]
    conn.commit()
    log.info(f"Fact table loaded: {count:,} listings in warehouse.fact_listings ✓")


def run():
    df = read_staging()
    df = clean(df)

    conn = psycopg2.connect(**DB_CONN)
    try:
        hood_map, rt_map = load_dimensions(df, conn)
        load_facts(df, hood_map, rt_map, conn)
    finally:
        conn.close()

    log.info("Transformation complete ✓")


if __name__ == "__main__":
    run()