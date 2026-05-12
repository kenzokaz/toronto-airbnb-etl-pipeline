"""
extractor.py
------------
Reads the Toronto Airbnb dataset from a local .xlsx file,
selects relevant columns, and bulk-loads into staging.listings_raw.
"""

import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [EXTRACTOR] %(message)s")
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DATA_FILE = Path(os.getenv("RAW_DATA_DIR", "/opt/airflow/data/raw")) / "listings_raw.xlsx"

DB_CONN = {
    "host":     os.getenv("DB_HOST",     "postgres"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "airbnb_dw"),
    "user":     os.getenv("DB_USER",     "airflow"),
    "password": os.getenv("DB_PASSWORD", "airflow"),
}

KEEP_COLS = [
    "id", "name", "host_id", "host_name",
    "neighbourhood_cleansed", "latitude", "longitude",
    "room_type", "price", "minimum_nights",
    "number_of_reviews", "last_review",
    "reviews_per_month", "calculated_host_listings_count",
    "availability_365",
]

RENAME_MAP = {"neighbourhood_cleansed": "neighbourhood"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    """Read listings sheet from local xlsx file."""
    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"Data file not found at {DATA_FILE}. "
            "Place listings_raw.xlsx in the data/raw/ folder."
        )
    log.info(f"Reading data from {DATA_FILE}…")
    df = pd.read_excel(DATA_FILE, sheet_name="listings", engine="openpyxl")
    log.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    return df


def select_and_rename(df: pd.DataFrame) -> pd.DataFrame:
    available = [c for c in KEEP_COLS if c in df.columns]
    df = df[available].rename(columns=RENAME_MAP)
    log.info(f"Selected {len(df.columns)} columns: {list(df.columns)}")
    return df


def load_to_staging(df: pd.DataFrame) -> None:
    cols = list(df.columns)

    # Convert all values to native Python types for psycopg2
    rows = [tuple(
        None if pd.isna(v) else v
        for v in row
    ) for row in df.itertuples(index=False, name=None)]

    insert_sql = f"""
        INSERT INTO staging.listings_raw ({', '.join(cols)})
        VALUES %s
    """

    conn = psycopg2.connect(**DB_CONN)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE staging.listings_raw")
                execute_values(cur, insert_sql, rows, page_size=1000)
                cur.execute("SELECT COUNT(*) FROM staging.listings_raw")
                count = cur.fetchone()[0]
        log.info(f"Staging loaded: {count:,} rows in staging.listings_raw ✓")
    finally:
        conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────
def run():
    df_raw    = load_data()
    df_staged = select_and_rename(df_raw)
    load_to_staging(df_staged)
    log.info("Extraction complete ✓")


if __name__ == "__main__":
    run()