# System Architecture & Design Documentation

## Selected Project: End-to-End ETL & Analytics Pipeline

**Dataset:** Toronto Airbnb Listings — Inside Airbnb (public dataset, ~15,000 listings)  
**Goal:** Ingest raw CSV data, clean and normalise it, warehouse it in a star schema, orchestrate the workflow, and surface insights via an interactive dashboard.

---

## Architecture Overview

The pipeline is entirely containerised using Docker Compose and consists of five services that communicate over an internal Docker network.

### Services

**1. PostgreSQL 15** (`postgres`)  
Serves as both the Airflow metadata database and the Airbnb data warehouse. The warehouse uses two schemas: `staging` for raw ingested data and `warehouse` for the cleaned, typed, star-schema tables. Four analytical views are pre-built so the dashboard needs no ad-hoc joins.

**2. Apache Airflow 2.8** (`airflow-webserver` + `airflow-scheduler`)  
Provides DAG-based orchestration with a weekly schedule (Sundays at 02:00). The DAG defines four tasks: extract → transform → validate → branch. The validation task enforces a data quality gate — if fewer than 70% of staged rows make it into the warehouse, the DAG marks the run as failed before it reaches the dashboard.

**3. Apache Spark 3.5** (`spark`)  
Provides distributed processing capability. For the current dataset scale (~15K rows), Pandas handles the transformations. The Spark container is included in the architecture to demonstrate scale-out readiness — the transformer is structured so Pandas DataFrames can be wrapped in `spark.createDataFrame()` for larger datasets without changing the transformation logic.

**4. Extractor + Transformer** (Python scripts, mounted into Airflow container)  
Custom Python services in `services/`. The extractor downloads the compressed CSV from Inside Airbnb, saves it locally, and bulk-loads it into `staging.listings_raw`. The transformer reads staging, applies a cleaning pipeline (price parsing, type casting, outlier removal, text normalisation), populates the dimension tables, then loads `warehouse.fact_listings`.

**5. Streamlit Dashboard** (`dashboard`)  
A containerised Streamlit app that connects directly to the PostgreSQL warehouse and renders four Plotly charts: a horizontal bar chart of average prices by neighbourhood, a donut chart of room type distribution, a bubble scatter of availability vs price, and a bar chart of top hosts by review volume. Sidebar filters (room type, price range) apply live via parameterised SQL.

---

## Data Flow Diagram

```
Inside Airbnb (HTTP)
       │
       ▼
  [Extractor]──────────────▶ staging.listings_raw
  (download CSV,                  │
   bulk INSERT)                   │
                                  ▼
                          [Transformer]
                          (clean prices,
                           cast types,
                           normalise text)
                                  │
                    ┌─────────────┼────────────────┐
                    ▼             ▼                 ▼
             dim_host   dim_neighbourhood   dim_room_type
                    │             │                 │
                    └─────────────▼─────────────────┘
                             fact_listings
                                  │
                     ┌────────────┼────────────┐
                     ▼            ▼            ▼
              vw_avg_price  vw_host_perf  vw_availability
                                  │
                                  ▼
                         [Streamlit Dashboard]
```

---

## Star Schema Design

The warehouse uses a classic star schema with one fact table and three dimension tables.

**Fact table: `warehouse.fact_listings`**  
Grain: one row per Airbnb listing. Contains all quantitative measures: `price_usd`, `minimum_nights`, `number_of_reviews`, `reviews_per_month`, `availability_365`. Foreign keys reference all three dims.

**Dimension tables:**
- `dim_neighbourhood` — unique neighbourhoods (normalised to title case)
- `dim_room_type` — Entire home/apt, Private room, Shared room, Hotel room
- `dim_host` — host identity and aggregate listing count

**Analytical views** pre-join the schema so the dashboard issues simple `SELECT * FROM view` queries rather than multi-table joins at runtime. This improves dashboard responsiveness and separates analytical SQL from application code.

---

## Technology Justifications

| Decision | Rationale |
|----------|-----------|
| **Docker Compose** over bare scripts | Ensures reproducibility — the pipeline runs identically on any machine with Docker installed. Each service is isolated; failures don't cascade. |
| **Apache Airflow** over cron | Provides visibility (UI, logs, retry history), dependency declaration between tasks, and parameterisable schedules. The validation branch task demonstrates DAG branching. |
| **PostgreSQL** over a file-based store | Supports concurrent reads from Airflow and Streamlit, enforces referential integrity across the star schema, and scales to millions of rows without configuration changes. |
| **Pandas** (with Spark available) | Pandas is sufficient for 15K rows and keeps the transformation code readable and testable. The Spark container demonstrates that upgrading to distributed processing requires no code rewrite — just wrapping the DataFrame. |
| **Streamlit + Plotly** over Grafana | Streamlit allows Python-native parameterised queries and interactive widgets with minimal boilerplate. Plotly provides publication-quality interactive charts. Grafana would require a separate JSON dashboard definition and plugin for Plotly-style charts. |

---

## Scalability

- **Horizontal scale:** Replacing `LocalExecutor` with `CeleryExecutor` in Airflow adds worker nodes without changing DAG code.
- **Data volume:** Swapping the Pandas transformer for a PySpark job requires changing ~5 lines — the DataFrame interface is identical.
- **New data sources:** The staging schema accepts any flat CSV; the transformer is the only file that needs updating.

## Reliability

- **Retries:** Each Airflow task has `retries=2` with a 5-minute delay.
- **Idempotency:** Both the extractor (`TRUNCATE` before insert) and transformer (`TRUNCATE ... RESTART IDENTITY CASCADE` before load) are safe to re-run.
- **Quality gate:** The validation task rejects runs where fewer than 70% of staged rows pass cleaning filters.

## Modularity

Each service (`extractor`, `transformer`, `dashboard`) is a standalone Python module callable independently from the command line or from within the DAG. Adding a new data source means writing a new extractor — the transformer and warehouse schema remain unchanged.
