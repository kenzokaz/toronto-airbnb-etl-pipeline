# 🏠 Toronto Airbnb ETL & Analytics Pipeline

> End-to-end containerized data pipeline — COSC4606 Final Project  
> **Extract → Transform → Load → Analyse** using real Toronto Airbnb data

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker%20Compose-2.24-2496ED?style=flat-square&logo=docker&logoColor=white)](https://docker.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      Docker Compose                          │
│                                                             │
│  ┌──────────┐   ┌──────────────┐   ┌─────────────────────┐ │
│  │ Extractor│──▶│  Transformer │──▶│   PostgreSQL DW      │ │
│  │ (Python) │   │ (Pandas /    │   │  ┌───────────────┐  │ │
│  │          │   │  PySpark)    │   │  │ staging schema│  │ │
│  └──────────┘   └──────────────┘   │  ├───────────────┤  │ │
│        ▲               ▲           │  │warehouse schema│  │ │
│        │               │           │  └───────────────┘  │ │
│  ┌─────┴───────────────┴────────┐  └─────────┬───────────┘ │
│  │      Apache Airflow DAG      │            │             │ │
│  │  (Weekly schedule + QA gate) │            ▼             │ │
│  └──────────────────────────────┘  ┌─────────────────────┐ │
│                                    │  Streamlit Dashboard │ │
│  ┌───────────────┐                 │  (4 interactive viz) │ │
│  │  Apache Spark │                 └─────────────────────┘ │
│  │  (Distributed │                                         │
│  │   processing) │                                         │
│  └───────────────┘                                         │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

| Step | Service | What happens |
|------|---------|--------------|
| **Extract** | `services/extractor/` | Downloads Toronto listings CSV from Inside Airbnb, loads into `staging.listings_raw` |
| **Transform** | `services/transformer/` | Cleans prices, casts types, normalises text, populates star-schema (dims + facts) |
| **Orchestrate** | Apache Airflow DAG | Schedules weekly runs, enforces a ≥70% data-retention quality gate |
| **Warehouse** | PostgreSQL 15 | Two-schema design: `staging` (raw) → `warehouse` (star schema + analytical views) |
| **Visualise** | Streamlit | Live dashboard querying warehouse views — 4 charts + KPI cards + data explorer |

---

## 📁 Project Structure

```
.
├── airflow/
│   └── dags/
│       └── airbnb_etl_dag.py       # Airflow DAG (extract → transform → validate → branch)
├── dashboards/
│   ├── app.py                       # Streamlit dashboard (4 visualisations)
│   ├── Dockerfile
│   └── requirements.txt
├── data/
│   └── raw/                         # Auto-created; stores downloaded CSV
├── docs/
│   └── architecture.md              # Design documentation
├── services/
│   ├── extractor/
│   │   └── extractor.py             # Download + stage raw data
│   ├── transformer/
│   │   └── transformer.py           # Clean, normalise, load warehouse
│   └── requirements.txt
├── sql/
│   └── init.sql                     # DB init: schemas, tables, analytical views
├── docker-compose.yml
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop ≥ 4.x
- Docker Compose ≥ 2.x
- ~4 GB RAM available for containers

### 1. Clone & start

```bash
git clone https://github.com/kenzokaz/COSC4606_project.git
cd COSC4606_project
docker compose up -d
```

### 2. Wait for services (~2 min on first boot)

```bash
docker compose ps        # all services should show "running"
```

### 3. Trigger the ETL pipeline

Open **Airflow UI** → [http://localhost:8080](http://localhost:8080)  
Login: `admin` / `admin`  
Enable the `airbnb_etl_pipeline` DAG → click **Trigger DAG ▶**

The pipeline will:
1. Download ~20MB of Toronto Airbnb data
2. Clean and transform 15,000+ listings
3. Load the warehouse star schema
4. Validate data quality (≥70% row retention gate)

### 4. View the dashboard

Open **Streamlit** → [http://localhost:8501](http://localhost:8501)

---

## 📊 Dashboard Insights

The Streamlit dashboard surfaces 4 analytical views:

| # | Metric | Chart type |
|---|--------|-----------|
| 1 | **KPIs** — total listings, avg price, unique hosts, avg availability | Metric cards |
| 2 | **Avg price by neighbourhood** (top 20, broken by room type) | Horizontal bar |
| 3 | **Room type distribution** — share of entire home vs private/shared room | Donut chart |
| 4 | **Availability vs price** — neighbourhood bubble scatter | Bubble scatter |
| 5 | **Top 15 hosts by reviews** — coloured by avg listing price | Bar + colour scale |

Sidebar filters let you slice by room type and price range in real time.

---

## 🗄 Warehouse Schema

```sql
-- Star schema inside airbnb_dw database
warehouse.dim_neighbourhood   -- PK: neighbourhood_id
warehouse.dim_room_type       -- PK: room_type_id
warehouse.dim_host            -- PK: host_id
warehouse.fact_listings       -- FK → all dims; grain: one row per listing

-- Analytical views (no joins needed in dashboard)
warehouse.vw_avg_price_by_neighbourhood
warehouse.vw_host_performance
warehouse.vw_availability_distribution
warehouse.vw_room_type_summary
```

### Example queries

```sql
-- Top 10 most expensive neighbourhoods for entire homes
SELECT neighbourhood, ROUND(AVG(price_usd), 2) AS avg_price
FROM warehouse.fact_listings f
JOIN warehouse.dim_neighbourhood n ON f.neighbourhood_id = n.neighbourhood_id
JOIN warehouse.dim_room_type rt    ON f.room_type_id     = rt.room_type_id
WHERE rt.room_type = 'Entire home/apt'
GROUP BY neighbourhood
ORDER BY avg_price DESC
LIMIT 10;

-- Hosts with >5 listings and their avg review rate
SELECT h.host_name, h.total_listings,
       ROUND(AVG(f.reviews_per_month), 2) AS avg_monthly_reviews
FROM warehouse.fact_listings f
JOIN warehouse.dim_host h ON f.host_id = h.host_id
WHERE h.total_listings > 5
GROUP BY h.host_id, h.host_name, h.total_listings
ORDER BY avg_monthly_reviews DESC;
```

---

## 🔧 Services Reference

| Service | URL | Credentials |
|---------|-----|------------|
| Airflow Webserver | http://localhost:8080 | admin / admin |
| Spark Master UI | http://localhost:8081 | — |
| Streamlit Dashboard | http://localhost:8501 | — |
| PostgreSQL | localhost:5432 | airflow / airflow |

---

## 🛠 Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Containerisation | Docker Compose | Reproducible, portable, single-command startup |
| Orchestration | Apache Airflow 2.8 | Industry-standard DAG scheduling + retry logic |
| Processing | Pandas + PySpark 3.5 | Pandas for transform logic; Spark for scale-out |
| Warehouse | PostgreSQL 15 | Reliable OLAP-friendly relational store |
| Dashboard | Streamlit 1.32 + Plotly | Fast interactive analytics UI, Python-native |
| Dataset | Inside Airbnb — Toronto | Real, public, rich (~15K listings, 70+ columns) |

---

## 📄 Deliverables

- ✅ Containerised ETL pipeline with working Airflow DAG
- ✅ Star-schema PostgreSQL warehouse (staging + warehouse schemas)
- ✅ Dashboard with 4+ metrics and interactive filters
- ✅ Data quality gate (≥70% row retention check)
- ✅ Documented system design and data flow

---

*Dataset: [Inside Airbnb — Toronto](http://insideairbnb.com/get-the-data/) · License: Creative Commons CC0*
