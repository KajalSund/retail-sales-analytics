# Retail Sales Analytics Platform

An end-to-end enterprise data pipeline that ingests retail sales data, processes it with PySpark on Azure Databricks, stores analytics-ready aggregates in PostgreSQL, and exposes them through a FastAPI REST API — all containerised with Docker.

---

## Architecture

```
Sales Data (CSV / API)
        │
        ▼
Azure Data Factory          ← orchestration & scheduling
        │
        ▼
ADLS Gen2 (Bronze Layer)    ← raw files, immutable audit trail
        │
        ▼
Azure Databricks (PySpark)  ← cleaning, validation, enrichment
        │
        ▼
Delta Lake (Silver Layer)   ← clean, deduplicated records
        │
        ▼
Delta Lake (Gold Layer)     ← business aggregates
        │
        ▼
PostgreSQL                  ← analytics read-store
        │
        ▼
FastAPI REST API            ← consumed by dashboards / downstream apps
```

---

## Project Structure

```
retail-sales-analytics/
│
├── data/
│   └── sales_data.csv              # 30-row sample dataset
│
├── databricks/
│   └── sales_processing.py         # PySpark Bronze → Silver → Gold pipeline
│
├── backend/
│   ├── app/
│   │   ├── main.py                 # FastAPI app, lifespan, middleware
│   │   ├── config.py               # Pydantic settings (env-driven)
│   │   ├── database.py             # SQLAlchemy async engine + ORM models
│   │   └── routes/
│   │       └── sales.py            # All analytics endpoints
│   └── requirements.txt
│
├── docker/
│   ├── Dockerfile                  # Multi-stage build
│   └── docker-compose.yml          # Local dev stack (Postgres + API)
│
└── README.md
```

---

## Technology Choices

| Layer | Technology | Why |
|---|---|---|
| Ingestion | Azure Data Factory | Managed ETL, schedule-driven, GUI pipeline builder |
| Raw storage | ADLS Gen2 | Hierarchical namespace, cheap blob storage |
| Processing | Azure Databricks + PySpark | Distributed compute, Delta Lake native |
| Storage format | Delta Lake | ACID transactions, time travel, schema enforcement |
| Analytics DB | PostgreSQL | Battle-tested, excellent async driver support |
| API | FastAPI | Native async, auto OpenAPI docs, Pydantic validation |
| Container | Docker (multi-stage) | Small image, reproducible builds, non-root security |

---

## Medallion Architecture

The pipeline follows the **Bronze → Silver → Gold** medallion pattern:

### Bronze (Raw)
- Files land from ADF exactly as received — no transforms
- Append-only Delta table; provides full audit trail
- Extra columns: `ingestion_timestamp`, `source_file`, `pipeline_run_date`

### Silver (Cleaned)
- Type casts, null handling, data quality filters
- Derived columns: `net_revenue`, `discount_amount`, calendar dimensions
- Upserted via `MERGE` so re-runs are idempotent

### Gold (Aggregated)
| Table | Description |
|---|---|
| `daily_revenue` | Daily net revenue, basket value, unique customers — per region |
| `product_performance` | Lifetime revenue & volume rank per product |
| `store_performance` | Revenue leaderboard per store |
| `category_trends` | Weekly revenue roll-up per product category |

---

## API Endpoints

Base URL: `http://localhost:8000`

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Liveness probe |
| GET | `/docs` | Swagger UI |
| GET | `/api/v1/sales/summary` | Executive KPI summary |
| GET | `/api/v1/sales/daily-revenue` | Daily revenue (paginated, filterable) |
| GET | `/api/v1/sales/top-products` | Top N products by revenue |
| GET | `/api/v1/sales/store-performance` | Store leaderboard |
| GET | `/api/v1/sales/store/{store_id}` | Single store detail |
| GET | `/api/v1/sales/category-trends` | Weekly category roll-up |

### Example Requests

```bash
# Executive summary
curl http://localhost:8000/api/v1/sales/summary

# Daily revenue for Ontario in January 2024
curl "http://localhost:8000/api/v1/sales/daily-revenue?region=Ontario&start_date=2024-01-01&end_date=2024-01-31"

# Top 5 products in the Footwear category
curl "http://localhost:8000/api/v1/sales/top-products?limit=5&category=Footwear"

# Single store detail
curl http://localhost:8000/api/v1/sales/store/S01
```

---

## Quick Start (Docker)

```bash
# 1. Clone the repo
git clone https://github.com/your-username/retail-sales-analytics.git
cd retail-sales-analytics

# 2. Start the full stack
docker compose -f docker/docker-compose.yml up --build

# 3. Open the API docs
open http://localhost:8000/docs
```

---

## Local Development (without Docker)

```bash
# Prerequisites: Python 3.12+, PostgreSQL 16 running locally

# 1. Create virtual environment
python -m venv .venv && source .venv/bin/activate

# 2. Install dependencies
pip install -r backend/requirements.txt

# 3. Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_USER=retail_user
export POSTGRES_PASSWORD=retail_pass
export POSTGRES_DB=retail_analytics
export DEBUG=true

# 4. Run the API (with hot-reload)
cd backend
uvicorn app.main:app --reload --port 8000
```

---

## Running the PySpark Pipeline (Local)

```bash
# Install PySpark + Delta Lake
pip install pyspark delta-spark

# Run the pipeline against the sample CSV
python databricks/sales_processing.py data/sales_data.csv
```

In production, upload `sales_processing.py` as a Databricks Job and trigger it from ADF using the **Azure Databricks** linked service.

---

## Sample Data

`data/sales_data.csv` contains 30 transactions across:
- **4 stores** in Toronto, Edmonton, Vancouver, and Ottawa
- **6 products** across Footwear, Apparel, Sports, Nutrition, Electronics, and Outdoors
- **12 days** of sales (Jan 2 – Jan 13 2024)
- Discounts ranging from 0% to 20%
