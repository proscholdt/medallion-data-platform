# Data Engineering Platform — Medallion Architecture

End-to-end data platform that extracts data from **8 sources**, transforms through a **Bronze → Silver → Gold** lakehouse architecture, and stores everything in **Azure Data Lake Gen2** as Parquet/Delta Lake.

Built for real production workloads at **IT Valley**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                   │
│                                                                         │
│  Facebook Ads ─┐                                                        │
│  Google Ads ───┤                                                        │
│  Hotmart ──────┤    ┌──────────┐    ┌──────────┐    ┌──────────┐       │
│  Voomp ────────┼───►│  BRONZE  │───►│  SILVER  │───►│   GOLD   │       │
│  Pipedrive ────┤    │ Raw JSON │    │ Parquet  │    │ Star     │       │
│  Instagram ────┤    │ Raw CSV  │    │ Cleansed │    │ Schema   │       │
│  YouTube ──────┤    └──────────┘    └──────────┘    └──────────┘       │
│  Exchange API ─┘         │               │               │              │
│                          ▼               ▼               ▼              │
│                   ┌─────────────────────────────────────────┐           │
│                   │     Azure Data Lake Gen2 (Blob Storage)  │           │
│                   │     Containers: bronze/ silver/ gold/     │           │
│                   └─────────────────────────────────────────┘           │
│                                      │                                  │
│                                      ▼                                  │
│                              ┌──────────────┐                           │
│                              │   Power BI /  │                           │
│                              │   Analytics   │                           │
│                              └──────────────┘                           │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Medallion Layers

| Layer | Purpose | Format | Storage |
|-------|---------|--------|---------|
| **Bronze** | Raw ingestion — minimal transformation, preserve fidelity | JSON, CSV | `bronze/` container |
| **Silver** | Cleansed, typed, deduplicated, normalized column names | Parquet (Snappy) | `silver/` container |
| **Gold** | Star schema — fact & dimension tables, business-ready | Parquet / Delta Lake | `gold/` container |

---

## Data Sources & Pipelines

| # | Datamart | Source API | Bronze | Silver | Gold |
|---|----------|-----------|--------|--------|------|
| 1 | **Facebook Ads** | Graph API | Campaigns, AdSets, Ads (JSON) | Parquet + type casting | Consolidated fact tables |
| 2 | **Google Ads** | Google Ads API + Sheets | Campaigns, Keywords, Audiences | Normalized metrics | 11 tables (6 facts + 5 dims) |
| 3 | **Social Media** | Graph API + YouTube API | Followers, engagement metrics | Unified social metrics | Time-series aggregation |
| 4 | **Calls** | Google Sheets | Raw call logs (CSV) | Cleaned + hashed IDs | Aggregated call analytics |
| 5 | **Hotmart** | Hotmart API | Sales, users, subscriptions | Deduplicated + MD5 keys | Customer + sales facts |
| 6 | **Voomp** | Voomp API | Sales, projections (Excel) | Filtered + validated | Revenue fact tables |
| 7 | **Currency** | APILayer | Daily exchange rates | Incremental Parquet | BRL/USD/EUR/CAD history |
| 8 | **Pipedrive** | Pipedrive API | Deals, persons, activities | CRM normalized | Pipeline analytics |

---

## Star Schema (Google Ads Gold — example)

```
              ┌───────────────┐
              │ dim_campanha  │
              └───────┬───────┘
                      │
┌──────────────┐      │      ┌─────────────────┐
│ dim_anuncio  │──────┼──────│ dim_dispositivo  │
└──────────────┘      │      └─────────────────┘
                      │
              ┌───────┴────────┐
              │ fato_campanha  │
              │ fato_anuncio   │
              │ fato_keywords  │
              │ fato_grupo     │
              │ fato_publico   │
              └───────┬────────┘
                      │
┌────────────────┐    │    ┌──────────────────┐
│ dim_grupoAnun  │────┘────│ dim_publicoAlvo  │
└────────────────┘         └──────────────────┘
```

---

## Tech Stack

| Category | Technologies |
|----------|-------------|
| **Language** | Python 3.13 |
| **DataFrames** | Polars, Pandas, PyArrow |
| **Storage** | Azure Blob Storage, Delta Lake, Parquet |
| **Extraction** | Requests, gspread (Google Sheets), OAuth2 |
| **Orchestration** | Custom Python orchestrators (sequential pipeline) |
| **Data Quality** | MD5 hashing (dedup), type enforcement, null filtering |
| **Infra** | Docker, docker-compose |

---

## Project Structure

```
├── Dockerfile                           # Container image
├── docker-compose.yml                   # ETL service
├── requirements.txt                     # Python dependencies
├── .env.example                         # Environment variables template
│
└── datamart/                            # ETL pipelines
    ├── 1_Orchestrator_General/          # Master orchestrator (runs all)
    │
    ├── facebook/                        # Facebook Ads pipeline
    │   ├── 1_bronze/
    │   │   ├── cargaDiaria/             #   Daily extraction scripts
    │   │   └── transformers/            #   Bronze → Silver transformers
    │   ├── 2_silver/
    │   └── 3_gold/
    │
    ├── google/                          # Google Ads pipeline
    │   └── ADS/
    │       ├── 1_bronze/Transformation/ #   15 extraction scripts
    │       ├── 2_silver/Transformation/ #   Cleansing + normalization
    │       └── 3_gold/Transformation/   #   11 star-schema tables
    │
    ├── redes_sociais/                   # Social media metrics
    │   ├── facebook/
    │   ├── instagram/
    │   └── youtube/
    │
    ├── hotmart_mastertech/              # E-commerce data
    ├── voomp/                           # Platform sales
    ├── calls_Gabriel/                   # Call center analytics
    ├── moedas/                          # Currency exchange rates
    └── pipedrive/                       # CRM pipeline
```

---

## Getting Started

### Prerequisites

- Python 3.13+
- Docker & Docker Compose (optional)
- Azure Storage Account with `bronze`, `silver`, `gold` containers

### Setup

```bash
# Clone
git clone https://github.com/your-user/data-engineering-platform.git
cd data-engineering-platform

# Environment
cp .env.example .env
# Edit .env with your credentials

# Install
pip install -r requirements.txt
```

### Run with Docker

```bash
docker compose run etl
```

### Run locally

```bash
python datamart/1_Orchestrator_Gerneral/Orchestartor_General.py
```

---

## Pipeline Execution

The **General Orchestrator** runs all datamarts sequentially:

```
Facebook → Google → Social Media → Calls → Hotmart → Voomp → Currency
```

Each datamart follows the same pattern:

```
1. Extract     → API/Sheets → Raw files (Bronze container)
2. Transform   → Clean, type cast, deduplicate → Parquet (Silver container)
3. Aggregate   → Star schema, business rules → Parquet/Delta (Gold container)
4. Archive     → Move processed files to *_carregados/
```

### Incremental Loading

- Currency pipeline: fetches only from the latest date in Gold forward
- Facebook Ads: daily extraction scripts append to Bronze, transformers detect new files
- Processed files are moved to `*_carregados/` (loaded) folders to prevent re-processing

### Data Quality

- **MD5 hashing** for surrogate key generation and deduplication
- **Type enforcement** at Silver layer (cast dates, numerics, strings)
- **Null filtering** — records without required fields are dropped at Silver
- **Record-level filtering** — business rules applied at Gold (e.g., exclude test transactions)

---

## Environment Variables

See [`.env.example`](.env.example) for all required variables. Key groups:

| Group | Variables | Purpose |
|-------|-----------|---------|
| Azure | `STORAGE_ACCOUNT_*`, `ORION_API_URL` | Data Lake access |
| Facebook | `FB_*`, `USER_TOKEN` | Ads & Graph API |
| Google | `GOOGLE_ADS_*` | Google Ads extraction |
| Social | `IG_USER_ID`, `API_KEY_youtube` | Instagram + YouTube |
| Others | `PD_API_KEY`, `API_ACCESS_KEY`, `ACTIVECAMPAIGN_*` | CRM, currency, email |

---

## License

Proprietary — IT Valley.
