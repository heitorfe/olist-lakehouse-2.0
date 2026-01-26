# Olist Lakehouse 2.0

A production-ready **Databricks Lakehouse** portfolio project demonstrating modern data engineering practices using **Lakeflow Declarative Pipelines** (formerly Delta Live Tables), **Unity Catalog**, and the **Medallion Architecture**.

## Overview

This project implements a complete data lakehouse solution for the [Olist Brazilian E-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), showcasing:

- **AutoLoader** for incremental data ingestion
- **AUTO CDC** (Change Data Capture) with SCD Type 1 and Type 2
- **Data Quality** with expectations at each layer
- **Unity Catalog** for governance and lineage
- **Databricks Asset Bundles** for CI/CD deployment

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            OLIST LAKEHOUSE 2.0                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────┐ │
│  │   RAW DATA   │    │    BRONZE    │    │    SILVER    │    │    GOLD    │ │
│  │   (Volume)   │───▶│  (Raw Load)  │───▶│  (Cleansed)  │───▶│   (KPIs)   │ │
│  └──────────────┘    └──────────────┘    └──────────────┘    └────────────┘ │
│         │                   │                   │                   │        │
│         │            AutoLoader          Data Quality        Aggregations    │
│         │            Streaming           Expectations        Business Logic  │
│         │                                                                    │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │   CDC FEED   │───▶│  CDC BRONZE  │───▶│  CDC SILVER  │                   │
│  │   (Volume)   │    │   (Changes)  │    │  (SCD 1 & 2) │                   │
│  └──────────────┘    └──────────────┘    └──────────────┘                   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        UNITY CATALOG                                  │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐ │   │
│  │  │ Catalog │  │ Schemas │  │ Tables  │  │ Volumes │  │  Lineage    │ │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘  └─────────────┘ │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Model

### Entities

| Entity | Type | Description |
|--------|------|-------------|
| **customers** | CDC | Customer profiles with location data |
| **products** | CDC | Product catalog with dimensions |
| **sellers** | CDC | Seller profiles and locations |
| **orders** | Append-only | Order transactions |
| **order_items** | Append-only | Order line items |
| **order_payments** | Append-only | Payment records |
| **order_reviews** | Append-only | Customer reviews |
| **geolocation** | Reference | Geographic lookup data |

### Medallion Layers

| Layer | Purpose | Data Quality |
|-------|---------|--------------|
| **Bronze** | Raw ingestion via AutoLoader | WARN (track issues) |
| **Silver** | Cleansed, validated, transformed | DROP (remove bad data) |
| **Gold** | Business KPIs and aggregations | FAIL (strict validation) |

## Project Structure

```
olist-lakehouse-2.0/
├── databricks.yml              # Databricks Asset Bundle configuration
├── README.md                   # This file
│
├── src/
│   ├── setup/
│   │   └── unity_catalog_setup.py   # Unity Catalog initialization
│   │
│   ├── pipelines/
│   │   ├── bronze/             # AutoLoader ingestion (8 tables)
│   │   │   ├── customers.sql
│   │   │   ├── orders.sql
│   │   │   ├── order_items.sql
│   │   │   ├── order_payments.sql
│   │   │   ├── order_reviews.sql
│   │   │   ├── products.sql
│   │   │   ├── sellers.sql
│   │   │   └── geolocation.sql
│   │   │
│   │   ├── silver/             # Data quality & transformation (9 tables)
│   │   │   ├── customers.sql
│   │   │   ├── orders.sql
│   │   │   ├── order_items.sql
│   │   │   ├── order_payments.sql
│   │   │   ├── order_reviews.sql
│   │   │   ├── products.sql
│   │   │   ├── sellers.sql
│   │   │   ├── geolocation.sql
│   │   │   └── orders_enriched.sql
│   │   │
│   │   ├── gold/               # Business KPIs (5 views)
│   │   │   ├── daily_orders.sql
│   │   │   ├── monthly_orders.sql
│   │   │   ├── orders_by_state.sql
│   │   │   ├── seller_performance.sql
│   │   │   └── category_performance.sql
│   │   │
│   │   └── cdc/                # CDC processing (6 files)
│   │       ├── cdc_customers_bronze.sql
│   │       ├── cdc_customers_silver.sql
│   │       ├── cdc_products_bronze.sql
│   │       ├── cdc_products_silver.sql
│   │       ├── cdc_sellers_bronze.sql
│   │       └── cdc_sellers_silver.sql
│   │
│   └── utils/
│       └── data_generator.py   # Synthetic data generator
│
├── resources/
│   └── olist_pipeline.yml      # Pipeline definitions
│
└── tests/                      # Unit and integration tests
```

## Key Features

### 1. AutoLoader (Append-Only Ingestion)

```sql
-- Bronze layer uses Auto Loader for incremental file processing
CREATE OR REFRESH STREAMING TABLE bronze_orders
AS SELECT
    *,
    _metadata.file_path AS _source_file,
    _metadata.file_modification_time AS _file_modified_at,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/raw/olist/orders/',
    format => 'csv',
    header => true,
    inferSchema => true
);
```

### 2. AUTO CDC (Change Data Capture)

```sql
-- SCD Type 1: Current state only
CREATE OR REFRESH STREAMING TABLE silver_customers_current;

APPLY CHANGES INTO silver_customers_current
FROM STREAM stg_cdc_customers
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
STORED AS SCD TYPE 1;

-- SCD Type 2: Full history with validity periods
CREATE OR REFRESH STREAMING TABLE silver_customers_history;

APPLY CHANGES INTO silver_customers_history
FROM STREAM stg_cdc_customers
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
STORED AS SCD TYPE 2;
```

### 3. Data Quality Expectations

```sql
-- Silver layer with data quality constraints
CREATE OR REFRESH STREAMING TABLE silver_orders (
    CONSTRAINT valid_order_id
        EXPECT (order_id IS NOT NULL AND LENGTH(TRIM(order_id)) = 32)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_order_status
        EXPECT (order_status IN ('created', 'approved', 'invoiced', ...))
        ON VIOLATION DROP ROW
)
AS SELECT ...
```

### 4. Gold Layer KPIs

- **Daily Orders**: Order volume, revenue, delivery metrics by day
- **Monthly Orders**: Trend analysis with month-over-month comparisons
- **Orders by State**: Geographic distribution and regional performance
- **Seller Performance**: GMV, ratings, and customer reach by seller
- **Category Performance**: Product category trends and rankings

## Deployment

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI installed and configured
- Access to create catalogs, schemas, and volumes

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/olist-lakehouse-2.0.git
cd olist-lakehouse-2.0

# 2. Configure Databricks CLI
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-token

# 3. Validate the bundle
databricks bundle validate

# 4. Deploy to development
databricks bundle deploy -t dev

# 5. Run the Unity Catalog setup notebook
# (Execute src/setup/unity_catalog_setup.py in your workspace)

# 6. Generate test data
# (Execute src/utils/data_generator.py in your workspace)

# 7. Run the main pipeline
databricks bundle run olist_main_pipeline -t dev

# 8. Run the CDC pipeline
databricks bundle run olist_cdc_pipeline -t dev
```

### Environment Targets

| Target | Catalog | Use Case |
|--------|---------|----------|
| `dev` | olist_dev | Development and testing |
| `staging` | olist_staging | Pre-production validation |
| `prod` | olist | Production workloads |

## Data Flow

### Initial Load (Append-Only Pipeline)

```
CSV Files → AutoLoader → Bronze Tables → Silver Tables → Gold Views
                            ↓               ↓              ↓
                        Raw Data      Cleansed Data    Business KPIs
```

### CDC Flow (Change Data Capture Pipeline)

```
CDC CSV Files → AutoLoader → Bronze CDC → Staging → SCD Type 1 (Current)
                                              ↓
                                         SCD Type 2 (History)
```

## Data Quality Strategy

| Layer | Policy | Behavior |
|-------|--------|----------|
| Bronze | `EXPECT` | Warn and track violations |
| Silver | `EXPECT ... ON VIOLATION DROP ROW` | Remove invalid records |
| Gold | `EXPECT ... ON VIOLATION FAIL UPDATE` | Fail pipeline on violations |

## Monitoring

### Pipeline Metrics

- View pipeline execution in the Databricks UI
- Monitor data quality metrics in the lineage graph
- Track expectation violations in pipeline logs

### Key Metrics to Monitor

- **Ingestion Rate**: Records processed per minute
- **Data Quality**: Violation counts by expectation
- **Latency**: Time from file arrival to gold layer
- **Cost**: DBU consumption per pipeline run

## Technologies Used

- **Databricks Lakeflow** (Delta Live Tables)
- **Unity Catalog** for governance
- **Auto Loader** for incremental ingestion
- **Delta Lake** for ACID transactions
- **Databricks Asset Bundles** for deployment

## Best Practices Implemented

- ✅ Serverless pipelines for cost optimization
- ✅ Unity Catalog for data governance
- ✅ Parameterized configurations for environment flexibility
- ✅ Data quality expectations at each layer
- ✅ SCD Type 1 and Type 2 for historical tracking
- ✅ Audit columns for data lineage
- ✅ Medallion architecture for clear data organization

## License

This project is for educational and portfolio purposes.

## Acknowledgments

- [Olist](https://olist.com/) for the Brazilian E-commerce dataset
- [Databricks](https://databricks.com/) for the Lakehouse platform
