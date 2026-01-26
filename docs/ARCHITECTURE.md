# Olist Lakehouse Architecture

## Overview

This document describes the architecture of the Olist Lakehouse project, including design decisions, data flow patterns, and implementation details.

## Design Principles

### 1. Medallion Architecture

The project follows the Medallion Architecture pattern, organizing data into three logical layers:

```
┌─────────────────────────────────────────────────────────────────┐
│                     MEDALLION ARCHITECTURE                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   ┌──────────┐      ┌──────────┐      ┌──────────┐             │
│   │  BRONZE  │ ───▶ │  SILVER  │ ───▶ │   GOLD   │             │
│   │          │      │          │      │          │             │
│   │ Raw Data │      │ Cleansed │      │   KPIs   │             │
│   │ Landing  │      │ Validated│      │ Business │             │
│   │   Zone   │      │   Data   │      │  Metrics │             │
│   └──────────┘      └──────────┘      └──────────┘             │
│        │                 │                 │                    │
│   - Schema on      - Type casting    - Aggregations            │
│     read           - Null handling   - Joins                   │
│   - Metadata       - Deduplication   - Business logic          │
│     tracking       - Normalization   - Pre-computed            │
│   - Audit cols     - Validation        metrics                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 2. Separation of Concerns

| Pipeline | Purpose | Tables |
|----------|---------|--------|
| **Main Pipeline** | Append-only transactional data | 22 tables/views |
| **CDC Pipeline** | Dimension updates (SCD 1 & 2) | 9 tables |

### 3. Data Quality Progression

```
Bronze (WARN) → Silver (DROP) → Gold (FAIL)
     ↓              ↓              ↓
  Track          Remove        Strict
  issues       bad data     validation
```

## Entity Classification

### Append-Only Entities (AutoLoader)

These entities are immutable after creation and use simple streaming ingestion:

| Entity | Rationale |
|--------|-----------|
| `orders` | Orders don't change after creation |
| `order_items` | Line items are fixed at order time |
| `order_payments` | Payments are recorded once |
| `order_reviews` | Reviews are submitted once |
| `geolocation` | Reference data, infrequent updates |

### CDC Entities (AUTO CDC)

These entities receive updates over time and require change tracking:

| Entity | Change Types | SCD Strategy |
|--------|--------------|--------------|
| `customers` | Address changes, profile updates | Type 1 + Type 2 |
| `products` | Category changes, dimension updates | Type 1 + Type 2 |
| `sellers` | Location changes, profile updates | Type 1 + Type 2 |

## CDC Implementation

### Change Data Capture Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                      CDC DATA FLOW                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Source System                                                   │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────┐   CDC Feed Format:                             │
│  │  CDC Files  │   - sequence_number (ordering)                 │
│  │   (CSV)     │   - operation (INSERT/UPDATE/DELETE)           │
│  └─────────────┘   - change_timestamp                           │
│       │            - entity fields                               │
│       ▼                                                          │
│  ┌─────────────┐                                                │
│  │   Bronze    │   Raw CDC events via AutoLoader                │
│  │  CDC Table  │                                                │
│  └─────────────┘                                                │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────┐                                                │
│  │   Staging   │   Validated, transformed CDC events            │
│  │    Table    │                                                │
│  └─────────────┘                                                │
│       │                                                          │
│       ├──────────────────┬──────────────────┐                   │
│       ▼                  ▼                  ▼                   │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐           │
│  │  SCD Type 1 │   │  SCD Type 2 │   │   (Future)  │           │
│  │   Current   │   │   History   │   │   Snapshot  │           │
│  └─────────────┘   └─────────────┘   └─────────────┘           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### SCD Type 1 vs Type 2

| Aspect | SCD Type 1 | SCD Type 2 |
|--------|------------|------------|
| **History** | No history retained | Full history |
| **Storage** | Lower (one row per key) | Higher (multiple rows per key) |
| **Use Case** | Current state queries | Point-in-time analysis |
| **Generated Columns** | None | `__START_AT`, `__END_AT`, `__IS_CURRENT` |

## Data Quality Implementation

### Expectation Patterns

```sql
-- Bronze: Track issues (WARN)
CONSTRAINT no_rescued_data
    EXPECT (_rescued_data IS NULL)

-- Silver: Remove bad data (DROP)
CONSTRAINT valid_customer_id
    EXPECT (customer_id IS NOT NULL AND LENGTH(TRIM(customer_id)) = 32)
    ON VIOLATION DROP ROW

-- Gold: Strict validation (FAIL)
CONSTRAINT positive_revenue
    EXPECT (total_revenue >= 0)
    ON VIOLATION FAIL UPDATE
```

### Validation Rules by Entity

| Entity | Key Validations |
|--------|----------------|
| `customers` | Valid UUID format, non-null zip code |
| `orders` | Valid status enum, valid timestamps |
| `order_items` | Positive price, valid foreign keys |
| `order_payments` | Valid payment type, positive value |
| `order_reviews` | Score 1-5, valid order reference |

## Silver Layer Transformations

### Data Cleansing

1. **String Normalization**
   - `TRIM()` for whitespace removal
   - `UPPER()` for state codes
   - `INITCAP()` for city names
   - `LOWER()` for categories

2. **Type Casting**
   - Strings to INT/DECIMAL where appropriate
   - Timestamp parsing with `TO_TIMESTAMP()`

3. **Derived Fields**
   - `delivery_days`: Days from purchase to delivery
   - `sentiment`: Positive/Neutral/Negative from score
   - `region`: Brazilian region from state code
   - `size_category`: Product size classification

## Gold Layer Design

### KPI Tables

| Table | Grain | Key Metrics |
|-------|-------|-------------|
| `gold_daily_orders` | Day | Revenue, volume, delivery rates |
| `gold_monthly_orders` | Month | Trends, MoM comparisons |
| `gold_orders_by_state` | Day × State | Geographic distribution |
| `gold_seller_performance` | Day × Seller | GMV, ratings by seller |
| `gold_category_performance` | Month × Category | Category trends |

### Enrichment Pattern

The `silver_orders_enriched` view denormalizes order data for efficient gold layer processing:

```sql
silver_orders
    + order_items_agg (item counts, totals)
    + order_payments_agg (payment summary)
    + order_reviews_agg (review metrics)
    = silver_orders_enriched
```

## Unity Catalog Structure

```
olist_dev (Catalog)
├── raw (Schema)
│   └── olist (Volume)
│       ├── customers/
│       ├── orders/
│       ├── ...
│       └── cdc/
│           ├── customers/
│           ├── products/
│           └── sellers/
├── bronze (Schema)
│   ├── bronze_customers
│   ├── bronze_orders
│   └── ...
├── silver (Schema)
│   ├── silver_customers
│   ├── silver_customers_current (CDC)
│   ├── silver_customers_history (CDC)
│   └── ...
└── gold (Schema)
    ├── gold_daily_orders
    ├── gold_monthly_orders
    └── ...
```

## Performance Considerations

### Auto Loader Benefits

- **Incremental Processing**: Only new files are processed
- **Schema Evolution**: Automatic schema inference and evolution
- **Exactly-Once**: Guaranteed processing via checkpointing
- **Scalability**: Handles millions of files efficiently

### Optimization Techniques

1. **Table Properties**
   ```sql
   TBLPROPERTIES (
       'pipelines.autoOptimize.managed' = 'true',
       'pipelines.autoOptimize.zOrderCols' = 'order_id'
   )
   ```

2. **Clustering**
   ```sql
   CLUSTER BY AUTO
   ```

3. **Materialized Views** for expensive aggregations

## Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DEPLOYMENT TARGETS                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Development          Staging            Production            │
│   ┌──────────┐        ┌──────────┐       ┌──────────┐          │
│   │ olist_dev│        │olist_stg │       │  olist   │          │
│   │          │        │          │       │          │          │
│   │ Dev Mode │  ───▶  │ Test Mode│  ───▶ │Prod Mode │          │
│   │   ON     │        │   OFF    │       │   OFF    │          │
│   │          │        │          │       │          │          │
│   │ 1-4 nodes│        │ 1-4 nodes│       │ 2-8 nodes│          │
│   └──────────┘        └──────────┘       └──────────┘          │
│                                                                  │
│   databricks bundle deploy -t dev                               │
│   databricks bundle deploy -t staging                           │
│   databricks bundle deploy -t prod                              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Future Enhancements

1. **Real-time Streaming**: Kafka/EventHubs integration
2. **ML Feature Store**: Feature engineering pipeline
3. **Data Quality Dashboard**: Monitoring and alerting
4. **Cost Optimization**: Automated scaling policies
5. **Data Masking**: PII protection for sensitive fields
