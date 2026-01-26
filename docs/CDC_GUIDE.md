# CDC (Change Data Capture) Implementation Guide

## Overview

This guide explains the CDC implementation in the Olist Lakehouse project, including the data format, processing patterns, and how to work with SCD Type 1 and Type 2 tables.

## AUTO CDC Syntax

This project uses the **AUTO CDC** syntax (the modern replacement for `APPLY CHANGES`):

```sql
CREATE FLOW flow_name
AS AUTO CDC INTO target_table
FROM stream(source_table)
KEYS (key_column)
SEQUENCE BY sequence_column
STORED AS SCD TYPE 1|2;
```

> **Note**: `AUTO CDC` and `APPLY CHANGES INTO` have identical functionality. AUTO CDC is the current recommended syntax in Databricks Lakeflow.

## CDC Data Format

### Change Event Structure

CDC files must contain the following columns:

| Column | Type | Description |
|--------|------|-------------|
| `sequence_number` | BIGINT | Monotonically increasing sequence for ordering |
| `operation` | STRING | `INSERT`, `UPDATE`, or `DELETE` |
| `change_timestamp` | TIMESTAMP | When the change occurred |
| `<entity_columns>` | Various | All entity-specific columns |

### Example CDC Record

```csv
sequence_number,operation,change_timestamp,customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state
10001,UPDATE,2024-01-15T10:30:00,abc123...,def456...,12345,Sao Paulo,SP
10002,INSERT,2024-01-15T10:31:00,ghi789...,jkl012...,67890,Rio De Janeiro,RJ
10003,DELETE,2024-01-15T10:32:00,mno345...,pqr678...,11111,Salvador,BA
```

## Processing Pipeline

### Bronze Layer: Raw CDC Ingestion

```sql
CREATE OR REFRESH STREAMING TABLE bronze_cdc_customers
AS SELECT
    CAST(sequence_number AS BIGINT) AS sequence_number,
    UPPER(TRIM(operation)) AS operation,
    TO_TIMESTAMP(change_timestamp) AS change_timestamp,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    _metadata.file_path AS _source_file,
    current_timestamp() AS _ingested_at
FROM STREAM read_files(
    '/Volumes/${catalog}/raw/olist/cdc/customers/',
    format => 'csv',
    header => true
);
```

### Staging Layer: Validation

```sql
CREATE OR REFRESH STREAMING TABLE stg_cdc_customers (
    CONSTRAINT valid_customer_id
        EXPECT (customer_id IS NOT NULL)
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_operation
        EXPECT (operation IN ('INSERT', 'UPDATE', 'DELETE'))
        ON VIOLATION DROP ROW,
    CONSTRAINT valid_sequence
        EXPECT (sequence_number IS NOT NULL)
        ON VIOLATION DROP ROW
)
AS SELECT
    sequence_number,
    operation,
    change_timestamp,
    TRIM(customer_id) AS customer_id,
    -- Additional transformations...
FROM STREAM(bronze_cdc_customers);
```

### Silver Layer: SCD Processing

#### SCD Type 1 (Current State)

```sql
-- Create the target streaming table
CREATE OR REFRESH STREAMING TABLE silver_customers_current
COMMENT 'Current state of customers (SCD Type 1)'
TBLPROPERTIES ('quality' = 'silver');

-- Create the AUTO CDC flow
CREATE FLOW customers_current_flow
AS AUTO CDC INTO silver_customers_current
FROM stream(stg_cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 1;
```

**Result**: One row per customer with the latest values.

#### SCD Type 2 (History)

```sql
-- Create the target streaming table
CREATE OR REFRESH STREAMING TABLE silver_customers_history
COMMENT 'Historical customer changes (SCD Type 2)'
TBLPROPERTIES ('quality' = 'silver');

-- Create the AUTO CDC flow
CREATE FLOW customers_history_flow
AS AUTO CDC INTO silver_customers_history
FROM stream(stg_cdc_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation, change_timestamp, _ingested_at)
STORED AS SCD TYPE 2;
```

**Result**: Multiple rows per customer with validity periods.

## AUTO CDC Syntax Reference

### Complete Syntax

```sql
CREATE FLOW flow_name
AS AUTO CDC INTO target_table
FROM stream(source_table)
KEYS (column1[, column2, ...])
[APPLY AS DELETE WHEN condition]
[APPLY AS TRUNCATE WHEN condition]
SEQUENCE BY sequence_column
[COLUMNS column_list | * [EXCEPT (column_list)]]
STORED AS SCD TYPE 1|2
[TRACK HISTORY ON column_list | * [EXCEPT (column_list)]];
```

### Key Components

| Clause | Required | Description |
|--------|----------|-------------|
| `CREATE FLOW name` | Yes | Names the CDC processing flow |
| `AS AUTO CDC INTO` | Yes | Specifies the target table |
| `FROM stream(source)` | Yes | Source table wrapped in `stream()` |
| `KEYS (columns)` | Yes | Primary key columns |
| `APPLY AS DELETE WHEN` | No | Condition for DELETE operations |
| `APPLY AS TRUNCATE WHEN` | No | Condition for TRUNCATE operations |
| `SEQUENCE BY` | Yes | Column(s) for ordering events |
| `COLUMNS` | No | Columns to include/exclude |
| `STORED AS SCD TYPE` | Yes | 1 for current state, 2 for history |
| `TRACK HISTORY ON` | No | SCD Type 2 only: columns to track |

### Selective History Tracking (SCD Type 2)

Exclude columns from creating new history versions:

```sql
CREATE FLOW customers_history_flow
AS AUTO CDC INTO silver_customers_history
FROM stream(stg_cdc_customers)
KEYS (customer_id)
SEQUENCE BY sequence_number
COLUMNS * EXCEPT (sequence_number, operation)
STORED AS SCD TYPE 2
TRACK HISTORY ON * EXCEPT (customer_city);  -- City changes don't create new versions
```

## SCD Type 2 Generated Columns

When using `STORED AS SCD TYPE 2`, Databricks automatically adds:

| Column | Description |
|--------|-------------|
| `__START_AT` | Timestamp when the record version became active |
| `__END_AT` | Timestamp when the record version was superseded (NULL for current) |
| `__IS_CURRENT` | Boolean flag indicating if this is the current version |

### Querying SCD Type 2 Data

```sql
-- Get current customer state
SELECT * FROM silver_customers_history
WHERE __IS_CURRENT = TRUE;

-- Get customer state at a specific point in time
SELECT * FROM silver_customers_history
WHERE customer_id = 'abc123'
  AND __START_AT <= '2024-01-15'
  AND (__END_AT IS NULL OR __END_AT > '2024-01-15');

-- Get all historical changes for a customer
SELECT * FROM silver_customers_history
WHERE customer_id = 'abc123'
ORDER BY __START_AT;
```

## Sequencing Best Practices

### Single Column Sequencing

Use when you have a single ordering column:

```sql
SEQUENCE BY sequence_number
```

### Multi-Column Sequencing

Use when ordering requires multiple columns:

```sql
SEQUENCE BY STRUCT(date_column, sequence_number)
```

### Requirements

- Sequence values must be **sortable** (INT, BIGINT, TIMESTAMP)
- Sequence values must be **unique per key**
- **NULL values are not supported** for sequencing

## Handling Out-of-Order Events

AUTO CDC automatically handles out-of-order events using the sequence column:

```
Events arrive: seq=3, seq=1, seq=2
Lakeflow processes: seq=1, seq=2, seq=3
```

No additional configuration needed.

## Delete Handling

### Soft Deletes

Records are marked as deleted but retained in SCD Type 2:

```sql
APPLY AS DELETE WHEN operation = 'DELETE'
```

### Hard Deletes

For SCD Type 1, deleted records are removed. For SCD Type 2, a final version is created with `__END_AT` set.

### Truncate Operations

Handle bulk deletions:

```sql
APPLY AS TRUNCATE WHEN operation = 'TRUNCATE'
```

## Testing CDC Processing

### Generate Test Data

Run the data generator notebook:

```python
# src/utils/data_generator.py
# Generates initial load + CDC batches with INSERT/UPDATE/DELETE operations
```

### Verify SCD Type 1

```sql
-- Count should match unique customers
SELECT COUNT(*) FROM silver_customers_current;

-- Should have latest values
SELECT * FROM silver_customers_current
WHERE customer_id = 'test_customer_id';
```

### Verify SCD Type 2

```sql
-- Should have multiple versions
SELECT
    customer_id,
    COUNT(*) as versions,
    SUM(CASE WHEN __IS_CURRENT THEN 1 ELSE 0 END) as current_versions
FROM silver_customers_history
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Verify no gaps in validity periods
SELECT *
FROM silver_customers_history t1
JOIN silver_customers_history t2
  ON t1.customer_id = t2.customer_id
  AND t1.__END_AT = t2.__START_AT
WHERE t1.__IS_CURRENT = FALSE;
```

## Monitoring CDC Performance

### Key Metrics

1. **Processing Latency**: Time from CDC file arrival to table update
2. **Out-of-Order Events**: Count of events processed out of sequence
3. **Sequence Gaps**: Missing sequence numbers
4. **Duplicate Keys**: Records with duplicate sequence values

### Pipeline Event Log

```sql
SELECT * FROM event_log(
    TABLE(olist_cdc_pipeline)
)
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```

## Migration from APPLY CHANGES

If migrating from `APPLY CHANGES INTO` to `CREATE FLOW AS AUTO CDC`:

| Old Syntax | New Syntax |
|------------|------------|
| `APPLY CHANGES INTO target` | `CREATE FLOW name AS AUTO CDC INTO target` |
| `FROM STREAM source` | `FROM stream(source)` |
| (same) | `KEYS (columns)` |
| (same) | `SEQUENCE BY column` |
| (same) | `STORED AS SCD TYPE 1\|2` |

**Example migration:**

```sql
-- Old (deprecated)
APPLY CHANGES INTO silver_customers_current
FROM STREAM stg_cdc_customers
KEYS (customer_id)
SEQUENCE BY sequence_number
STORED AS SCD TYPE 1;

-- New (recommended)
CREATE FLOW customers_current_flow
AS AUTO CDC INTO silver_customers_current
FROM stream(stg_cdc_customers)
KEYS (customer_id)
SEQUENCE BY sequence_number
STORED AS SCD TYPE 1;
```

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Missing updates | NULL sequence values | Ensure sequence is never NULL |
| Duplicate records | Same sequence for same key | Ensure unique sequence per key |
| Wrong order | Non-sortable sequence type | Use INT/BIGINT/TIMESTAMP |
| Pipeline failure | Invalid operation value | Validate operation in staging |

### Debug Queries

```sql
-- Check for NULL sequences
SELECT COUNT(*) FROM bronze_cdc_customers
WHERE sequence_number IS NULL;

-- Check for duplicate sequences
SELECT customer_id, sequence_number, COUNT(*)
FROM bronze_cdc_customers
GROUP BY customer_id, sequence_number
HAVING COUNT(*) > 1;

-- Check operation distribution
SELECT operation, COUNT(*)
FROM bronze_cdc_customers
GROUP BY operation;
```

## Production Recommendations

1. **Monitor sequence gaps** - May indicate missed events
2. **Set up alerts** for high latency or error rates
3. **Use staging table** for validation before CDC processing
4. **Test with realistic data volumes** before production
5. **Consider retention policies** for SCD Type 2 history
6. **Use named flows** - Makes pipeline debugging easier
7. **Validate in staging** - Drop invalid records before CDC processing

## References

- [Databricks AUTO CDC Documentation](https://docs.databricks.com/aws/en/ldp/cdc)
- [Lakeflow Best Practices](https://docs.databricks.com/en/lakeflow/index.html)
