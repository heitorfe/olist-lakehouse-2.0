# Deployment Guide

## Prerequisites

### Required Access

- Databricks workspace with Unity Catalog enabled
- Permissions to create catalogs, schemas, and volumes
- Databricks CLI installed (v0.200+)

### Environment Setup

```bash
# Install Databricks CLI
pip install databricks-cli

# Or using Homebrew (macOS)
brew install databricks/tap/databricks

# Configure authentication
databricks configure --token
# Enter your workspace URL and personal access token
```

## Deployment Workflow

### 1. Clone and Configure

```bash
# Clone the repository
git clone https://github.com/yourusername/olist-lakehouse-2.0.git
cd olist-lakehouse-2.0

# Set environment variables
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=your-personal-access-token
```

### 2. Validate Configuration

```bash
# Validate the bundle configuration
databricks bundle validate

# Expected output:
# Name: olist-lakehouse
# Target: dev
# Workspace:
#   Host: https://your-workspace.cloud.databricks.com
```

### 3. Deploy to Development

```bash
# Deploy all resources to development environment
databricks bundle deploy -t dev

# This will create:
# - [DEV] Olist Lakehouse - Main Pipeline
# - [DEV] Olist Lakehouse - CDC Pipeline
```

### 4. Initialize Unity Catalog

In your Databricks workspace:

1. Navigate to **Workspace** > **Users** > your username
2. Import `src/setup/unity_catalog_setup.py`
3. Run all cells to create:
   - `olist_dev` catalog
   - `raw`, `bronze`, `silver`, `gold` schemas
   - `olist` volume for data files

### 5. Upload Sample Data

Download the [Olist dataset from Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and upload to:

```
/Volumes/olist_dev/raw/olist/
├── customers/olist_customers_dataset.csv
├── orders/olist_orders_dataset.csv
├── order_items/olist_order_items_dataset.csv
├── order_payments/olist_order_payments_dataset.csv
├── order_reviews/olist_order_reviews_dataset.csv
├── products/olist_products_dataset.csv
├── sellers/olist_sellers_dataset.csv
└── geolocation/olist_geolocation_dataset.csv
```

Or generate synthetic data:

1. Import `src/utils/data_generator.py`
2. Run the notebook to generate test data

### 6. Run Pipelines

```bash
# Run the main ETL pipeline
databricks bundle run olist_main_pipeline -t dev

# Run the CDC pipeline
databricks bundle run olist_cdc_pipeline -t dev
```

## Environment-Specific Deployment

### Development

```bash
databricks bundle deploy -t dev
databricks bundle run olist_main_pipeline -t dev
```

Features:
- Development mode enabled
- Smaller cluster (1-4 workers)
- Pipeline names prefixed with `[DEV]`
- Full refresh on each run

### Staging

```bash
databricks bundle deploy -t staging
databricks bundle run olist_main_pipeline -t staging
```

Features:
- Development mode disabled
- Catalog: `olist_staging`
- Pipeline names prefixed with `[STAGING]`

### Production

```bash
databricks bundle deploy -t prod
databricks bundle run olist_main_pipeline -t prod
```

Features:
- Production mode
- Larger cluster (2-8 workers)
- Photon enabled
- Root path: `/Shared/olist-lakehouse`
- Restricted permissions

## Pipeline Management

### View Pipeline Status

```bash
# List all pipelines
databricks pipelines list

# Get pipeline details
databricks pipelines get <pipeline-id>
```

### Trigger Pipeline Updates

```bash
# Full refresh
databricks pipelines start-update <pipeline-id> --full-refresh

# Incremental update
databricks pipelines start-update <pipeline-id>
```

### Stop Pipeline

```bash
databricks pipelines stop <pipeline-id>
```

## Monitoring

### Pipeline UI

1. Navigate to **Workflows** > **Delta Live Tables**
2. Select your pipeline
3. View:
   - Execution history
   - Data quality metrics
   - Lineage graph

### Query Event Log

```sql
SELECT
    timestamp,
    event_type,
    message
FROM event_log(TABLE(olist_main_pipeline))
WHERE timestamp > current_timestamp() - INTERVAL 1 DAY
ORDER BY timestamp DESC;
```

## Troubleshooting

### Common Issues

#### Bundle Validation Fails

```bash
# Check for syntax errors
databricks bundle validate -t dev

# Common fixes:
# - Ensure YAML indentation is correct
# - Verify variable references: ${var.catalog}
# - Check file paths exist
```

#### Permission Denied

```sql
-- Grant necessary permissions
GRANT USE CATALOG ON CATALOG olist_dev TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA olist_dev.bronze TO `user@example.com`;
GRANT SELECT ON SCHEMA olist_dev.bronze TO `user@example.com`;
```

#### Pipeline Stuck

```bash
# Stop and restart
databricks pipelines stop <pipeline-id>
databricks pipelines start-update <pipeline-id> --full-refresh
```

#### Schema Evolution Issues

```sql
-- Check for schema mismatches
DESCRIBE EXTENDED olist_dev.bronze.bronze_customers;

-- If needed, drop and recreate (dev only!)
DROP TABLE IF EXISTS olist_dev.bronze.bronze_customers;
```

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Deploy Olist Lakehouse

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Validate Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle validate -t dev

  deploy-dev:
    needs: validate
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy to Dev
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle deploy -t dev
```

## Cleanup

### Remove Development Resources

```bash
# Destroy deployed resources
databricks bundle destroy -t dev

# This removes:
# - Pipelines
# - Jobs
# - Associated metadata
```

### Drop Catalog (Manual)

```sql
-- WARNING: This deletes all data!
DROP CATALOG IF EXISTS olist_dev CASCADE;
```

## Cost Optimization

### Development

- Use `development: true` for faster iterations
- Keep clusters small (1-4 workers)
- Use triggered mode (not continuous)

### Production

- Enable Photon for better performance
- Use serverless for cost efficiency
- Schedule pipelines during off-peak hours
- Monitor DBU consumption
