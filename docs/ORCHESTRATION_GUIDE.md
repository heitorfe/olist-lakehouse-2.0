# Orchestration Guide

This document describes the Lakeflow Jobs orchestration for the Olist Lakehouse project, including daily data generation and pipeline execution.

## Table of Contents

- [Overview](#overview)
- [Workflow Architecture](#workflow-architecture)
- [Jobs](#jobs)
  - [Daily Orchestration Job](#daily-orchestration-job)
  - [Weekly Full Refresh Job](#weekly-full-refresh-job)
- [Tasks](#tasks)
- [Environment Configuration](#environment-configuration)
- [Operations](#operations)
  - [Manual Execution](#manual-execution)
  - [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

---

## Overview

The orchestration layer automates the daily ETL workflow for the Olist Lakehouse:

1. **Data Generation** - Generates synthetic CDC and transactional data
2. **Main ETL Pipeline** - Processes data through Bronze -> Silver -> Gold layers
3. **CDC Pipeline** - Applies SCD Type 1 and Type 2 transformations

### Key Features

| Feature | Description |
|---------|-------------|
| **Scheduled Execution** | Daily at 06:00 UTC (production) |
| **Parallel Processing** | Main and CDC pipelines run in parallel after data generation |
| **Environment Isolation** | Separate configurations for dev/staging/prod |
| **Failure Notifications** | Email alerts on job failure |
| **Retry Logic** | Automatic retry for data generation task |

---

## Workflow Architecture

```
                     DAILY ORCHESTRATION JOB
                     Schedule: 06:00 UTC

+----------------------------------------------------------------+
|                                                                |
|   [1] generate_data                                            |
|       Notebook: src/utils/data_generator.py                    |
|       Duration: ~10-30 min                                     |
|       Output: CSV files in /Volumes/{catalog}/raw/olist/       |
|                                                                |
+-------------------------------+--------------------------------+
                                |
               depends_on       |
           +--------------------+--------------------+
           |                                         |
           v                                         v
+------------------------+            +------------------------+
| [2] run_main_pipeline  |            | [3] run_cdc_pipeline   |
|                        |            |                        |
| Pipeline:              |            | Pipeline:              |
| olist_main_pipeline    |            | olist_cdc_pipeline     |
|                        |            |                        |
| Tables:                |            | Tables:                |
| - 8 Bronze tables      |            | - 3 CDC Bronze tables  |
| - 9 Silver tables      |            | - 6 CDC Silver tables  |
| - 5 Gold views         |            |   (SCD Type 1 & 2)     |
|                        |            |                        |
| Duration: ~30-60 min   |            | Duration: ~15-30 min   |
+------------------------+            +------------------------+
           |                                         |
           +--------------------+--------------------+
                                |
                                v
                        [Job Complete]
                     Total: ~1-2 hours
```

### Data Flow

```
Data Generator                 Main Pipeline                    CDC Pipeline
+-------------+               +---------------+               +---------------+
| customers/  | -- ingest --> | bronze_*      | -- N/A        |               |
| products/   |               | silver_*      |               |               |
| sellers/    |               | gold_*        |               |               |
| orders/     |               +---------------+               |               |
| ...         |                                               |               |
+-------------+               +---------------+               +---------------+
| cdc/        | ------------- | N/A           | -- ingest --> | bronze_cdc_*  |
| customers/  |               |               |               | stg_cdc_*     |
| products/   |               |               |               | silver_*      |
| sellers/    |               |               |               |   _current    |
+-------------+               +---------------+               |   _history    |
                                                              +---------------+
```

---

## Jobs

### Daily Orchestration Job

**Resource:** `olist_daily_job`

**Purpose:** Automates the complete daily ETL workflow including data generation and pipeline execution.

**Configuration:**

| Setting | Value | Description |
|---------|-------|-------------|
| Schedule | `0 0 6 * * ?` | Daily at 06:00 UTC |
| Timeout | 3 hours (10,800s) | Maximum job duration |
| Max Concurrent | 1 | Prevents overlapping runs |
| Retry | 1 retry for generate_data | Handles transient failures |

**CRON Expression Breakdown:**

```
0 0 6 * * ?
| | | | | |
| | | | | +-- Day of week (? = any)
| | | | +---- Month (* = every)
| | | +------ Day of month (* = every)
| | +-------- Hour (6 = 6 AM)
| +---------- Minute (0)
+------------ Second (0)
```

---

### Weekly Full Refresh Job

**Resource:** `olist_weekly_refresh_job`

**Purpose:** Performs a complete refresh of all pipeline tables. Useful for:
- Recovering from data corruption
- Applying schema changes
- Resetting test environments

**Configuration:**

| Setting | Value | Description |
|---------|-------|-------------|
| Schedule | `0 0 2 ? * SUN` | Every Sunday at 02:00 UTC |
| Timeout | 6 hours (21,600s) | Extended for full refresh |
| Status | PAUSED (default) | Enable when needed |

**Note:** This job is paused by default. Enable it manually when a full refresh is required.

---

## Tasks

### Task 1: generate_data

**Type:** `notebook_task`

**Notebook:** `src/utils/data_generator.py`

**Parameters:**

| Parameter | Value | Description |
|-----------|-------|-------------|
| `catalog` | `${var.catalog}` | Target Unity Catalog (environment-specific) |

**Generated Data:**

| Data Type | Location | Description |
|-----------|----------|-------------|
| Initial Load | `/Volumes/{catalog}/raw/olist/{entity}/` | Customers, products, sellers, orders, etc. |
| CDC Events | `/Volumes/{catalog}/raw/olist/cdc/{entity}/` | Change events with INSERT/UPDATE/DELETE |

**Retry Configuration:**
- Max retries: 1
- Retry interval: 60 seconds
- Retry on timeout: No

---

### Task 2: run_main_pipeline

**Type:** `pipeline_task`

**Pipeline:** `olist_main_pipeline`

**Depends On:** `generate_data`

**Refresh Mode:** Incremental (default) or Full Refresh

**Tables Processed:**

| Layer | Tables | Count |
|-------|--------|-------|
| Bronze | `bronze_customers`, `bronze_orders`, `bronze_products`, etc. | 8 |
| Silver | `silver_customers`, `silver_orders`, `silver_orders_enriched`, etc. | 9 |
| Gold | `gold_daily_orders`, `gold_monthly_orders`, `gold_seller_performance`, etc. | 5 |

---

### Task 3: run_cdc_pipeline

**Type:** `pipeline_task`

**Pipeline:** `olist_cdc_pipeline`

**Depends On:** `generate_data`

**Refresh Mode:** Incremental (default) or Full Refresh

**Tables Processed:**

| Layer | Tables | Description |
|-------|--------|-------------|
| Bronze | `bronze_cdc_customers`, `bronze_cdc_products`, `bronze_cdc_sellers` | Raw CDC events |
| Staging | `stg_cdc_customers`, `stg_cdc_products`, `stg_cdc_sellers` | Validated events |
| Silver | `silver_*_current` (SCD Type 1), `silver_*_history` (SCD Type 2) | 6 tables |

---

## Environment Configuration

### Development

| Setting | Value |
|---------|-------|
| Job Name | `[DEV] Olist Lakehouse - Daily Orchestration` |
| Schedule | **PAUSED** |
| Catalog | `olist_dev` |
| Notifications | None |

**Notes:**
- Schedule is paused to prevent automatic runs
- Use manual execution for testing
- No email notifications (avoid noise during development)

---

### Staging

| Setting | Value |
|---------|-------|
| Job Name | `[STAGING] Olist Lakehouse - Daily Orchestration` |
| Schedule | Daily at 07:00 UTC |
| Catalog | `olist_staging` |
| Notifications | On failure |

**Notes:**
- Runs 1 hour after production (07:00 vs 06:00)
- Allows production issues to surface first
- Email notifications enabled for monitoring

---

### Production

| Setting | Value |
|---------|-------|
| Job Name | `Olist Lakehouse - Daily Orchestration` |
| Schedule | Daily at 06:00 UTC |
| Catalog | `olist` |
| Timeout | 4 hours (extended) |
| Notifications | On failure |

**Notes:**
- Extended timeout for larger data volumes
- Email notifications to data engineering team
- Weekly refresh job also enabled

---

## Operations

### Manual Execution

**Using Databricks CLI:**

```bash
# Run daily job in development
databricks bundle run olist_daily_job -t dev

# Run daily job in staging
databricks bundle run olist_daily_job -t staging

# Run daily job in production
databricks bundle run olist_daily_job -t prod

# Run weekly full refresh
databricks bundle run olist_weekly_refresh_job -t dev
```

**Using GitHub Actions:**

```bash
# Trigger via workflow dispatch (if configured)
gh workflow run "Deploy to Development" -f run_pipeline=true
```

**Via Databricks UI:**

1. Navigate to **Workflows** > **Jobs**
2. Find the job (e.g., `[DEV] Olist Lakehouse - Daily Orchestration`)
3. Click **Run Now**

---

### Monitoring

**View Job Runs:**

```bash
# List recent job runs
databricks runs list --limit 10

# Get specific run details
databricks runs get --run-id <run_id>
```

**Key Metrics to Monitor:**

| Metric | Expected | Alert Threshold |
|--------|----------|-----------------|
| Total Duration | 1-2 hours | > 3 hours |
| Data Generation | 10-30 min | > 1 hour |
| Main Pipeline | 30-60 min | > 1.5 hours |
| CDC Pipeline | 15-30 min | > 1 hour |

**Databricks UI:**

1. Navigate to **Workflows** > **Job Runs**
2. Select the job run to view task-level details
3. Check the **Graph** view for task dependencies
4. Review **Logs** for each task

---

## Troubleshooting

### Common Issues

#### 1. Data Generation Fails

**Symptoms:**
- `generate_data` task fails
- Error: "Volume path not found"

**Causes:**
- Unity Catalog volumes not created
- Incorrect catalog name

**Solution:**
1. Verify Unity Catalog setup is complete
2. Run the setup notebook: `src/setup/unity_catalog_setup.py`
3. Check catalog variable: `databricks bundle validate -t dev`

---

#### 2. Pipeline Timeout

**Symptoms:**
- `run_main_pipeline` or `run_cdc_pipeline` exceeds timeout

**Causes:**
- Large data volume
- Cluster scaling issues
- Data quality issues causing many dropped rows

**Solution:**
1. Increase `timeout_seconds` in job configuration
2. Check pipeline event logs for bottlenecks
3. Review data quality metrics in pipeline UI

---

#### 3. Job Not Running on Schedule

**Symptoms:**
- Job doesn't execute at scheduled time

**Causes:**
- Schedule is paused
- Timezone mismatch
- Previous run still in progress

**Solution:**
1. Check `pause_status` in job configuration
2. Verify timezone is set to UTC
3. Check for stuck runs: `databricks runs list --active-only`

---

#### 4. CDC Pipeline SCD Issues

**Symptoms:**
- Missing history records
- Incorrect `__START_AT` / `__END_AT` values

**Causes:**
- Out-of-order CDC events
- Missing sequence numbers

**Solution:**
1. Verify `sequence_number` is monotonically increasing
2. Check for NULL values in CDC metadata columns
3. Review CDC staging table for validation failures

---

### Getting Help

1. **Job Logs:** Check the job run logs in Databricks UI
2. **Pipeline Event Log:** Query the pipeline's event log for detailed metrics
3. **Documentation:**
   - [DEPLOYMENT.md](DEPLOYMENT.md) - Deployment instructions
   - [CDC_GUIDE.md](CDC_GUIDE.md) - CDC implementation details
   - [CICD.md](CICD.md) - CI/CD pipeline documentation

---

## Quick Reference

### Job Commands

```bash
# Validate bundle (includes jobs)
databricks bundle validate -t dev

# Deploy jobs
databricks bundle deploy -t dev

# Run daily job
databricks bundle run olist_daily_job -t dev

# Run weekly full refresh
databricks bundle run olist_weekly_refresh_job -t dev

# List job runs
databricks runs list --limit 5
```

### File Locations

| Purpose | File |
|---------|------|
| Job Definitions | `resources/olist_jobs.yml` |
| Target Overrides | `databricks.yml` |
| Data Generator | `src/utils/data_generator.py` |
| Main Pipeline | `resources/olist_pipeline.yml` |
| This Guide | `docs/ORCHESTRATION_GUIDE.md` |

---

## Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2024-01-27 | 1.0.0 | Initial orchestration implementation |
