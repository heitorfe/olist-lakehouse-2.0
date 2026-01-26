# CI/CD Pipeline Documentation

This document describes the Continuous Integration and Continuous Deployment (CI/CD) pipeline for the Olist Lakehouse project.

## Table of Contents

- [Overview](#overview)
- [Pipeline Architecture](#pipeline-architecture)
- [Workflows](#workflows)
  - [PR Validation](#pr-validation)
  - [Deploy to Development](#deploy-to-development)
  - [Deploy to Staging](#deploy-to-staging)
  - [Deploy to Production](#deploy-to-production)
- [Configuration](#configuration)
  - [GitHub Secrets](#github-secrets)
  - [GitHub Environments](#github-environments)
  - [Branch Protection Rules](#branch-protection-rules)
- [Local Development](#local-development)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

---

## Overview

The CI/CD pipeline automates code quality checks, testing, and deployment of Databricks Asset Bundles across three environments:

| Environment | Trigger | Approval Required | Catalog |
|-------------|---------|-------------------|---------|
| **Development** | Push to `main` | No | `olist_dev` |
| **Staging** | Tag `staging/*` | Optional | `olist_staging` |
| **Production** | Tag `v*.*.*` | **Yes** | `olist` |

### Technology Stack

- **GitHub Actions** - CI/CD orchestration
- **Databricks Asset Bundles (DAB)** - Infrastructure as Code deployment
- **SQLFluff** - SQL linting with Databricks dialect
- **Ruff** - Python linting and formatting
- **pytest** - Python testing framework

---

## Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PULL REQUEST                                    │
│                                                                              │
│   ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │
│   │ SQL Lint │  │ Python   │  │ YAML     │  │ Bundle   │  │ Unit Tests   │ │
│   │ SQLFluff │  │ Ruff     │  │ yamllint │  │ Validate │  │ pytest       │ │
│   └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────────┘ │
│                         ↓ All must pass ↓                                    │
│                      ┌─────────────────────┐                                │
│                      │  Ready to Merge     │                                │
│                      └─────────────────────┘                                │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Merge to main
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DEVELOPMENT DEPLOYMENT                               │
│                                                                              │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                 │
│   │ Lint &       │───▶│ Deploy to    │───▶│ Notify       │                 │
│   │ Validate     │    │ DEV          │    │ (optional)   │                 │
│   └──────────────┘    └──────────────┘    └──────────────┘                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Tag: staging/v1.0.0
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          STAGING DEPLOYMENT                                  │
│                                                                              │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                 │
│   │ Validate     │───▶│ Deploy to    │───▶│ Smoke Tests  │                 │
│   │ Bundle       │    │ STAGING      │    │              │                 │
│   └──────────────┘    └──────────────┘    └──────────────┘                 │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Tag: v1.0.0
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PRODUCTION DEPLOYMENT                                │
│                                                                              │
│   ┌──────────┐   ┌──────────────┐   ┌──────────┐   ┌──────────────────┐   │
│   │ Validate │──▶│   APPROVAL   │──▶│ Deploy   │──▶│ Verify & Release │   │
│   │          │   │   GATE       │   │ to PROD  │   │                  │   │
│   └──────────┘   └──────────────┘   └──────────┘   └──────────────────┘   │
│                    ⚠️ Manual                                                │
│                    Approval                                                  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Workflows

### PR Validation

**File:** `.github/workflows/pr-validation.yml`

**Trigger:** Pull requests targeting `main` branch

**Jobs:**

| Job | Description | Tools |
|-----|-------------|-------|
| `sql-lint` | Validates SQL syntax and style | SQLFluff 3.0.7 |
| `python-lint` | Checks Python code quality | Ruff |
| `yaml-validation` | Validates YAML configuration | yamllint |
| `bundle-validation` | Validates DAB for all environments | Databricks CLI |
| `security-scan` | Checks for hardcoded credentials | grep patterns |
| `unit-tests` | Runs Python test suite | pytest |

**Example output:**
```
✓ SQL Lint (SQLFluff)      - 2m 15s
✓ Python Lint (Ruff)       - 45s
✓ YAML Validation          - 12s
✓ DAB Validate (dev)       - 1m 30s
✓ DAB Validate (staging)   - 1m 28s
✓ DAB Validate (prod)      - 1m 32s
✓ Security Scan            - 20s
✓ Unit Tests               - 1m 05s
```

---

### Deploy to Development

**File:** `.github/workflows/deploy-dev.yml`

**Triggers:**
- Automatic: Push to `main` branch
- Manual: Workflow dispatch with optional pipeline run

**Process:**
1. Lint SQL and Python code
2. Validate bundle configuration
3. Deploy to Databricks dev workspace
4. (Optional) Trigger pipeline run

**Manual trigger with pipeline run:**
```bash
# Via GitHub CLI
gh workflow run "Deploy to Development" -f run_pipeline=true

# Or via GitHub UI: Actions → Deploy to Development → Run workflow
```

---

### Deploy to Staging

**File:** `.github/workflows/deploy-staging.yml`

**Triggers:**
- Tag push: `staging/*` (e.g., `staging/v1.0.0`, `staging/release-candidate`)
- Manual: Workflow dispatch

**Process:**
1. Validate bundle configuration
2. Deploy to Databricks staging workspace
3. Run smoke tests to verify deployment

**Creating a staging release:**
```bash
# Create and push a staging tag
git tag staging/v1.0.0
git push origin staging/v1.0.0

# Or for release candidates
git tag staging/rc-2024-01-15
git push origin staging/rc-2024-01-15
```

---

### Deploy to Production

**File:** `.github/workflows/deploy-prod.yml`

**Triggers:**
- Tag push: Semantic version tags (`v1.0.0`, `v1.0.0-beta.1`)
- Manual: Workflow dispatch with version input

**Process:**
1. Full validation (lint + bundle validate)
2. **Manual approval required** (via GitHub Environment)
3. Deploy to Databricks production workspace
4. Post-deployment verification
5. Create GitHub Release

**Creating a production release:**
```bash
# Create a semantic version tag
git tag v1.0.0
git push origin v1.0.0

# For pre-releases (marked as prerelease in GitHub)
git tag v1.0.0-beta.1
git push origin v1.0.0-beta.1
```

**Semantic Versioning Guide:**
- `v1.0.0` - Major.Minor.Patch
- `vX.0.0` - Breaking changes
- `v1.X.0` - New features (backwards compatible)
- `v1.0.X` - Bug fixes

---

## Configuration

### GitHub Secrets

Navigate to: **Repository → Settings → Secrets and variables → Actions**

#### Required Secrets

| Secret Name | Description | Example |
|-------------|-------------|---------|
| `DATABRICKS_HOST` | Dev workspace URL | `https://adb-1234567890.1.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Dev Personal Access Token | `dapi1234567890abcdef...` |

#### Staging Secrets (if using separate workspace)

| Secret Name | Description |
|-------------|-------------|
| `DATABRICKS_HOST_STAGING` | Staging workspace URL |
| `DATABRICKS_TOKEN_STAGING` | Staging service principal token |

#### Production Secrets (if using separate workspace)

| Secret Name | Description |
|-------------|-------------|
| `DATABRICKS_HOST_PROD` | Production workspace URL |
| `DATABRICKS_TOKEN_PROD` | Production service principal token |

#### Optional Secrets

| Secret Name | Description |
|-------------|-------------|
| `SLACK_WEBHOOK_URL` | Slack webhook for notifications |

> **Note:** If using a single workspace for all environments, you can use the same `DATABRICKS_HOST` and `DATABRICKS_TOKEN` for all environments. The DAB configuration handles catalog separation.

---

### GitHub Environments

Navigate to: **Repository → Settings → Environments**

Create the following environments:

#### 1. Development Environment

- **Name:** `development`
- **Protection rules:** None required
- **Secrets:** Uses repository-level secrets

#### 2. Staging Environment

- **Name:** `staging`
- **Protection rules (optional):**
  - Required reviewers: 1
  - Deployment branches: `main`, tags matching `staging/*`
- **Secrets:** `DATABRICKS_HOST_STAGING`, `DATABRICKS_TOKEN_STAGING`

#### 3. Production Environment

- **Name:** `production`
- **Protection rules (required):**
  - ✅ Required reviewers: 1-2 approvers
  - ✅ Deployment branches: tags matching `v*`
  - (Optional) Wait timer: 5 minutes
- **Secrets:** `DATABRICKS_HOST_PROD`, `DATABRICKS_TOKEN_PROD`

---

### Branch Protection Rules

Navigate to: **Repository → Settings → Branches → Add rule**

**For `main` branch:**

```yaml
Branch name pattern: main

Protection rules:
  ✅ Require a pull request before merging
     ✅ Require approvals: 1
     ✅ Dismiss stale reviews when new commits are pushed

  ✅ Require status checks to pass before merging
     Required checks:
       - "SQL Lint (SQLFluff)"
       - "Python Lint (Ruff)"
       - "YAML Validation"
       - "DAB Validate (dev)"
       - "DAB Validate (staging)"
       - "DAB Validate (prod)"
       - "Unit Tests"
       - "Security Scan"

  ✅ Require conversation resolution before merging

  ☐ Require signed commits (optional, for enterprise)

  ✅ Do not allow bypassing the above settings
```

---

## Local Development

### Initial Setup

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Install pre-commit hooks (optional but recommended)
pre-commit install
```

### Running Checks Locally

```bash
# SQL Linting
sqlfluff lint src/pipelines/ --dialect databricks

# Fix SQL issues automatically
sqlfluff fix src/pipelines/ --dialect databricks

# Python Linting
ruff check src/ tests/

# Fix Python issues automatically
ruff check src/ tests/ --fix

# Python Formatting
ruff format src/ tests/

# YAML Validation
yamllint -c .yamllint.yml databricks.yml resources/

# Run Tests
pytest tests/ -v

# Run Tests with Coverage
pytest tests/ -v --cov=src --cov-report=html
```

### Validating Databricks Bundle

```bash
# Validate for development
databricks bundle validate -t dev

# Validate for all environments
databricks bundle validate -t dev
databricks bundle validate -t staging
databricks bundle validate -t prod
```

### Pre-commit Hooks

The `.pre-commit-config.yaml` runs these checks automatically before each commit:

- Trailing whitespace removal
- End of file fixer
- YAML syntax check
- Large file check (>1MB)
- Merge conflict check
- Private key detection
- Ruff linting and formatting
- SQLFluff linting
- YAML linting

To run manually on all files:
```bash
pre-commit run --all-files
```

---

## Testing

### Test Structure

```
tests/
├── __init__.py
├── conftest.py              # Shared fixtures
├── unit/
│   ├── __init__.py
│   └── test_data_generator.py   # Python unit tests
└── sql/
    ├── __init__.py
    └── test_sql_validation.py   # SQL convention tests
```

### SQL Validation Tests

The SQL tests verify your medallion architecture follows conventions:

| Test | What it checks |
|------|----------------|
| `test_bronze_uses_streaming_table` | Bronze layer uses `CREATE OR REFRESH STREAMING TABLE` |
| `test_bronze_uses_read_files` | Bronze layer uses `read_files()` for AutoLoader |
| `test_bronze_has_audit_columns` | Bronze includes `_ingested_at`, `_source_file` columns |
| `test_silver_has_data_quality_constraints` | Silver layer has `CONSTRAINT` definitions |
| `test_silver_uses_drop_row_violation` | Silver uses `ON VIOLATION DROP ROW` |
| `test_gold_uses_materialized_view` | Gold layer uses `MATERIALIZED VIEW` |
| `test_cdc_silver_uses_apply_changes` | CDC Silver uses `APPLY CHANGES` |
| `test_no_hardcoded_catalogs` | No hardcoded `olist_dev`/`olist_staging`/`olist` |
| `test_uses_catalog_variable` | Volume paths use `${catalog}` variable |

### Running Specific Tests

```bash
# Run only SQL tests
pytest tests/sql/ -v

# Run only unit tests
pytest tests/unit/ -v

# Run tests matching a pattern
pytest tests/ -v -k "bronze"

# Run with verbose output
pytest tests/ -v --tb=long
```

---

## Troubleshooting

### Common Issues

#### 1. Bundle Validation Fails

**Error:** `Error: cannot resolve variable`

**Solution:** Ensure the Databricks CLI is authenticated:
```bash
export DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...your-token...
databricks bundle validate -t dev
```

#### 2. SQLFluff Errors

**Error:** `L001: Unnecessary trailing whitespace`

**Solution:** Run SQLFluff fix:
```bash
sqlfluff fix src/pipelines/ --dialect databricks
```

#### 3. Workflow Not Triggering

**Issue:** Push to main doesn't trigger deployment

**Check:**
1. Verify the workflow file exists in `.github/workflows/`
2. Check if the changed files match the `paths` filter in the workflow
3. Review Actions tab for any workflow errors

#### 4. Environment Approval Stuck

**Issue:** Production deployment waiting indefinitely

**Solution:**
1. Go to **Actions** → Select the workflow run
2. Click on the "Production Approval" job
3. Click "Review pending deployments"
4. Approve or reject the deployment

#### 5. Secrets Not Available

**Error:** `Error: DATABRICKS_HOST is not set`

**Check:**
1. Verify secrets are set in repository settings
2. For environment-specific secrets, ensure they're set in the correct environment
3. Check the secret names match exactly (case-sensitive)

### Getting Help

1. Check workflow logs: **Actions** → Select run → Click on failed job
2. Review GitHub Actions documentation: https://docs.github.com/en/actions
3. Databricks CLI documentation: https://docs.databricks.com/dev-tools/cli/bundle-cli.html

---

## Quick Reference

### Deployment Commands

```bash
# Deploy to dev (automatic on push to main, or manual)
gh workflow run "Deploy to Development"

# Deploy to staging
git tag staging/v1.0.0 && git push origin staging/v1.0.0

# Deploy to production
git tag v1.0.0 && git push origin v1.0.0
# Then approve in GitHub Actions
```

### File Locations

| Purpose | File |
|---------|------|
| PR Validation | `.github/workflows/pr-validation.yml` |
| Dev Deployment | `.github/workflows/deploy-dev.yml` |
| Staging Deployment | `.github/workflows/deploy-staging.yml` |
| Prod Deployment | `.github/workflows/deploy-prod.yml` |
| SQL Lint Config | `.sqlfluff` |
| Python Config | `pyproject.toml` |
| YAML Lint Config | `.yamllint.yml` |
| Dev Dependencies | `requirements-dev.txt` |
| Pre-commit Config | `.pre-commit-config.yaml` |
| Code Owners | `.github/CODEOWNERS` |

---

## Changelog

| Date | Version | Changes |
|------|---------|---------|
| 2024-01-26 | 1.0.0 | Initial CI/CD pipeline implementation |
