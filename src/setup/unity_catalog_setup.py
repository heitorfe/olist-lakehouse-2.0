# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Setup for Olist Lakehouse
# MAGIC
# MAGIC This notebook creates all the Unity Catalog objects required for the Olist Lakehouse project:
# MAGIC - **Catalog**: `olist` (or dev/prod based on environment)
# MAGIC - **Schemas**: `bronze`, `silver`, `gold`, `raw`
# MAGIC - **Volumes**: External volume for raw data ingestion
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - Metastore Admin or Catalog CREATE privilege
# MAGIC - Workspace configured with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Environment configuration - change these based on your setup
ENVIRONMENT = "dev"  # Options: dev, staging, prod
CATALOG_NAME = f"olist_{ENVIRONMENT}" if ENVIRONMENT != "prod" else "olist"
STORAGE_LOCATION = f"/Volumes/{CATALOG_NAME}/raw/olist"

# Schema names following medallion architecture
SCHEMAS = ["raw", "bronze", "silver", "gold"]

print(f"Environment: {ENVIRONMENT}")
print(f"Catalog: {CATALOG_NAME}")
print(f"Storage Location: {STORAGE_LOCATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog

# COMMAND ----------

spark.sql(f"""
    CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}
    COMMENT 'Olist E-commerce Lakehouse - {ENVIRONMENT.upper()} environment'
""")

spark.sql(f"USE CATALOG {CATALOG_NAME}")
print(f"Catalog '{CATALOG_NAME}' created and selected")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas

# COMMAND ----------

for schema in SCHEMAS:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {schema}
        COMMENT '{schema.capitalize()} layer for Olist data - {ENVIRONMENT.upper()}'
    """)
    print(f"Schema '{CATALOG_NAME}.{schema}' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create External Volume for Raw Data

# COMMAND ----------

# Create the raw schema volume for data ingestion
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.raw.olist
    COMMENT 'External volume for Olist raw CSV data files'
""")

print(f"Volume '{CATALOG_NAME}.raw.olist' created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create CDC Subdirectory Structure
# MAGIC
# MAGIC The volume will have the following structure:
# MAGIC ```
# MAGIC /Volumes/{catalog}/raw/olist/
# MAGIC ├── customers/          # Initial load + append-only
# MAGIC ├── orders/             # Initial load + append-only
# MAGIC ├── order_items/        # Initial load + append-only
# MAGIC ├── order_payments/     # Initial load + append-only
# MAGIC ├── order_reviews/      # Initial load + append-only
# MAGIC ├── products/           # Initial load + append-only
# MAGIC ├── sellers/            # Initial load + append-only
# MAGIC ├── geolocation/        # Reference data
# MAGIC └── cdc/                # CDC change feeds
# MAGIC     ├── customers/      # Customer updates
# MAGIC     ├── products/       # Product updates
# MAGIC     └── sellers/        # Seller updates
# MAGIC ```

# COMMAND ----------

# Create directory structure using dbutils
import os

base_path = f"/Volumes/{CATALOG_NAME}/raw/olist"

# Main data directories (append-only entities)
append_only_entities = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation"
]

# CDC entities (entities that receive updates)
cdc_entities = [
    "customers",
    "products",
    "sellers"
]

# Create main directories
for entity in append_only_entities:
    path = f"{base_path}/{entity}"
    dbutils.fs.mkdirs(path)
    print(f"Created: {path}")

# Create CDC directories
for entity in cdc_entities:
    path = f"{base_path}/cdc/{entity}"
    dbutils.fs.mkdirs(path)
    print(f"Created: {path}")

print("\nDirectory structure created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions
# MAGIC
# MAGIC Grant appropriate permissions to groups/users for the catalog and schemas.

# COMMAND ----------

# Example permission grants - customize based on your organization
# Uncomment and modify as needed

# Data Engineers - Full access
# spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {CATALOG_NAME} TO `data-engineers`")

# Data Scientists - Read access to silver and gold
# spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `data-scientists`")
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.silver TO `data-scientists`")
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.gold TO `data-scientists`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.silver TO `data-scientists`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.gold TO `data-scientists`")

# Analysts - Read access to gold only
# spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG_NAME} TO `analysts`")
# spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG_NAME}.gold TO `analysts`")
# spark.sql(f"GRANT SELECT ON SCHEMA {CATALOG_NAME}.gold TO `analysts`")

print("Permission grants template ready - uncomment and customize as needed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Setup

# COMMAND ----------

# Verify catalog
catalogs = spark.sql("SHOW CATALOGS").collect()
print("Available Catalogs:")
for cat in catalogs:
    print(f"  - {cat.catalog}")

# COMMAND ----------

# Verify schemas
spark.sql(f"USE CATALOG {CATALOG_NAME}")
schemas = spark.sql("SHOW SCHEMAS").collect()
print(f"\nSchemas in {CATALOG_NAME}:")
for schema in schemas:
    print(f"  - {schema.databaseName}")

# COMMAND ----------

# Verify volume
volumes = spark.sql(f"SHOW VOLUMES IN {CATALOG_NAME}.raw").collect()
print(f"\nVolumes in {CATALOG_NAME}.raw:")
for vol in volumes:
    print(f"  - {vol.volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC Unity Catalog setup complete! The following objects have been created:
# MAGIC
# MAGIC | Object Type | Name | Purpose |
# MAGIC |-------------|------|---------|
# MAGIC | Catalog | `{CATALOG_NAME}` | Main catalog for Olist data |
# MAGIC | Schema | `raw` | External data ingestion |
# MAGIC | Schema | `bronze` | Raw data landing zone |
# MAGIC | Schema | `silver` | Cleansed, validated data |
# MAGIC | Schema | `gold` | Business-level aggregations |
# MAGIC | Volume | `raw.olist` | CSV file storage |
# MAGIC
# MAGIC ### Next Steps
# MAGIC 1. Upload Olist CSV files to `/Volumes/{CATALOG_NAME}/raw/olist/`
# MAGIC 2. Run the data generator for CDC testing
# MAGIC 3. Deploy and run the Lakeflow pipelines
