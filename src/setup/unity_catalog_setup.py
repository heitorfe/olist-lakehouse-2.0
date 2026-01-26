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
# MAGIC ## Download Olist Dataset from Kaggle
# MAGIC
# MAGIC This section downloads the Brazilian E-commerce dataset from Kaggle and uploads it to the Volume.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC 1. Create a Kaggle account at https://www.kaggle.com
# MAGIC 2. Generate API credentials at https://www.kaggle.com/settings (Create New Token)
# MAGIC 3. Store credentials in Databricks Secrets:
# MAGIC    ```bash
# MAGIC    databricks secrets create-scope kaggle
# MAGIC    databricks secrets put-secret kaggle username --string-value "your_username"
# MAGIC    databricks secrets put-secret kaggle key --string-value "your_api_key"
# MAGIC    ```

# COMMAND ----------

# Install Kaggle package
%pip install kaggle --quiet

# COMMAND ----------

import os
import zipfile
import shutil
import tempfile

# Configuration for Kaggle download
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
DOWNLOAD_ENABLED = True  # Set to False to skip download

# Mapping of Kaggle CSV files to Volume directories
FILE_MAPPING = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "reference"  # Extra reference file
}

# COMMAND ----------

def setup_kaggle_credentials():
    """
    Configure Kaggle credentials from Databricks Secrets.

    The credentials are stored in the 'kaggle' secret scope with keys:
    - 'username': Your Kaggle username
    - 'key': Your Kaggle API key
    """
    try:
        # Retrieve credentials from Databricks Secrets
        kaggle_username = dbutils.secrets.get(scope="kaggle", key="username")
        kaggle_key = dbutils.secrets.get(scope="kaggle", key="key")

        # Set environment variables for Kaggle API
        os.environ["KAGGLE_USERNAME"] = kaggle_username
        os.environ["KAGGLE_KEY"] = kaggle_key

        print("✓ Kaggle credentials configured from Databricks Secrets")
        return True
    except Exception as e:
        print(f"⚠ Could not retrieve Kaggle credentials: {str(e)}")
        print("\nTo configure Kaggle credentials, run these commands in your terminal:")
        print("  databricks secrets create-scope kaggle")
        print('  databricks secrets put-secret kaggle username --string-value "your_username"')
        print('  databricks secrets put-secret kaggle key --string-value "your_api_key"')
        return False

# COMMAND ----------

def download_kaggle_dataset(dataset_name, download_path):
    """
    Download and extract a Kaggle dataset.

    Args:
        dataset_name: Kaggle dataset identifier (e.g., 'olistbr/brazilian-ecommerce')
        download_path: Local path to download and extract files

    Returns:
        List of extracted file paths
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    # Initialize Kaggle API
    api = KaggleApi()
    api.authenticate()

    print(f"Downloading dataset: {dataset_name}")
    print(f"Download path: {download_path}")

    # Download the dataset (comes as a zip file)
    api.dataset_download_files(
        dataset=dataset_name,
        path=download_path,
        unzip=True
    )

    # List extracted files
    extracted_files = []
    for root, dirs, files in os.walk(download_path):
        for file in files:
            if file.endswith('.csv'):
                extracted_files.append(os.path.join(root, file))
                print(f"  ✓ Extracted: {file}")

    return extracted_files

# COMMAND ----------

def upload_to_volume(local_path, volume_path, filename):
    """
    Upload a local file to a Databricks Volume using direct file I/O.

    Unity Catalog Volumes are FUSE-mounted, so we can use standard Python
    file operations to read/write directly to the Volume path.

    Args:
        local_path: Path to the local file
        volume_path: Volume directory path (e.g., /Volumes/catalog/schema/volume/dir)
        filename: Target filename in the Volume
    """
    # Ensure target directory exists
    os.makedirs(volume_path, exist_ok=True)

    target_path = f"{volume_path}/{filename}"

    # Read file content into memory and write directly to Volume
    with open(local_path, 'rb') as src_file:
        file_content = src_file.read()

    with open(target_path, 'wb') as dst_file:
        dst_file.write(file_content)

    file_size_kb = len(file_content) / 1024
    print(f"  ✓ Uploaded to: {target_path} ({file_size_kb:.1f} KB)")

# COMMAND ----------

def download_and_upload_olist_dataset():
    """
    Main function to download Olist dataset from Kaggle and upload to Volumes.
    """
    print("=" * 60)
    print("DOWNLOADING OLIST DATASET FROM KAGGLE")
    print("=" * 60)

    # Step 1: Setup credentials
    if not setup_kaggle_credentials():
        print("\n⚠ Skipping download - credentials not configured")
        return False

    # Step 2: Create temp directory for download
    temp_dir = tempfile.mkdtemp(prefix="olist_kaggle_")
    print(f"\nTemp directory: {temp_dir}")

    try:
        # Step 3: Download dataset
        print("\n--- Downloading Dataset ---")
        extracted_files = download_kaggle_dataset(KAGGLE_DATASET, temp_dir)

        if not extracted_files:
            print("⚠ No files were extracted from the dataset")
            return False

        # Step 4: Create reference directory if needed (using Python's os module)
        reference_path = f"{base_path}/reference"
        os.makedirs(reference_path, exist_ok=True)

        # Step 5: Upload files to Volume
        print("\n--- Uploading to Volumes ---")
        uploaded_count = 0

        for file_path in extracted_files:
            filename = os.path.basename(file_path)

            if filename in FILE_MAPPING:
                target_dir = FILE_MAPPING[filename]
                volume_dir = f"{base_path}/{target_dir}"

                # Rename to simpler names for consistency
                target_filename = filename.replace("olist_", "").replace("_dataset", "")

                upload_to_volume(file_path, volume_dir, target_filename)
                uploaded_count += 1
            else:
                print(f"  ⚠ Skipped (unmapped): {filename}")

        print(f"\n✓ Successfully uploaded {uploaded_count} files to Volumes")
        return True

    except Exception as e:
        print(f"\n✗ Error during download/upload: {str(e)}")
        raise

    finally:
        # Cleanup temp directory
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
            print(f"\n✓ Cleaned up temp directory")

# COMMAND ----------

# Execute download if enabled
if DOWNLOAD_ENABLED:
    try:
        download_and_upload_olist_dataset()
    except Exception as e:
        print(f"\n⚠ Download failed: {str(e)}")
        print("You can manually download the dataset from:")
        print("  https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        print(f"  and upload to: {base_path}/")
else:
    print("Dataset download is disabled. Set DOWNLOAD_ENABLED = True to enable.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Downloaded Data

# COMMAND ----------

# List all files in the Volume after download
print("Files in Volume after Kaggle download:")
print("=" * 60)

for entity in append_only_entities + ['reference']:
    try:
        files = dbutils.fs.ls(f"{base_path}/{entity}/")
        print(f"\n{entity}/:")
        for f in files:
            size_kb = f.size / 1024
            print(f"  - {f.name} ({size_kb:.1f} KB)")
    except Exception as e:
        print(f"\n{entity}/: (empty or not created)")

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
# MAGIC ### Kaggle Dataset
# MAGIC If credentials were configured, the following files were downloaded and uploaded:
# MAGIC
# MAGIC | Kaggle File | Volume Directory |
# MAGIC |-------------|-----------------|
# MAGIC | `olist_customers_dataset.csv` | `/customers/` |
# MAGIC | `olist_orders_dataset.csv` | `/orders/` |
# MAGIC | `olist_order_items_dataset.csv` | `/order_items/` |
# MAGIC | `olist_order_payments_dataset.csv` | `/order_payments/` |
# MAGIC | `olist_order_reviews_dataset.csv` | `/order_reviews/` |
# MAGIC | `olist_products_dataset.csv` | `/products/` |
# MAGIC | `olist_sellers_dataset.csv` | `/sellers/` |
# MAGIC | `olist_geolocation_dataset.csv` | `/geolocation/` |
# MAGIC | `product_category_name_translation.csv` | `/reference/` |
# MAGIC
# MAGIC ### Next Steps
# MAGIC 1. If Kaggle download failed, manually upload CSV files to `/Volumes/{CATALOG_NAME}/raw/olist/`
# MAGIC 2. Run the data generator for CDC testing
# MAGIC 3. Deploy and run the Lakeflow pipelines
