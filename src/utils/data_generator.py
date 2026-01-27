# Databricks notebook source
# MAGIC %md
# MAGIC # Olist CDC Data Generator
# MAGIC
# MAGIC This notebook generates synthetic CDC (Change Data Capture) data for testing the Lakeflow pipelines.
# MAGIC
# MAGIC ## What it does:
# MAGIC 1. **Initial Load**: Generates base records for customers, products, and sellers
# MAGIC 2. **CDC Batches**: Generates change events (INSERT, UPDATE, DELETE) with proper sequencing
# MAGIC 3. **Realistic Data**: Uses Brazilian-style data (names, states, cities)
# MAGIC
# MAGIC ## Output Location:
# MAGIC - Initial data: `/Volumes/{catalog}/raw/olist/{entity}/`
# MAGIC - CDC data: `/Volumes/{catalog}/raw/olist/cdc/{entity}/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import uuid
import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Configuration
CATALOG = dbutils.widgets.get("catalog")
VOLUME_PATH = f"/Volumes/{CATALOG}/raw/olist"

# Data generation settings - ranges for variability
# Each run will generate a random count within these ranges
CUSTOMERS_RANGE = (800, 1200)
PRODUCTS_RANGE = (400, 600)
SELLERS_RANGE = (80, 120)
ORDERS_RANGE = (4000, 6000)

# CDC settings
CDC_BATCHES = 3
CDC_CHANGES_RANGE = (40, 60)

# Data quality settings - introduce noise for DQ testing
# Approximately 2% of records will have data quality issues
BAD_DATA_RATE = 0.02


def get_random_count(range_tuple):
    """Get a random count within the specified range."""
    return random.randint(range_tuple[0], range_tuple[1])

print(f"Catalog: {CATALOG}")
print(f"Volume Path: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

# Brazilian data for realistic generation
BRAZILIAN_STATES = {
    'SP': 'Sao Paulo', 'RJ': 'Rio De Janeiro', 'MG': 'Belo Horizonte',
    'RS': 'Porto Alegre', 'PR': 'Curitiba', 'SC': 'Florianopolis',
    'BA': 'Salvador', 'PE': 'Recife', 'CE': 'Fortaleza',
    'DF': 'Brasilia', 'GO': 'Goiania', 'PA': 'Belem'
}

PRODUCT_CATEGORIES = [
    'electronics', 'computers', 'home appliances', 'furniture', 'sports',
    'toys', 'health beauty', 'fashion bags', 'watches gifts', 'garden tools',
    'auto', 'books', 'music', 'food drink', 'pet shop'
]

ORDER_STATUSES = ['created', 'approved', 'invoiced', 'processing', 'shipped', 'delivered', 'canceled']
PAYMENT_TYPES = ['credit_card', 'boleto', 'voucher', 'debit_card']


def generate_uuid():
    """Generate a 32-character UUID without hyphens."""
    return uuid.uuid4().hex


def random_brazilian_location():
    """Generate random Brazilian state and city."""
    state = random.choice(list(BRAZILIAN_STATES.keys()))
    city = BRAZILIAN_STATES[state]
    zip_prefix = random.randint(10000, 99999)
    return state, city, zip_prefix


def random_timestamp(start_date, end_date):
    """Generate random timestamp between two dates."""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)
    return start_date + timedelta(days=random_days, seconds=random_seconds)


def introduce_bad_data(record, entity_type, bad_data_rate=BAD_DATA_RATE):
    """
    Introduces data quality issues for testing DQ expectations.

    This function randomly corrupts records to test that the Data Quality
    constraints in the silver layer properly catch and drop invalid rows.

    Args:
        record: The data record dictionary
        entity_type: Type of entity (customer, product, seller, order, etc.)
        bad_data_rate: Probability of introducing bad data (default 2%)

    Returns:
        The record, potentially with data quality issues
    """
    if random.random() >= bad_data_rate:
        return record  # Keep clean

    # Violation types mapped to DQ rules in silver layer
    if entity_type == 'customer':
        violation = random.choice(['null_id', 'short_id', 'null_zip'])
        if violation == 'null_id':
            record['customer_id'] = None
        elif violation == 'short_id':
            record['customer_id'] = 'INVALID_SHORT'  # Not 32 chars
        elif violation == 'null_zip':
            record['customer_zip_code_prefix'] = None

    elif entity_type == 'product':
        violation = random.choice(['null_id', 'negative_weight', 'negative_dimension'])
        if violation == 'null_id':
            record['product_id'] = None
        elif violation == 'negative_weight':
            record['product_weight_g'] = -random.randint(1, 1000)
        elif violation == 'negative_dimension':
            dim = random.choice(['product_length_cm', 'product_height_cm', 'product_width_cm'])
            record[dim] = -random.randint(1, 50)

    elif entity_type == 'seller':
        violation = random.choice(['null_id', 'short_id'])
        if violation == 'null_id':
            record['seller_id'] = None
        elif violation == 'short_id':
            record['seller_id'] = 'BAD_SELLER'  # Not 32 chars

    elif entity_type == 'order':
        violation = random.choice(['null_id', 'invalid_status', 'null_timestamp'])
        if violation == 'null_id':
            record['order_id'] = None
        elif violation == 'invalid_status':
            record['order_status'] = 'INVALID_STATUS_XYZ'
        elif violation == 'null_timestamp':
            record['order_purchase_timestamp'] = None

    elif entity_type == 'order_item':
        violation = random.choice(['negative_price', 'negative_freight'])
        if violation == 'negative_price':
            record['price'] = -round(random.uniform(1, 100), 2)
        elif violation == 'negative_freight':
            record['freight_value'] = -round(random.uniform(1, 50), 2)

    elif entity_type == 'order_payment':
        violation = random.choice(['invalid_type', 'negative_value'])
        if violation == 'invalid_type':
            record['payment_type'] = 'INVALID_PAYMENT_TYPE'
        elif violation == 'negative_value':
            record['payment_value'] = -round(random.uniform(1, 100), 2)

    elif entity_type == 'order_review':
        violation = random.choice(['invalid_score_low', 'invalid_score_high'])
        if violation == 'invalid_score_low':
            record['review_score'] = 0  # Should be 1-5
        elif violation == 'invalid_score_high':
            record['review_score'] = random.randint(6, 10)  # Should be 1-5

    return record

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Initial Load Data

# COMMAND ----------

def generate_customers(count, apply_bad_data=True):
    """Generate initial customer data with optional data quality issues."""
    customers = []
    for _ in range(count):
        state, city, zip_prefix = random_brazilian_location()
        customer = {
            'customer_id': generate_uuid(),
            'customer_unique_id': generate_uuid(),
            'customer_zip_code_prefix': zip_prefix,
            'customer_city': city,
            'customer_state': state
        }
        if apply_bad_data:
            customer = introduce_bad_data(customer, 'customer')
        customers.append(customer)
    return customers


def generate_products(count, apply_bad_data=True):
    """Generate initial product data with optional data quality issues."""
    products = []
    for _ in range(count):
        product = {
            'product_id': generate_uuid(),
            'product_category_name': random.choice(PRODUCT_CATEGORIES),
            'product_name_lenght': random.randint(10, 100),
            'product_description_lenght': random.randint(50, 500),
            'product_photos_qty': random.randint(1, 10),
            'product_weight_g': random.randint(100, 50000),
            'product_length_cm': random.randint(5, 100),
            'product_height_cm': random.randint(5, 100),
            'product_width_cm': random.randint(5, 100)
        }
        if apply_bad_data:
            product = introduce_bad_data(product, 'product')
        products.append(product)
    return products


def generate_sellers(count, apply_bad_data=True):
    """Generate initial seller data with optional data quality issues."""
    sellers = []
    for _ in range(count):
        state, city, zip_prefix = random_brazilian_location()
        seller = {
            'seller_id': generate_uuid(),
            'seller_zip_code_prefix': zip_prefix,
            'seller_city': city,
            'seller_state': state
        }
        if apply_bad_data:
            seller = introduce_bad_data(seller, 'seller')
        sellers.append(seller)
    return sellers


def generate_geolocation():
    """Generate geolocation reference data."""
    geolocations = []
    for state, city in BRAZILIAN_STATES.items():
        for _ in range(10):  # 10 entries per state
            zip_prefix = random.randint(10000, 99999)
            geolocations.append({
                'geolocation_zip_code_prefix': zip_prefix,
                'geolocation_lat': random.uniform(-30, -5),
                'geolocation_lng': random.uniform(-55, -35),
                'geolocation_city': city,
                'geolocation_state': state
            })
    return geolocations

# COMMAND ----------

def generate_orders(count, customer_ids, seller_ids, product_ids, apply_bad_data=True):
    """Generate order data with related items, payments, and reviews."""
    orders = []
    order_items = []
    order_payments = []
    order_reviews = []

    start_date = datetime(2017, 1, 1)
    end_date = datetime(2018, 12, 31)

    for _ in range(count):
        order_id = generate_uuid()
        customer_id = random.choice(customer_ids)
        purchase_time = random_timestamp(start_date, end_date)
        status = random.choice(ORDER_STATUSES)

        # Order
        order = {
            'order_id': order_id,
            'customer_id': customer_id,
            'order_status': status,
            'order_purchase_timestamp': purchase_time.isoformat(),
            'order_approved_at': (purchase_time + timedelta(hours=random.randint(1, 24))).isoformat() if status != 'created' else None,
            'order_delivered_carrier_date': (purchase_time + timedelta(days=random.randint(1, 5))).isoformat() if status in ['shipped', 'delivered'] else None,
            'order_delivered_customer_date': (purchase_time + timedelta(days=random.randint(5, 30))).isoformat() if status == 'delivered' else None,
            'order_estimated_delivery_date': (purchase_time + timedelta(days=random.randint(7, 45))).isoformat()
        }
        if apply_bad_data:
            order = introduce_bad_data(order, 'order')
        orders.append(order)

        # Order Items (1-5 items per order)
        num_items = random.randint(1, 5)
        for item_id in range(1, num_items + 1):
            item = {
                'order_id': order_id,
                'order_item_id': item_id,
                'product_id': random.choice(product_ids),
                'seller_id': random.choice(seller_ids),
                'shipping_limit_date': (purchase_time + timedelta(days=random.randint(1, 7))).isoformat(),
                'price': round(random.uniform(10, 1000), 2),
                'freight_value': round(random.uniform(5, 100), 2)
            }
            if apply_bad_data:
                item = introduce_bad_data(item, 'order_item')
            order_items.append(item)

        # Order Payments (1-3 payments per order)
        num_payments = random.randint(1, 3)
        total_value = sum(item['price'] + item['freight_value'] for item in order_items if item['order_id'] == order_id)
        payment_per = total_value / num_payments
        for seq in range(1, num_payments + 1):
            payment = {
                'order_id': order_id,
                'payment_sequential': seq,
                'payment_type': random.choice(PAYMENT_TYPES),
                'payment_installments': random.randint(1, 12),
                'payment_value': round(payment_per, 2)
            }
            if apply_bad_data:
                payment = introduce_bad_data(payment, 'order_payment')
            order_payments.append(payment)

        # Order Review (if delivered)
        if status == 'delivered' and random.random() > 0.3:
            review_date = purchase_time + timedelta(days=random.randint(10, 60))
            review = {
                'review_id': generate_uuid(),
                'order_id': order_id,
                'review_score': random.randint(1, 5),
                'review_comment_title': 'Review Title' if random.random() > 0.5 else None,
                'review_comment_message': 'This is a review comment.' if random.random() > 0.5 else None,
                'review_creation_date': review_date.isoformat(),
                'review_answer_timestamp': (review_date + timedelta(days=random.randint(1, 7))).isoformat()
            }
            if apply_bad_data:
                review = introduce_bad_data(review, 'order_review')
            order_reviews.append(review)

    return orders, order_items, order_payments, order_reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate CDC Events

# COMMAND ----------

def generate_cdc_batch(existing_records, entity_type, batch_num, changes_count):
    """
    Generate CDC change events for a batch.

    Returns list of change events with:
    - sequence_number: Monotonically increasing sequence
    - operation: INSERT, UPDATE, or DELETE
    - change_timestamp: When the change occurred
    - All entity fields
    """
    changes = []
    base_sequence = batch_num * 10000
    base_time = datetime.now() + timedelta(hours=batch_num)

    for i in range(changes_count):
        sequence_number = base_sequence + i
        change_time = base_time + timedelta(seconds=i)

        # Decide operation: 60% UPDATE, 30% INSERT, 10% DELETE
        operation_roll = random.random()
        if operation_roll < 0.6 and existing_records:
            # UPDATE existing record
            operation = 'UPDATE'
            record = random.choice(existing_records).copy()
        elif operation_roll < 0.9:
            # INSERT new record
            operation = 'INSERT'
            if entity_type == 'customers':
                state, city, zip_prefix = random_brazilian_location()
                record = {
                    'customer_id': generate_uuid(),
                    'customer_unique_id': generate_uuid(),
                    'customer_zip_code_prefix': zip_prefix,
                    'customer_city': city,
                    'customer_state': state
                }
            elif entity_type == 'products':
                record = {
                    'product_id': generate_uuid(),
                    'product_category_name': random.choice(PRODUCT_CATEGORIES),
                    'product_name_lenght': random.randint(10, 100),
                    'product_description_lenght': random.randint(50, 500),
                    'product_photos_qty': random.randint(1, 10),
                    'product_weight_g': random.randint(100, 50000),
                    'product_length_cm': random.randint(5, 100),
                    'product_height_cm': random.randint(5, 100),
                    'product_width_cm': random.randint(5, 100)
                }
            elif entity_type == 'sellers':
                state, city, zip_prefix = random_brazilian_location()
                record = {
                    'seller_id': generate_uuid(),
                    'seller_zip_code_prefix': zip_prefix,
                    'seller_city': city,
                    'seller_state': state
                }
            existing_records.append(record)
        else:
            # DELETE existing record
            if existing_records:
                operation = 'DELETE'
                record = random.choice(existing_records).copy()
            else:
                continue

        # Apply changes for UPDATE operations
        if operation == 'UPDATE':
            if entity_type == 'customers':
                state, city, zip_prefix = random_brazilian_location()
                record['customer_city'] = city
                record['customer_state'] = state
                record['customer_zip_code_prefix'] = zip_prefix
            elif entity_type == 'products':
                record['product_category_name'] = random.choice(PRODUCT_CATEGORIES)
                record['product_weight_g'] = random.randint(100, 50000)
            elif entity_type == 'sellers':
                state, city, zip_prefix = random_brazilian_location()
                record['seller_city'] = city
                record['seller_state'] = state

        # Add CDC metadata
        change_record = {
            'sequence_number': sequence_number,
            'operation': operation,
            'change_timestamp': change_time.isoformat(),
            **record
        }
        changes.append(change_record)

    return changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Data to Volumes

# COMMAND ----------

def save_to_csv(data, path, filename):
    """Save data to CSV in the specified volume path."""
    if not data:
        print(f"No data to save for {filename}")
        return

    df = spark.createDataFrame(data)
    full_path = f"{path}/{filename}"

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(full_path)
    print(f"Saved {len(data)} records to {full_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Data Generation

# COMMAND ----------

# Generate initial load data
print("=" * 60)
print("GENERATING INITIAL LOAD DATA")
print("=" * 60)

# Generate random counts within configured ranges
num_customers = get_random_count(CUSTOMERS_RANGE)
num_products = get_random_count(PRODUCTS_RANGE)
num_sellers = get_random_count(SELLERS_RANGE)
num_orders = get_random_count(ORDERS_RANGE)

print(f"\nTarget counts (with variability):")
print(f"  - Customers: {num_customers} (range: {CUSTOMERS_RANGE})")
print(f"  - Products: {num_products} (range: {PRODUCTS_RANGE})")
print(f"  - Sellers: {num_sellers} (range: {SELLERS_RANGE})")
print(f"  - Orders: {num_orders} (range: {ORDERS_RANGE})")
print(f"  - Bad data rate: {BAD_DATA_RATE:.1%}")

# Generate base entities
customers = generate_customers(num_customers)
products = generate_products(num_products)
sellers = generate_sellers(num_sellers)
geolocations = generate_geolocation()

# Get IDs for order generation (filter out None IDs from bad data)
customer_ids = [c['customer_id'] for c in customers if c['customer_id'] is not None]
product_ids = [p['product_id'] for p in products if p['product_id'] is not None]
seller_ids = [s['seller_id'] for s in sellers if s['seller_id'] is not None]

# Generate transactional data
orders, order_items, order_payments, order_reviews = generate_orders(
    num_orders, customer_ids, seller_ids, product_ids
)

print(f"\nGenerated:")
print(f"  - {len(customers)} customers")
print(f"  - {len(products)} products")
print(f"  - {len(sellers)} sellers")
print(f"  - {len(geolocations)} geolocation records")
print(f"  - {len(orders)} orders")
print(f"  - {len(order_items)} order items")
print(f"  - {len(order_payments)} order payments")
print(f"  - {len(order_reviews)} order reviews")

# COMMAND ----------

# Save initial load data
print("\n" + "=" * 60)
print("SAVING INITIAL LOAD DATA")
print("=" * 60)

save_to_csv(customers, f"{VOLUME_PATH}/customers", "customers_initial.csv")
save_to_csv(products, f"{VOLUME_PATH}/products", "products_initial.csv")
save_to_csv(sellers, f"{VOLUME_PATH}/sellers", "sellers_initial.csv")
save_to_csv(geolocations, f"{VOLUME_PATH}/geolocation", "geolocation_initial.csv")
save_to_csv(orders, f"{VOLUME_PATH}/orders", "orders_initial.csv")
save_to_csv(order_items, f"{VOLUME_PATH}/order_items", "order_items_initial.csv")
save_to_csv(order_payments, f"{VOLUME_PATH}/order_payments", "order_payments_initial.csv")
save_to_csv(order_reviews, f"{VOLUME_PATH}/order_reviews", "order_reviews_initial.csv")

# COMMAND ----------

# Generate and save CDC batches
print("\n" + "=" * 60)
print("GENERATING CDC BATCHES")
print("=" * 60)

for batch in range(CDC_BATCHES):
    # Generate random change count for this batch
    changes_count = get_random_count(CDC_CHANGES_RANGE)
    print(f"\n--- Batch {batch + 1} ({changes_count} changes per entity) ---")

    # Generate CDC events
    customer_changes = generate_cdc_batch(customers, 'customers', batch, changes_count)
    product_changes = generate_cdc_batch(products, 'products', batch, changes_count)
    seller_changes = generate_cdc_batch(sellers, 'sellers', batch, changes_count)

    # Save CDC batches
    save_to_csv(customer_changes, f"{VOLUME_PATH}/cdc/customers", f"customers_cdc_batch_{batch + 1}.csv")
    save_to_csv(product_changes, f"{VOLUME_PATH}/cdc/products", f"products_cdc_batch_{batch + 1}.csv")
    save_to_csv(seller_changes, f"{VOLUME_PATH}/cdc/sellers", f"sellers_cdc_batch_{batch + 1}.csv")

print("\n" + "=" * 60)
print("DATA GENERATION COMPLETE!")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Generated Data

# COMMAND ----------

# List generated files
print("Generated files in volume:")
for entity in ['customers', 'products', 'sellers', 'orders', 'order_items', 'order_payments', 'order_reviews', 'geolocation']:
    try:
        files = dbutils.fs.ls(f"{VOLUME_PATH}/{entity}/")
        print(f"\n{entity}/:")
        for f in files:
            print(f"  - {f.name}")
    except Exception as e:
        print(f"\n{entity}/: No files yet")

# CDC directories
print("\n\nCDC directories:")
for entity in ['customers', 'products', 'sellers']:
    try:
        files = dbutils.fs.ls(f"{VOLUME_PATH}/cdc/{entity}/")
        print(f"\ncdc/{entity}/:")
        for f in files:
            print(f"  - {f.name}")
    except Exception as e:
        print(f"\ncdc/{entity}/: No files yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC The data generator has created:
# MAGIC
# MAGIC ### Initial Load (with Variability)
# MAGIC | Entity | Range | Description |
# MAGIC |--------|-------|-------------|
# MAGIC | customers | 800-1,200 | Customer profiles |
# MAGIC | products | 400-600 | Product catalog |
# MAGIC | sellers | 80-120 | Seller profiles |
# MAGIC | orders | 4,000-6,000 | Order transactions |
# MAGIC | order_items | ~12,000-18,000 | Order line items |
# MAGIC | order_payments | ~6,000-9,000 | Payment records |
# MAGIC | order_reviews | ~1,600-2,400 | Customer reviews |
# MAGIC | geolocation | 120 | Geographic reference |
# MAGIC
# MAGIC ### CDC Batches (with Variability)
# MAGIC | Batch | Changes per Entity | Operations |
# MAGIC |-------|-------------------|------------|
# MAGIC | 1-3 | 40-60 (random) | INSERT, UPDATE, DELETE |
# MAGIC
# MAGIC ### Data Quality Testing
# MAGIC - **Bad Data Rate:** ~2% of records contain data quality issues
# MAGIC - **Purpose:** Test DQ expectations in silver layer
# MAGIC - **Violation Types:** NULL IDs, invalid lengths, negative values, invalid enums
# MAGIC
# MAGIC ### Next Steps
# MAGIC 1. Run the Lakeflow pipeline to process initial load
# MAGIC 2. Verify Data Quality expectations catch bad records
# MAGIC 3. Trigger pipeline updates to process CDC batches
# MAGIC 4. Query SCD Type 1 and Type 2 tables to verify CDC processing
