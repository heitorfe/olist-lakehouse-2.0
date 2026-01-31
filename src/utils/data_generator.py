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
# MAGIC 4. **PII Fields**: Generates customer PII (name, email, phone) for Unity Catalog masking demos
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

# Data generation settings
INITIAL_CUSTOMERS = 1000
INITIAL_PRODUCTS = 500
INITIAL_SELLERS = 100
INITIAL_ORDERS = 5000

# CDC settings
CDC_BATCHES = 3
CHANGES_PER_BATCH = 50

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

# Brazilian names for PII generation
BRAZILIAN_FIRST_NAMES = [
    'Joao', 'Maria', 'Carlos', 'Ana', 'Paulo', 'Fernanda',
    'Pedro', 'Julia', 'Lucas', 'Beatriz', 'Gabriel', 'Leticia',
    'Rafael', 'Camila', 'Bruno', 'Amanda', 'Felipe', 'Larissa',
    'Matheus', 'Isabela', 'Gustavo', 'Mariana', 'Rodrigo', 'Patricia'
]

BRAZILIAN_LAST_NAMES = [
    'Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues',
    'Ferreira', 'Alves', 'Pereira', 'Lima', 'Gomes',
    'Costa', 'Ribeiro', 'Martins', 'Carvalho', 'Almeida',
    'Lopes', 'Soares', 'Fernandes', 'Vieira', 'Barbosa'
]

EMAIL_DOMAINS = ['gmail.com', 'hotmail.com', 'yahoo.com.br', 'outlook.com', 'uol.com.br']

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


def generate_brazilian_name():
    """Generate a random Brazilian full name."""
    first_name = random.choice(BRAZILIAN_FIRST_NAMES)
    last_name = random.choice(BRAZILIAN_LAST_NAMES)
    return first_name, last_name


def generate_email(first_name, last_name):
    """Generate a realistic email address based on name."""
    domain = random.choice(EMAIL_DOMAINS)
    patterns = [
        f"{first_name.lower()}.{last_name.lower()}",
        f"{first_name.lower()}{last_name.lower()}",
        f"{first_name.lower()}.{last_name.lower()}{random.randint(10, 99)}",
        f"{first_name.lower()}_{last_name.lower()}",
        f"{first_name.lower()[0]}{last_name.lower()}"
    ]
    local_part = random.choice(patterns)
    return f"{local_part}@{domain}"


def generate_phone():
    """Generate a Brazilian mobile phone number."""
    ddd = random.randint(11, 99)
    first_part = random.randint(1000, 9999)
    second_part = random.randint(1000, 9999)
    return f"+55 ({ddd}) 9{first_part}-{second_part}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Initial Load Data

# COMMAND ----------

def generate_customers(count):
    """Generate initial customer data with PII fields."""
    customers = []
    for _ in range(count):
        state, city, zip_prefix = random_brazilian_location()
        first_name, last_name = generate_brazilian_name()
        customers.append({
            'customer_id': generate_uuid(),
            'customer_unique_id': generate_uuid(),
            'customer_zip_code_prefix': zip_prefix,
            'customer_city': city,
            'customer_state': state,
            'customer_name': f"{first_name} {last_name}",
            'customer_email': generate_email(first_name, last_name),
            'customer_phone': generate_phone()
        })
    return customers


def generate_products(count):
    """Generate initial product data."""
    products = []
    for _ in range(count):
        products.append({
            'product_id': generate_uuid(),
            'product_category_name': random.choice(PRODUCT_CATEGORIES),
            'product_name_lenght': random.randint(10, 100),
            'product_description_lenght': random.randint(50, 500),
            'product_photos_qty': random.randint(1, 10),
            'product_weight_g': random.randint(100, 50000),
            'product_length_cm': random.randint(5, 100),
            'product_height_cm': random.randint(5, 100),
            'product_width_cm': random.randint(5, 100)
        })
    return products


def generate_sellers(count):
    """Generate initial seller data."""
    sellers = []
    for _ in range(count):
        state, city, zip_prefix = random_brazilian_location()
        sellers.append({
            'seller_id': generate_uuid(),
            'seller_zip_code_prefix': zip_prefix,
            'seller_city': city,
            'seller_state': state
        })
    return sellers


def generate_geolocation():
    """Generate geolocation reference data."""
    geolocations = []
    for state, city in BRAZILIAN_STATES.items():
        for _ in range(10):
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

def generate_orders(count, customer_ids, seller_ids, product_ids):
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
        orders.append(order)

        num_items = random.randint(1, 5)
        for item_id in range(1, num_items + 1):
            order_items.append({
                'order_id': order_id,
                'order_item_id': item_id,
                'product_id': random.choice(product_ids),
                'seller_id': random.choice(seller_ids),
                'shipping_limit_date': (purchase_time + timedelta(days=random.randint(1, 7))).isoformat(),
                'price': round(random.uniform(10, 1000), 2),
                'freight_value': round(random.uniform(5, 100), 2)
            })

        num_payments = random.randint(1, 3)
        total_value = sum(item['price'] + item['freight_value'] for item in order_items if item['order_id'] == order_id)
        payment_per = total_value / num_payments
        for seq in range(1, num_payments + 1):
            order_payments.append({
                'order_id': order_id,
                'payment_sequential': seq,
                'payment_type': random.choice(PAYMENT_TYPES),
                'payment_installments': random.randint(1, 12),
                'payment_value': round(payment_per, 2)
            })

        if status == 'delivered' and random.random() > 0.3:
            review_date = purchase_time + timedelta(days=random.randint(10, 60))
            order_reviews.append({
                'review_id': generate_uuid(),
                'order_id': order_id,
                'review_score': random.randint(1, 5),
                'review_comment_title': 'Review Title' if random.random() > 0.5 else None,
                'review_comment_message': 'This is a review comment.' if random.random() > 0.5 else None,
                'review_creation_date': review_date.isoformat(),
                'review_answer_timestamp': (review_date + timedelta(days=random.randint(1, 7))).isoformat()
            })

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

        operation_roll = random.random()
        if operation_roll < 0.6 and existing_records:
            operation = 'UPDATE'
            record = random.choice(existing_records).copy()
        elif operation_roll < 0.9:
            operation = 'INSERT'
            if entity_type == 'customers':
                state, city, zip_prefix = random_brazilian_location()
                first_name, last_name = generate_brazilian_name()
                record = {
                    'customer_id': generate_uuid(),
                    'customer_unique_id': generate_uuid(),
                    'customer_zip_code_prefix': zip_prefix,
                    'customer_city': city,
                    'customer_state': state,
                    'customer_name': f"{first_name} {last_name}",
                    'customer_email': generate_email(first_name, last_name),
                    'customer_phone': generate_phone()
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
            if existing_records:
                operation = 'DELETE'
                record = random.choice(existing_records).copy()
            else:
                continue

        if operation == 'UPDATE':
            if entity_type == 'customers':
                state, city, zip_prefix = random_brazilian_location()
                record['customer_city'] = city
                record['customer_state'] = state
                record['customer_zip_code_prefix'] = zip_prefix
                if random.random() < 0.3:
                    first_name, last_name = generate_brazilian_name()
                    record['customer_email'] = generate_email(first_name, last_name)
                if random.random() < 0.2:
                    record['customer_phone'] = generate_phone()
            elif entity_type == 'products':
                record['product_category_name'] = random.choice(PRODUCT_CATEGORIES)
                record['product_weight_g'] = random.randint(100, 50000)
            elif entity_type == 'sellers':
                state, city, zip_prefix = random_brazilian_location()
                record['seller_city'] = city
                record['seller_state'] = state

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

print("=" * 60)
print("GENERATING INITIAL LOAD DATA")
print("=" * 60)

customers = generate_customers(INITIAL_CUSTOMERS)
products = generate_products(INITIAL_PRODUCTS)
sellers = generate_sellers(INITIAL_SELLERS)
geolocations = generate_geolocation()

customer_ids = [c['customer_id'] for c in customers]
product_ids = [p['product_id'] for p in products]
seller_ids = [s['seller_id'] for s in sellers]

orders, order_items, order_payments, order_reviews = generate_orders(
    INITIAL_ORDERS, customer_ids, seller_ids, product_ids
)

print(f"\nGenerated:")
print(f"  - {len(customers)} customers (with PII: name, email, phone)")
print(f"  - {len(products)} products")
print(f"  - {len(sellers)} sellers")
print(f"  - {len(geolocations)} geolocation records")
print(f"  - {len(orders)} orders")
print(f"  - {len(order_items)} order items")
print(f"  - {len(order_payments)} order payments")
print(f"  - {len(order_reviews)} order reviews")

# COMMAND ----------

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

print("\n" + "=" * 60)
print("GENERATING CDC BATCHES")
print("=" * 60)

for batch in range(CDC_BATCHES):
    print(f"\n--- Batch {batch + 1} ---")

    customer_changes = generate_cdc_batch(customers, 'customers', batch, CHANGES_PER_BATCH)
    product_changes = generate_cdc_batch(products, 'products', batch, CHANGES_PER_BATCH)
    seller_changes = generate_cdc_batch(sellers, 'sellers', batch, CHANGES_PER_BATCH)

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

print("Generated files in volume:")
for entity in ['customers', 'products', 'sellers', 'orders', 'order_items', 'order_payments', 'order_reviews', 'geolocation']:
    try:
        files = dbutils.fs.ls(f"{VOLUME_PATH}/{entity}/")
        print(f"\n{entity}/:")
        for f in files:
            print(f"  - {f.name}")
    except Exception as e:
        print(f"\n{entity}/: No files yet")

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
# MAGIC ### Initial Load (Append-Only)
# MAGIC | Entity | Records | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | customers | 1,000 | Customer profiles with PII (name, email, phone) |
# MAGIC | products | 500 | Product catalog |
# MAGIC | sellers | 100 | Seller profiles |
# MAGIC | orders | 5,000 | Order transactions |
# MAGIC | order_items | ~15,000 | Order line items |
# MAGIC | order_payments | ~7,500 | Payment records |
# MAGIC | order_reviews | ~2,000 | Customer reviews |
# MAGIC | geolocation | 120 | Geographic reference |
# MAGIC
# MAGIC ### CDC Batches
# MAGIC | Batch | Changes per Entity | Operations |
# MAGIC |-------|-------------------|------------|
# MAGIC | 1 | 50 | INSERT, UPDATE, DELETE |
# MAGIC | 2 | 50 | INSERT, UPDATE, DELETE |
# MAGIC | 3 | 50 | INSERT, UPDATE, DELETE |
# MAGIC
# MAGIC ### PII Fields (for Unity Catalog Masking)
# MAGIC | Field | Description | Masking Strategy |
# MAGIC |-------|-------------|------------------|
# MAGIC | customer_name | Full name (first + last) | Partial mask: "J*** Silva" |
# MAGIC | customer_email | Email address | Partial mask: "jo****@gmail.com" |
# MAGIC | customer_phone | Brazilian mobile number | Partial mask: "+55 (11) 9****-****" |
# MAGIC
# MAGIC ### Next Steps
# MAGIC 1. Run the Lakeflow pipeline to process initial load
# MAGIC 2. Apply Unity Catalog column masks for PII protection
# MAGIC 3. Configure row filters based on user groups
# MAGIC 4. Query silver tables to verify masked data
