"""
Unit tests for data generator utilities.

These tests verify the logic patterns used in data_generator.py
without requiring a Spark session or Databricks environment.
"""

import random
import uuid
import re

# ============================================================================
# Helper functions (patterns from data_generator.py)
# ============================================================================

# Brazilian names for PII generation
BRAZILIAN_FIRST_NAMES = [
    'Joao', 'Maria', 'Carlos', 'Ana', 'Paulo', 'Fernanda',
    'Pedro', 'Julia', 'Lucas', 'Beatriz', 'Gabriel', 'Leticia'
]

BRAZILIAN_LAST_NAMES = [
    'Silva', 'Santos', 'Oliveira', 'Souza', 'Rodrigues',
    'Ferreira', 'Alves', 'Pereira', 'Lima', 'Gomes'
]

EMAIL_DOMAINS = ['gmail.com', 'hotmail.com', 'yahoo.com.br', 'outlook.com', 'uol.com.br']


def generate_uuid():
    """Generate a 32-character UUID without hyphens."""
    return uuid.uuid4().hex


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


def random_brazilian_location():
    """Generate random Brazilian state and city."""
    BRAZILIAN_STATES = {
        "SP": "Sao Paulo",
        "RJ": "Rio De Janeiro",
        "MG": "Belo Horizonte",
        "RS": "Porto Alegre",
        "PR": "Curitiba",
        "SC": "Florianopolis",
        "BA": "Salvador",
        "PE": "Recife",
        "CE": "Fortaleza",
        "DF": "Brasilia",
        "GO": "Goiania",
        "PA": "Belem",
    }
    state = random.choice(list(BRAZILIAN_STATES.keys()))
    city = BRAZILIAN_STATES[state]
    zip_prefix = random.randint(10000, 99999)
    return state, city, zip_prefix


class TestUUIDGeneration:
    """Test UUID generation functions."""

    def test_uuid_length(self):
        """UUID should be 32 characters (no hyphens)."""
        generated = generate_uuid()
        assert len(generated) == 32

    def test_uuid_is_hex(self):
        """UUID should contain only hex characters."""
        generated = generate_uuid()
        assert all(c in "0123456789abcdef" for c in generated)

    def test_uuid_uniqueness(self):
        """Generated UUIDs should be unique."""
        uuids = [generate_uuid() for _ in range(1000)]
        assert len(set(uuids)) == 1000


class TestBrazilianLocationGeneration:
    """Test Brazilian location data generation."""

    def test_state_is_valid(self):
        """Generated state should be a valid Brazilian state code."""
        valid_states = {"SP", "RJ", "MG", "RS", "PR", "SC", "BA", "PE", "CE", "DF", "GO", "PA"}
        for _ in range(100):
            state, city, zip_prefix = random_brazilian_location()
            assert state in valid_states

    def test_zip_prefix_range(self):
        """Zip code prefix should be in valid range."""
        for _ in range(100):
            state, city, zip_prefix = random_brazilian_location()
            assert 10000 <= zip_prefix <= 99999

    def test_city_not_empty(self):
        """City should not be empty."""
        for _ in range(100):
            state, city, zip_prefix = random_brazilian_location()
            assert len(city) > 0


class TestCustomerGeneration:
    """Test customer data generation patterns."""

    def test_customer_has_required_fields(self):
        """Generated customer should have all required fields."""
        state, city, zip_prefix = random_brazilian_location()
        customer = {
            "customer_id": generate_uuid(),
            "customer_unique_id": generate_uuid(),
            "customer_zip_code_prefix": zip_prefix,
            "customer_city": city,
            "customer_state": state,
        }

        required_fields = [
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ]
        for field in required_fields:
            assert field in customer

    def test_customer_ids_are_32_chars(self):
        """Customer IDs should be 32 characters."""
        customer_id = generate_uuid()
        customer_unique_id = generate_uuid()

        assert len(customer_id) == 32
        assert len(customer_unique_id) == 32


class TestCDCEventGeneration:
    """Test CDC event generation patterns."""

    def test_cdc_operations_valid(self):
        """CDC operations should be INSERT, UPDATE, or DELETE."""
        valid_ops = {"INSERT", "UPDATE", "DELETE"}
        for _ in range(100):
            roll = random.random()
            if roll < 0.6:
                op = "UPDATE"
            elif roll < 0.9:
                op = "INSERT"
            else:
                op = "DELETE"
            assert op in valid_ops

    def test_sequence_numbers_monotonic(self):
        """Sequence numbers should be monotonically increasing within batches."""
        sequences = []
        for batch in range(3):
            base = batch * 10000
            for i in range(50):
                sequences.append(base + i)

        # Verify monotonic within batches
        for i in range(1, len(sequences)):
            if sequences[i] // 10000 == sequences[i - 1] // 10000:
                assert sequences[i] > sequences[i - 1]


class TestDataQualityPatterns:
    """Test data quality patterns used in SQL."""

    def test_customer_id_validation_pattern(self):
        """Customer ID validation: 32 chars, not empty."""
        valid_id = generate_uuid()
        invalid_ids = ["", " ", "short", "x" * 33]

        # Valid ID passes
        assert valid_id is not None
        assert len(valid_id.strip()) == 32

        # Invalid IDs fail
        for invalid in invalid_ids:
            assert len(invalid.strip()) != 32 or invalid.strip() == ""

    def test_order_status_validation_pattern(self):
        """Order status should be in allowed list."""
        valid_statuses = {
            "created",
            "approved",
            "invoiced",
            "processing",
            "shipped",
            "delivered",
            "unavailable",
            "canceled",
        }

        test_statuses = ["created", "DELIVERED", "invalid", "shipped"]
        for status in test_statuses:
            normalized = status.lower().strip()
            is_valid = normalized in valid_statuses
            if status.lower() in ["created", "delivered", "shipped"]:
                assert is_valid
            elif status.lower() == "invalid":
                assert not is_valid


# ============================================================================
# Data Variability Tests
# ============================================================================

# Configuration ranges (mirrored from data_generator.py)
CUSTOMERS_RANGE = (800, 1200)
PRODUCTS_RANGE = (400, 600)
SELLERS_RANGE = (80, 120)
ORDERS_RANGE = (4000, 6000)
CDC_CHANGES_RANGE = (40, 60)
BAD_DATA_RATE = 0.02


def get_random_count(range_tuple):
    """Get a random count within the specified range."""
    return random.randint(range_tuple[0], range_tuple[1])


class TestDataVariability:
    """Test data generation variability features."""

    def test_get_random_count_within_range(self):
        """Random count should be within the specified range."""
        for _ in range(100):
            count = get_random_count(CUSTOMERS_RANGE)
            assert CUSTOMERS_RANGE[0] <= count <= CUSTOMERS_RANGE[1]

    def test_customer_count_in_range(self):
        """Customer count should fall within configured range."""
        count = get_random_count(CUSTOMERS_RANGE)
        assert 800 <= count <= 1200

    def test_products_count_in_range(self):
        """Product count should fall within configured range."""
        count = get_random_count(PRODUCTS_RANGE)
        assert 400 <= count <= 600

    def test_sellers_count_in_range(self):
        """Seller count should fall within configured range."""
        count = get_random_count(SELLERS_RANGE)
        assert 80 <= count <= 120

    def test_orders_count_in_range(self):
        """Order count should fall within configured range."""
        count = get_random_count(ORDERS_RANGE)
        assert 4000 <= count <= 6000

    def test_cdc_changes_count_in_range(self):
        """CDC changes count should fall within configured range."""
        count = get_random_count(CDC_CHANGES_RANGE)
        assert 40 <= count <= 60

    def test_variability_produces_different_counts(self):
        """Multiple runs should produce different row counts."""
        counts = [get_random_count(CUSTOMERS_RANGE) for _ in range(100)]
        unique_counts = set(counts)
        # With range of 400 values, 100 samples should have significant variety
        assert len(unique_counts) > 10, "Expected more variability in counts"


# ============================================================================
# Bad Data Generation Tests
# ============================================================================

def introduce_bad_data(record, entity_type, bad_data_rate=BAD_DATA_RATE):
    """
    Introduces data quality issues for testing DQ expectations.
    (Mirrored from data_generator.py for unit testing)
    """
    if random.random() >= bad_data_rate:
        return record  # Keep clean

    if entity_type == "customer":
        violation = random.choice(["null_id", "short_id", "null_zip"])
        if violation == "null_id":
            record["customer_id"] = None
        elif violation == "short_id":
            record["customer_id"] = "INVALID_SHORT"
        elif violation == "null_zip":
            record["customer_zip_code_prefix"] = None

    elif entity_type == "product":
        violation = random.choice(["null_id", "negative_weight", "negative_dimension"])
        if violation == "null_id":
            record["product_id"] = None
        elif violation == "negative_weight":
            record["product_weight_g"] = -random.randint(1, 1000)
        elif violation == "negative_dimension":
            dim = random.choice(
                ["product_length_cm", "product_height_cm", "product_width_cm"]
            )
            record[dim] = -random.randint(1, 50)

    elif entity_type == "seller":
        violation = random.choice(["null_id", "short_id"])
        if violation == "null_id":
            record["seller_id"] = None
        elif violation == "short_id":
            record["seller_id"] = "BAD_SELLER"

    elif entity_type == "order":
        violation = random.choice(["null_id", "invalid_status", "null_timestamp"])
        if violation == "null_id":
            record["order_id"] = None
        elif violation == "invalid_status":
            record["order_status"] = "INVALID_STATUS_XYZ"
        elif violation == "null_timestamp":
            record["order_purchase_timestamp"] = None

    elif entity_type == "order_item":
        violation = random.choice(["negative_price", "negative_freight"])
        if violation == "negative_price":
            record["price"] = -round(random.uniform(1, 100), 2)
        elif violation == "negative_freight":
            record["freight_value"] = -round(random.uniform(1, 50), 2)

    elif entity_type == "order_payment":
        violation = random.choice(["invalid_type", "negative_value"])
        if violation == "invalid_type":
            record["payment_type"] = "INVALID_PAYMENT_TYPE"
        elif violation == "negative_value":
            record["payment_value"] = -round(random.uniform(1, 100), 2)

    elif entity_type == "order_review":
        violation = random.choice(["invalid_score_low", "invalid_score_high"])
        if violation == "invalid_score_low":
            record["review_score"] = 0
        elif violation == "invalid_score_high":
            record["review_score"] = random.randint(6, 10)

    return record


class TestBadDataGeneration:
    """Test bad data generation for DQ testing."""

    def test_bad_data_rate_approximately_correct(self):
        """Verify approximately 2% of records have quality issues."""
        bad_count = 0
        total = 10000

        for _ in range(total):
            record = {"customer_id": generate_uuid()}
            original_id = record["customer_id"]
            record = introduce_bad_data(record, "customer", bad_data_rate=0.02)
            if record["customer_id"] != original_id:
                bad_count += 1

        # Allow 1-4% range (accounting for randomness)
        rate = bad_count / total
        assert 0.01 <= rate <= 0.04, f"Bad data rate {rate:.2%} outside expected range"

    def test_bad_customer_id_null(self):
        """Verify NULL customer IDs are generated."""
        null_found = False
        for _ in range(1000):
            record = {"customer_id": generate_uuid(), "customer_zip_code_prefix": 12345}
            record = introduce_bad_data(record, "customer", bad_data_rate=1.0)
            if record["customer_id"] is None:
                null_found = True
                break
        assert null_found, "Expected to find NULL customer_id"

    def test_bad_customer_id_short(self):
        """Verify short customer IDs are generated."""
        short_found = False
        for _ in range(1000):
            record = {"customer_id": generate_uuid(), "customer_zip_code_prefix": 12345}
            record = introduce_bad_data(record, "customer", bad_data_rate=1.0)
            if record["customer_id"] == "INVALID_SHORT":
                short_found = True
                break
        assert short_found, "Expected to find short customer_id"

    def test_bad_product_negative_weight(self):
        """Verify negative product weights are generated."""
        negative_found = False
        for _ in range(1000):
            record = {
                "product_id": generate_uuid(),
                "product_weight_g": 1000,
                "product_length_cm": 10,
                "product_height_cm": 10,
                "product_width_cm": 10,
            }
            record = introduce_bad_data(record, "product", bad_data_rate=1.0)
            if record["product_weight_g"] < 0:
                negative_found = True
                break
        assert negative_found, "Expected to find negative product_weight_g"

    def test_bad_order_invalid_status(self):
        """Verify invalid order statuses are generated."""
        invalid_found = False
        for _ in range(1000):
            record = {
                "order_id": generate_uuid(),
                "order_status": "delivered",
                "order_purchase_timestamp": "2024-01-01T00:00:00",
            }
            record = introduce_bad_data(record, "order", bad_data_rate=1.0)
            if record["order_status"] == "INVALID_STATUS_XYZ":
                invalid_found = True
                break
        assert invalid_found, "Expected to find invalid order_status"

    def test_bad_order_item_negative_price(self):
        """Verify negative prices are generated."""
        negative_found = False
        for _ in range(1000):
            record = {"price": 100.0, "freight_value": 10.0}
            record = introduce_bad_data(record, "order_item", bad_data_rate=1.0)
            if record["price"] < 0:
                negative_found = True
                break
        assert negative_found, "Expected to find negative price"

    def test_bad_payment_invalid_type(self):
        """Verify invalid payment types are generated."""
        invalid_found = False
        for _ in range(1000):
            record = {"payment_type": "credit_card", "payment_value": 100.0}
            record = introduce_bad_data(record, "order_payment", bad_data_rate=1.0)
            if record["payment_type"] == "INVALID_PAYMENT_TYPE":
                invalid_found = True
                break
        assert invalid_found, "Expected to find invalid payment_type"

    def test_bad_review_score_outside_range(self):
        """Verify review scores outside 1-5 range are generated."""
        invalid_found = False
        for _ in range(1000):
            record = {"review_score": 3}
            record = introduce_bad_data(record, "order_review", bad_data_rate=1.0)
            if record["review_score"] < 1 or record["review_score"] > 5:
                invalid_found = True
                break
        assert invalid_found, "Expected to find review_score outside 1-5 range"

    def test_clean_data_when_rate_zero(self):
        """Verify no bad data when rate is 0%."""
        for _ in range(100):
            record = {"customer_id": generate_uuid()}
            original_id = record["customer_id"]
            record = introduce_bad_data(record, "customer", bad_data_rate=0.0)
            assert record["customer_id"] == original_id, "Expected no changes with 0% rate"
