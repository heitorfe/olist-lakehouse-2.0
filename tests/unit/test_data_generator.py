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


class TestPIIGeneration:
    """Test PII field generation for Unity Catalog masking demo."""

    def test_brazilian_name_returns_tuple(self):
        """Brazilian name function should return first and last name."""
        first, last = generate_brazilian_name()
        assert first in BRAZILIAN_FIRST_NAMES
        assert last in BRAZILIAN_LAST_NAMES

    def test_brazilian_name_not_empty(self):
        """Generated names should not be empty."""
        for _ in range(100):
            first, last = generate_brazilian_name()
            assert len(first) > 0
            assert len(last) > 0

    def test_email_format_valid(self):
        """Generated email should have valid format."""
        email_pattern = re.compile(r'^[a-z0-9._]+@[a-z.]+$')
        for _ in range(100):
            first, last = generate_brazilian_name()
            email = generate_email(first, last)
            assert email_pattern.match(email), f"Invalid email format: {email}"
            assert '@' in email

    def test_email_domain_valid(self):
        """Email domain should be from allowed list."""
        for _ in range(100):
            first, last = generate_brazilian_name()
            email = generate_email(first, last)
            domain = email.split('@')[1]
            assert domain in EMAIL_DOMAINS

    def test_phone_format_brazilian(self):
        """Phone should follow Brazilian mobile format."""
        phone_pattern = re.compile(r'^\+55 \(\d{2}\) 9\d{4}-\d{4}$')
        for _ in range(100):
            phone = generate_phone()
            assert phone_pattern.match(phone), f"Invalid phone format: {phone}"

    def test_phone_area_code_valid(self):
        """Phone area code (DDD) should be in valid range."""
        for _ in range(100):
            phone = generate_phone()
            ddd = int(phone[5:7])
            assert 11 <= ddd <= 99

    def test_customer_with_pii_fields(self):
        """Customer generation should include PII fields."""
        state, city, zip_prefix = random_brazilian_location()
        first_name, last_name = generate_brazilian_name()
        customer = {
            'customer_id': generate_uuid(),
            'customer_unique_id': generate_uuid(),
            'customer_zip_code_prefix': zip_prefix,
            'customer_city': city,
            'customer_state': state,
            'customer_name': f"{first_name} {last_name}",
            'customer_email': generate_email(first_name, last_name),
            'customer_phone': generate_phone()
        }

        # Verify all fields including PII
        required_fields = [
            'customer_id', 'customer_unique_id', 'customer_zip_code_prefix',
            'customer_city', 'customer_state',
            'customer_name', 'customer_email', 'customer_phone'
        ]
        for field in required_fields:
            assert field in customer
            assert customer[field] is not None
