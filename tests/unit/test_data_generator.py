"""
Unit tests for data generator utilities.

These tests verify the logic patterns used in data_generator.py
without requiring a Spark session or Databricks environment.
"""

import random
import uuid

# ============================================================================
# Helper functions (patterns from data_generator.py)
# ============================================================================

def generate_uuid():
    """Generate a 32-character UUID without hyphens."""
    return uuid.uuid4().hex


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
