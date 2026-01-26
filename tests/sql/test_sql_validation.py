"""
SQL validation tests for Olist Lakehouse pipelines.

These tests verify:
1. SQL files are present in expected locations
2. Required patterns are present (CREATE OR REFRESH, STREAMING TABLE, etc.)
3. Data quality constraints follow conventions
"""

import re


class TestSQLFileStructure:
    """Test SQL file structure and conventions."""

    def test_all_sql_files_exist(self, sql_files):
        """Verify SQL files exist in expected locations."""
        assert len(sql_files) >= 28, f"Expected at least 28 SQL files, found {len(sql_files)}"

    def test_bronze_files_count(self, bronze_sql_files):
        """Verify Bronze layer has expected number of files."""
        assert len(bronze_sql_files) == 8, f"Expected 8 Bronze files, found {len(bronze_sql_files)}"

    def test_silver_files_count(self, silver_sql_files):
        """Verify Silver layer has expected number of files."""
        assert len(silver_sql_files) == 9, f"Expected 9 Silver files, found {len(silver_sql_files)}"

    def test_gold_files_count(self, gold_sql_files):
        """Verify Gold layer has expected number of files."""
        assert len(gold_sql_files) == 5, f"Expected 5 Gold files, found {len(gold_sql_files)}"

    def test_cdc_files_count(self, cdc_sql_files):
        """Verify CDC pipeline has expected number of files."""
        assert len(cdc_sql_files) == 6, f"Expected 6 CDC files, found {len(cdc_sql_files)}"


class TestBronzeLayerSQL:
    """Test Bronze layer SQL conventions."""

    def test_bronze_uses_streaming_table(self, bronze_sql_files):
        """Bronze layer should use STREAMING TABLE with AutoLoader."""
        for sql_file in bronze_sql_files:
            content = sql_file.read_text()
            assert "CREATE OR REFRESH STREAMING TABLE" in content, (
                f"{sql_file.name} should use CREATE OR REFRESH STREAMING TABLE"
            )

    def test_bronze_uses_read_files(self, bronze_sql_files):
        """Bronze layer should use read_files() for AutoLoader."""
        for sql_file in bronze_sql_files:
            content = sql_file.read_text()
            has_read_files = "read_files(" in content.lower() or "STREAM read_files" in content
            assert has_read_files, f"{sql_file.name} should use read_files() for data ingestion"

    def test_bronze_has_audit_columns(self, bronze_sql_files):
        """Bronze layer should include audit columns."""
        audit_patterns = ["_ingested_at", "_source_file", "_file_modified"]
        for sql_file in bronze_sql_files:
            content = sql_file.read_text()
            has_audit = any(pattern in content for pattern in audit_patterns)
            assert has_audit, f"{sql_file.name} should include audit columns"


class TestSilverLayerSQL:
    """Test Silver layer SQL conventions."""

    def test_silver_has_data_quality_constraints(self, silver_sql_files):
        """Silver layer should have CONSTRAINT definitions."""
        for sql_file in silver_sql_files:
            content = sql_file.read_text()
            # Silver layer should have constraints (except for views like orders_enriched)
            if "MATERIALIZED VIEW" not in content:
                assert "CONSTRAINT" in content, f"{sql_file.name} should have data quality constraints"

    def test_silver_uses_drop_row_violation(self, silver_sql_files):
        """Silver layer constraints should use ON VIOLATION DROP ROW."""
        for sql_file in silver_sql_files:
            content = sql_file.read_text()
            if "CONSTRAINT" in content and "STREAMING TABLE" in content:
                assert "ON VIOLATION DROP ROW" in content, f"{sql_file.name} should use ON VIOLATION DROP ROW"


class TestGoldLayerSQL:
    """Test Gold layer SQL conventions."""

    def test_gold_uses_materialized_view(self, gold_sql_files):
        """Gold layer should use MATERIALIZED VIEW."""
        for sql_file in gold_sql_files:
            content = sql_file.read_text()
            assert "MATERIALIZED VIEW" in content, f"{sql_file.name} should use CREATE OR REFRESH MATERIALIZED VIEW"


class TestCDCLayerSQL:
    """Test CDC pipeline SQL conventions."""

    def test_cdc_bronze_uses_streaming_table(self, cdc_sql_files):
        """CDC Bronze should use STREAMING TABLE."""
        bronze_files = [f for f in cdc_sql_files if "bronze" in f.name]
        for sql_file in bronze_files:
            content = sql_file.read_text()
            assert "STREAMING TABLE" in content, f"{sql_file.name} should use STREAMING TABLE"

    def test_cdc_silver_uses_apply_changes(self, cdc_sql_files):
        """CDC Silver should use AUTO CDC for SCD processing."""
        silver_files = [f for f in cdc_sql_files if "silver" in f.name]
        for sql_file in silver_files:
            content = sql_file.read_text()
            assert "AUTO CDC" in content, f"{sql_file.name} should use APPLY CHANGES INTO for CDC"

    def test_cdc_silver_has_scd_types(self, cdc_sql_files):
        """CDC Silver should implement both SCD Type 1 and Type 2."""
        silver_files = [f for f in cdc_sql_files if "silver" in f.name]
        for sql_file in silver_files:
            content = sql_file.read_text()
            has_scd1 = "SCD TYPE 1" in content
            has_scd2 = "SCD TYPE 2" in content
            assert has_scd1 and has_scd2, f"{sql_file.name} should implement both SCD TYPE 1 and SCD TYPE 2"

    def test_cdc_silver_has_sequence_by(self, cdc_sql_files):
        """CDC Silver should have SEQUENCE BY for ordering."""
        silver_files = [f for f in cdc_sql_files if "silver" in f.name]
        for sql_file in silver_files:
            content = sql_file.read_text()
            assert "SEQUENCE BY" in content, f"{sql_file.name} should use SEQUENCE BY for ordering"

    def test_cdc_silver_handles_deletes(self, cdc_sql_files):
        """CDC Silver should handle DELETE operations."""
        silver_files = [f for f in cdc_sql_files if "silver" in f.name]
        for sql_file in silver_files:
            content = sql_file.read_text()
            assert "APPLY AS DELETE WHEN" in content, f"{sql_file.name} should handle DELETE operations"


class TestSQLSyntax:
    """Test SQL syntax patterns."""

    def test_no_hardcoded_catalogs(self, sql_files):
        """SQL should use variables, not hardcoded catalog names."""
        hardcoded_pattern = re.compile(r"FROM\s+(olist_dev|olist_staging|olist)\.", re.IGNORECASE)
        for sql_file in sql_files:
            content = sql_file.read_text()
            match = hardcoded_pattern.search(content)
            assert match is None, f"{sql_file.name} has hardcoded catalog name: {match.group() if match else ''}"

    def test_uses_catalog_variable(self, sql_files):
        """SQL with Volume paths should use ${catalog} variable."""
        for sql_file in sql_files:
            content = sql_file.read_text()
            if "/Volumes/" in content:
                assert "${catalog}" in content, f"{sql_file.name} should use ${{catalog}} variable for Volume paths"

    def test_has_comment_header(self, sql_files):
        """SQL files should have descriptive comment headers."""
        for sql_file in sql_files:
            content = sql_file.read_text()
            assert content.strip().startswith("--"), f"{sql_file.name} should start with a comment header"
