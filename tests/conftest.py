"""
Pytest configuration and shared fixtures for Olist Lakehouse tests.
"""

import pytest
from pathlib import Path


@pytest.fixture(scope="session")
def project_root():
    """Return the project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def sql_files(project_root):
    """Return all SQL files in the project."""
    sql_dir = project_root / "src" / "pipelines"
    return list(sql_dir.rglob("*.sql"))


@pytest.fixture(scope="session")
def bronze_sql_files(project_root):
    """Return Bronze layer SQL files."""
    return list((project_root / "src" / "pipelines" / "bronze").glob("*.sql"))


@pytest.fixture(scope="session")
def silver_sql_files(project_root):
    """Return Silver layer SQL files."""
    return list((project_root / "src" / "pipelines" / "silver").glob("*.sql"))


@pytest.fixture(scope="session")
def gold_sql_files(project_root):
    """Return Gold layer SQL files."""
    return list((project_root / "src" / "pipelines" / "gold").glob("*.sql"))


@pytest.fixture(scope="session")
def cdc_sql_files(project_root):
    """Return CDC pipeline SQL files."""
    return list((project_root / "src" / "pipelines" / "cdc").glob("*.sql"))
