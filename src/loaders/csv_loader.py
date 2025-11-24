from pathlib import Path
from typing import Set

from pyspark.sql import DataFrame, SparkSession

from src.config.settings import EXPECTED_COLUMNS
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class SchemaValidationError(Exception):
    """Raised when CSV schema doesn't match expected schema."""

    pass


def load_location_data(spark: SparkSession, file_path: Path) -> DataFrame:
    """Load and validate location tracking CSV data.

    Args:
        spark: Active SparkSession instance
        file_path: Path to CSV file

    Returns:
        DataFrame with validated schema

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        SchemaValidationError: If schema doesn't match expected columns
    """
    logger.info(f"Loading location data from {file_path}")

    if not file_path.exists():
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    df = spark.read.csv(
        str(file_path), header=True, inferSchema=True
    )

    initial_count = df.count()
    logger.info(f"Loaded {initial_count:,} records from CSV")

    _validate_schema(df)

    return df


def _validate_schema(df: DataFrame) -> None:
    """Validate DataFrame schema against expected columns.

    Args:
        df: DataFrame to validate

    Raises:
        SchemaValidationError: If required columns are missing
    """
    actual_columns: Set[str] = set(df.columns)
    expected_columns: Set[str] = set(EXPECTED_COLUMNS)

    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns

    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise SchemaValidationError(
            f"Missing columns: {', '.join(sorted(missing_columns))}"
        )

    if extra_columns:
        logger.warning(
            f"Extra columns found (will be ignored): {extra_columns}"
        )

    logger.info("Schema validation passed")
