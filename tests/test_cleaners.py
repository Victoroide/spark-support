import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.cleaners.coordinate_cleaner import clean_coordinates
from src.cleaners.null_handler import remove_null_critical_fields
from src.cleaners.timestamp_cleaner import clean_timestamps


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    spark_session = (
        SparkSession.builder.appName("test")
        .master("local[2]")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_clean_coordinates_removes_invalid_latitude(spark):
    """Test that coordinates outside valid range are removed."""
    schema = StructType(
        [
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
        ]
    )

    data = [
        (45.0, -75.0),
        (100.0, -75.0),
        (-95.0, -75.0),
        (45.0, 200.0),
    ]

    df = spark.createDataFrame(data, schema)
    df_clean = clean_coordinates(df)

    assert df_clean.count() == 1


def test_clean_timestamps_removes_nulls(spark):
    """Test that null timestamps are removed."""
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )

    data = [("1", "2024-01-01 10:00:00"), ("2", None)]

    df = spark.createDataFrame(data, schema)
    df_clean = clean_timestamps(df)

    assert df_clean.count() == 1


def test_remove_null_critical_fields(spark):
    """Test that records with nulls in critical fields are removed."""
    schema = StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("value", DoubleType(), True),
        ]
    )

    data = [("device1", 10.0), (None, 20.0), ("device2", 30.0)]

    df = spark.createDataFrame(data, schema)
    df_clean = remove_null_critical_fields(df, ["device_id"])

    assert df_clean.count() == 2
