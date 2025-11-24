import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    TimestampType,
)

from src.transformers.temporal_features import add_temporal_features


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


def test_add_temporal_features_creates_columns(spark):
    """Test that temporal features are added correctly."""
    schema = StructType(
        [StructField("timestamp", TimestampType(), True)]
    )

    data = [("2024-01-15 14:30:00",), ("2024-06-20 09:15:00",)]

    df = spark.createDataFrame(data, schema)
    result = add_temporal_features(df)

    expected_columns = ["date", "hour", "day_of_week", "weekday_name"]

    for col in expected_columns:
        assert col in result.columns

    first_row = result.first()
    assert first_row.hour == 14
