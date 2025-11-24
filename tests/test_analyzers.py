import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.analyzers.device_analyzer import analyze_device_metrics
from src.analyzers.network_analyzer import analyze_network_distribution


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


def test_analyze_device_metrics_aggregates_correctly(spark):
    """Test device metrics aggregation."""
    schema = StructType(
        [
            StructField("device_id", StringType(), True),
            StructField("battery", DoubleType(), True),
            StructField("signal", DoubleType(), True),
            StructField("speed", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("device_name", StringType(), True),
        ]
    )

    data = [
        ("d1", 80.0, 75.0, 50.0, "2024-01-01 10:00:00", "phone1"),
        ("d1", 70.0, 70.0, 60.0, "2024-01-01 11:00:00", "phone1"),
        ("d2", 90.0, 80.0, 40.0, "2024-01-01 10:00:00", "phone2"),
    ]

    df = spark.createDataFrame(data, schema)
    result = analyze_device_metrics(df)

    assert result.count() == 2
    assert "total_records" in result.columns
    assert "avg_battery" in result.columns


def test_analyze_network_distribution_calculates_percentages(spark):
    """Test network distribution analysis."""
    schema = StructType(
        [
            StructField("sim_operator", StringType(), True),
            StructField("network_type", StringType(), True),
        ]
    )

    data = [
        ("Operator1", "4G"),
        ("Operator1", "4G"),
        ("Operator2", "5G"),
    ]

    df = spark.createDataFrame(data, schema)
    result = analyze_network_distribution(df)

    assert result.count() == 2
    assert "percentage" in result.columns

    percentages = result.select("percentage").collect()
    total = sum([row.percentage for row in percentages])
    assert abs(total - 100.0) < 0.01
