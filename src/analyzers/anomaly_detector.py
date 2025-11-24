from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.settings import (
    BATTERY_CRITICAL,
    GEOGRAPHIC_JUMP_THRESHOLD,
    SPEED_THRESHOLD,
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def detect_speed_anomalies(df: DataFrame) -> DataFrame:
    """Detect records with impossible speeds.

    Args:
        df: Input DataFrame with speed column

    Returns:
        DataFrame with anomalous speed records
    """
    speed_anomalies = (
        df.filter(
            (F.col("speed").isNotNull())
            & (F.col("speed") > SPEED_THRESHOLD)
        )
        .select(
            "id",
            "device_id",
            "timestamp",
            "speed",
            "latitude",
            "longitude",
        )
        .withColumn("anomaly_type", F.lit("impossible_speed"))
    )

    count = speed_anomalies.count()
    logger.info(f"Detected {count:,} impossible speed anomalies")

    return speed_anomalies


def detect_battery_anomalies(df: DataFrame) -> DataFrame:
    """Detect records with critical battery levels.

    Args:
        df: Input DataFrame with battery column

    Returns:
        DataFrame with critical battery records
    """
    battery_anomalies = (
        df.filter(
            (F.col("battery").isNotNull())
            & (F.col("battery") < BATTERY_CRITICAL)
        )
        .select(
            "id",
            "device_id",
            "timestamp",
            "battery",
            "latitude",
            "longitude",
        )
        .withColumn("anomaly_type", F.lit("critical_battery"))
    )

    count = battery_anomalies.count()
    logger.info(f"Detected {count:,} critical battery anomalies")

    return battery_anomalies


def detect_geographic_anomalies(df: DataFrame) -> DataFrame:
    """Detect records with unrealistic geographic jumps.

    Requires DataFrame to have distance_km column from geospatial_calc.

    Args:
        df: Input DataFrame with distance calculations

    Returns:
        DataFrame with geographic jump anomalies
    """
    geographic_anomalies = (
        df.filter(
            (F.col("distance_km").isNotNull())
            & (F.col("time_diff_hours").isNotNull())
            & (F.col("time_diff_hours") > 0)
            & (F.col("distance_km") > GEOGRAPHIC_JUMP_THRESHOLD)
        )
        .select(
            "id",
            "device_id",
            "timestamp",
            "distance_km",
            "time_diff_hours",
            "latitude",
            "longitude",
        )
        .withColumn("anomaly_type", F.lit("geographic_jump"))
    )

    count = geographic_anomalies.count()
    logger.info(f"Detected {count:,} geographic jump anomalies")

    return geographic_anomalies


def detect_all_anomalies(df: DataFrame) -> DataFrame:
    """Detect all types of anomalies in the dataset.

    Combines speed, battery, and geographic anomaly detection.

    Args:
        df: Input DataFrame with distance calculations

    Returns:
        Unified DataFrame containing all detected anomalies
    """
    logger.info("Starting comprehensive anomaly detection")

    speed_anomalies = detect_speed_anomalies(df)
    battery_anomalies = detect_battery_anomalies(df)
    geographic_anomalies = detect_geographic_anomalies(df)

    all_anomalies = speed_anomalies.unionByName(
        battery_anomalies, allowMissingColumns=True
    ).unionByName(geographic_anomalies, allowMissingColumns=True)

    total_anomalies = all_anomalies.count()
    logger.info(f"Total anomalies detected: {total_anomalies:,}")

    return all_anomalies
