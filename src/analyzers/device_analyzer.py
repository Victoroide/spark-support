from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def analyze_device_metrics(df: DataFrame) -> DataFrame:
    """Calculate aggregated metrics per device.

    Output columns:
    - device_id: Device identifier
    - total_records: Total number of records for device
    - avg_battery: Average battery level
    - min_battery: Minimum battery level
    - avg_signal: Average signal strength
    - avg_speed: Average speed in km/h
    - max_speed: Maximum speed in km/h
    - first_activity: Timestamp of first recorded activity
    - last_activity: Timestamp of last recorded activity
    - activity_window_hours: Hours between first and last activity
    - device_name_count: Number of unique device names

    Args:
        df: Input DataFrame with device tracking data

    Returns:
        DataFrame with per-device aggregated metrics
    """
    logger.info("Calculating device metrics")

    device_stats = df.groupBy("device_id").agg(
        F.count("*").alias("total_records"),
        F.avg("battery").alias("avg_battery"),
        F.min("battery").alias("min_battery"),
        F.avg("signal").alias("avg_signal"),
        F.avg("speed").alias("avg_speed"),
        F.max("speed").alias("max_speed"),
        F.min("timestamp").alias("first_activity"),
        F.max("timestamp").alias("last_activity"),
        F.countDistinct("device_name").alias("device_name_count"),
    )

    device_stats = device_stats.withColumn(
        "activity_window_hours",
        (
            F.unix_timestamp("last_activity")
            - F.unix_timestamp("first_activity")
        )
        / 3600,
    )

    unique_devices = device_stats.count()
    logger.info(f"Device metrics calculated for {unique_devices:,} devices")

    return device_stats
