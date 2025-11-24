from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def analyze_network_distribution(df: DataFrame) -> DataFrame:
    """Calculate distribution of records by network operator and type.

    Output columns:
    - sim_operator: SIM operator name
    - network_type: Network type (3G, 4G, 5G, etc.)
    - record_count: Number of records for this combination
    - percentage: Percentage of total records

    Args:
        df: Input DataFrame with sim_operator and network_type columns

    Returns:
        DataFrame with network distribution metrics sorted by count
    """
    logger.info("Analyzing network distribution")

    total_records = df.count()

    network_stats = df.groupBy("sim_operator", "network_type").agg(
        F.count("*").alias("record_count")
    )

    network_stats = network_stats.withColumn(
        "percentage", (F.col("record_count") / total_records) * 100
    )

    network_stats = network_stats.orderBy(F.col("record_count").desc())

    unique_combinations = network_stats.count()
    logger.info(
        f"Found {unique_combinations} unique operator/network combinations"
    )

    return network_stats
