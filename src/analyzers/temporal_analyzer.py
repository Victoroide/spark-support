from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def analyze_temporal_patterns(df: DataFrame) -> DataFrame:
    """Calculate temporal usage patterns across hours and weekdays.

    Output columns:
    - pattern_type: Type of pattern ('hourly' or 'weekday')
    - dimension: Hour (0-23) or day (1-7 with name)
    - record_count: Number of records for this dimension

    Args:
        df: Input DataFrame with hour and day_of_week columns

    Returns:
        DataFrame with temporal pattern statistics
    """
    logger.info("Analyzing temporal patterns")

    hourly_distribution = (
        df.groupBy("hour")
        .agg(F.count("*").alias("record_count"))
        .orderBy("hour")
    )

    hourly_patterns = (
        hourly_distribution.withColumn(
            "pattern_type", F.lit("hourly")
        )
        .withColumn("dimension", F.col("hour").cast("string"))
        .select("pattern_type", "dimension", "record_count")
    )

    weekday_distribution = (
        df.groupBy("day_of_week", "weekday_name")
        .agg(F.count("*").alias("record_count"))
        .orderBy("day_of_week")
    )

    weekday_patterns = (
        weekday_distribution.withColumn(
            "pattern_type", F.lit("weekday")
        )
        .withColumn(
            "dimension",
            F.concat(
                F.col("day_of_week").cast("string"),
                F.lit("-"),
                F.col("weekday_name"),
            ),
        )
        .select("pattern_type", "dimension", "record_count")
    )

    combined_patterns = hourly_patterns.union(weekday_patterns)

    logger.info("Temporal pattern analysis complete")

    return combined_patterns
