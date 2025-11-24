from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def add_temporal_features(df: DataFrame) -> DataFrame:
    """Derive temporal features from timestamp column.

    Adds the following columns:
    - date: Date extracted from timestamp
    - hour: Hour of day (0-23)
    - day_of_week: Day of week (1=Sunday, 7=Saturday)
    - weekday_name: Full weekday name (Monday, Tuesday, etc.)

    Args:
        df: DataFrame with timestamp column

    Returns:
        DataFrame with additional temporal feature columns
    """
    logger.info("Adding temporal features")

    df_temporal = (
        df.withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))
        .withColumn(
            "weekday_name", F.date_format("timestamp", "EEEE")
        )
    )

    logger.info("Temporal features added: date, hour, day_of_week")

    return df_temporal
