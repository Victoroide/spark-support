from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def clean_timestamps(df: DataFrame) -> DataFrame:
    """Remove records with invalid or null timestamps.

    Args:
        df: Input DataFrame with timestamp column

    Returns:
        DataFrame with only valid timestamps
    """
    initial_count = df.count()

    df_clean = df.filter(F.col("timestamp").isNotNull())

    final_count = df_clean.count()
    discarded = initial_count - final_count

    logger.info(
        f"Timestamp cleaning: {final_count:,} valid, "
        f"{discarded:,} discarded"
    )

    if discarded > 0:
        discard_rate = (discarded / initial_count) * 100
        logger.warning(
            f"Discarded {discard_rate:.2f}% of records "
            f"due to invalid timestamps"
        )

    return df_clean
