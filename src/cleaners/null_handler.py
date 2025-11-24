from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def remove_null_critical_fields(
    df: DataFrame, critical_fields: List[str]
) -> DataFrame:
    """Remove records with null values in critical fields.

    Args:
        df: Input DataFrame
        critical_fields: List of column names that must not be null

    Returns:
        DataFrame with no nulls in critical fields
    """
    initial_count = df.count()

    filter_condition = F.col(critical_fields[0]).isNotNull()
    for field in critical_fields[1:]:
        filter_condition = filter_condition & F.col(field).isNotNull()

    df_clean = df.filter(filter_condition)

    final_count = df_clean.count()
    discarded = initial_count - final_count

    logger.info(
        f"Null handling for {critical_fields}: "
        f"{final_count:,} valid, {discarded:,} discarded"
    )

    if discarded > 0:
        discard_rate = (discarded / initial_count) * 100
        logger.warning(
            f"Discarded {discard_rate:.2f}% of records "
            f"due to nulls in critical fields"
        )

    return df_clean
