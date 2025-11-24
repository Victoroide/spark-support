from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.config.settings import LATITUDE_RANGE, LONGITUDE_RANGE
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def clean_coordinates(df: DataFrame) -> DataFrame:
    """Remove records with invalid geographic coordinates.

    Filters out records where:
    - Latitude is null or outside [-90, 90] range
    - Longitude is null or outside [-180, 180] range

    Args:
        df: Input DataFrame with latitude and longitude columns

    Returns:
        DataFrame with only valid coordinates
    """
    initial_count = df.count()

    lat_min, lat_max = LATITUDE_RANGE
    lon_min, lon_max = LONGITUDE_RANGE

    df_clean = df.filter(
        (F.col("latitude").isNotNull())
        & (F.col("longitude").isNotNull())
        & (F.col("latitude").between(lat_min, lat_max))
        & (F.col("longitude").between(lon_min, lon_max))
    )

    final_count = df_clean.count()
    discarded = initial_count - final_count

    logger.info(
        f"Coordinate cleaning: {final_count:,} valid, "
        f"{discarded:,} discarded"
    )

    if discarded > 0:
        discard_rate = (discarded / initial_count) * 100
        logger.warning(
            f"Discarded {discard_rate:.2f}% of records "
            f"due to invalid coordinates"
        )

    return df_clean
