from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.utils.logger import setup_logger

logger = setup_logger(__name__)

EARTH_RADIUS_KM: float = 6371.0


def calculate_haversine_distance(
    lat1_col: str, lon1_col: str, lat2_col: str, lon2_col: str
) -> F.Column:
    """Calculate great-circle distance between two points using Haversine.

    Args:
        lat1_col: Column name for first point latitude
        lon1_col: Column name for first point longitude
        lat2_col: Column name for second point latitude
        lon2_col: Column name for second point longitude

    Returns:
        Column expression for distance in kilometers
    """
    lat1_rad = F.radians(F.col(lat1_col))
    lat2_rad = F.radians(F.col(lat2_col))
    lon1_rad = F.radians(F.col(lon1_col))
    lon2_rad = F.radians(F.col(lon2_col))

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = F.sin(dlat / 2) ** 2 + F.cos(lat1_rad) * F.cos(
        lat2_rad
    ) * F.sin(dlon / 2) ** 2

    c = 2 * F.asin(F.sqrt(a))

    return EARTH_RADIUS_KM * c


def add_distance_from_previous(df: DataFrame) -> DataFrame:
    """Add distance calculation from previous location per device.

    Adds columns:
    - prev_latitude: Previous latitude for the device
    - prev_longitude: Previous longitude for the device
    - prev_timestamp: Previous timestamp for the device
    - distance_km: Distance from previous location in kilometers
    - time_diff_hours: Time difference from previous reading in hours

    Args:
        df: DataFrame with device_id, latitude, longitude, timestamp

    Returns:
        DataFrame with distance and time difference columns
    """
    logger.info("Calculating distances from previous locations")

    window_device = Window.partitionBy("device_id").orderBy(
        "timestamp"
    )

    df_with_prev = (
        df.withColumn(
            "prev_latitude", F.lag("latitude").over(window_device)
        )
        .withColumn(
            "prev_longitude", F.lag("longitude").over(window_device)
        )
        .withColumn(
            "prev_timestamp", F.lag("timestamp").over(window_device)
        )
    )

    df_with_distance = df_with_prev.withColumn(
        "distance_km",
        calculate_haversine_distance(
            "prev_latitude", "prev_longitude", "latitude", "longitude"
        ),
    ).withColumn(
        "time_diff_hours",
        (
            F.unix_timestamp("timestamp")
            - F.unix_timestamp("prev_timestamp")
        )
        / 3600,
    )

    logger.info("Distance calculations added")

    return df_with_distance
