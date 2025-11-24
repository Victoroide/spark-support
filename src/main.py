import sys
import time
from typing import Dict

from src.analyzers.anomaly_detector import detect_all_anomalies
from src.analyzers.device_analyzer import analyze_device_metrics
from src.analyzers.network_analyzer import analyze_network_distribution
from src.analyzers.temporal_analyzer import analyze_temporal_patterns
from src.cleaners.coordinate_cleaner import clean_coordinates
from src.cleaners.null_handler import remove_null_critical_fields
from src.cleaners.timestamp_cleaner import clean_timestamps
from src.config.settings import (
    ANOMALIES_PATH,
    DEVICE_STATS_PATH,
    INPUT_CSV_PATH,
    NETWORK_ANALYSIS_PATH,
    TEMPORAL_PATTERNS_PATH,
)
from src.exporters.result_writer import (
    write_anomalies,
    write_device_stats,
    write_network_analysis,
    write_temporal_patterns,
)
from src.loaders.csv_loader import load_location_data
from src.transformers.geospatial_calc import add_distance_from_previous
from src.transformers.temporal_features import add_temporal_features
from src.utils.logger import setup_logger
from src.utils.spark_session import create_spark_session

logger = setup_logger(__name__)


def main() -> int:
    """Execute complete ETL pipeline for mobile tracking data.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    start_time = time.time()
    spark = None

    try:
        logger.info("=" * 60)
        logger.info("Starting Mobile Tracking ETL Pipeline")
        logger.info("=" * 60)

        spark = create_spark_session()

        df_raw = load_location_data(spark, INPUT_CSV_PATH)
        initial_count = df_raw.count()

        logger.info("Starting data cleaning pipeline")
        df_clean = clean_coordinates(df_raw)
        df_clean = clean_timestamps(df_clean)
        df_clean = remove_null_critical_fields(df_clean, ["device_id"])

        clean_count = df_clean.count()
        discarded_count = initial_count - clean_count

        logger.info("Starting data transformation pipeline")
        df_transformed = add_temporal_features(df_clean)
        df_transformed = add_distance_from_previous(df_transformed)

        logger.info("Starting analysis pipeline")
        device_stats = analyze_device_metrics(df_transformed)
        network_analysis = analyze_network_distribution(df_transformed)
        temporal_patterns = analyze_temporal_patterns(df_transformed)
        anomalies = detect_all_anomalies(df_transformed)

        logger.info("Starting export pipeline")
        write_device_stats(device_stats, DEVICE_STATS_PATH)
        write_network_analysis(network_analysis, NETWORK_ANALYSIS_PATH)
        write_temporal_patterns(temporal_patterns, TEMPORAL_PATTERNS_PATH)
        write_anomalies(anomalies, ANOMALIES_PATH)

        elapsed_time = time.time() - start_time

        _print_execution_summary(
            elapsed_time,
            initial_count,
            clean_count,
            discarded_count,
            device_stats.count(),
            anomalies.count(),
        )

        logger.info("ETL pipeline completed successfully")
        return 0

    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}", exc_info=True)
        return 1

    finally:
        if spark is not None:
            import warnings

            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    if sys.platform == "win32":
                        try:
                            log4j = spark._jvm.org.apache.log4j
                            log4j.LogManager.getRootLogger().setLevel(
                                log4j.Level.OFF
                            )
                        except:
                            pass

                    spark.stop()
                except:
                    pass

        logger.info("Pipeline finalizado")

        if sys.platform == "win32":
            import os

            os._exit(0)


def _print_execution_summary(
    elapsed_time: float,
    initial_count: int,
    clean_count: int,
    discarded_count: int,
    unique_devices: int,
    anomaly_count: int,
) -> None:
    """Print formatted execution summary."""
    logger.info("=" * 60)
    logger.info("ETL PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total execution time: {elapsed_time:.2f} seconds")
    logger.info(f"Initial records: {initial_count:,}")
    logger.info(f"Clean records: {clean_count:,}")
    logger.info(f"Discarded records: {discarded_count:,}")
    logger.info(
        f"Discard rate: {(discarded_count/initial_count)*100:.2f}%"
    )
    logger.info(f"Unique devices: {unique_devices:,}")
    logger.info(f"Anomalies detected: {anomaly_count:,}")
    logger.info("=" * 60)


if __name__ == "__main__":
    sys.exit(main())
