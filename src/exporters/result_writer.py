import sys
import time
from pathlib import Path
from typing import List, Optional

from pyspark.sql import DataFrame

from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def write_partitioned(
    df: DataFrame,
    output_path: Path,
    partition_cols: Optional[List[str]] = None,
    coalesce: bool = False,
) -> None:
    """Write DataFrame to CSV with platform-specific handling.

    On Windows, uses Pandas export to avoid Hadoop dependencies.
    On Linux/macOS, uses native Spark write operations.

    Args:
        df: DataFrame to write
        output_path: Target directory path
        partition_cols: Optional list of columns to partition by
        coalesce: If True, coalesce to single file before writing
    """
    start_time = time.time()
    record_count = df.count()

    output_dir = Path(output_path).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(
        f"Writing {record_count:,} records to {output_dir} "
        f"(partitioned={bool(partition_cols)})"
    )

    if sys.platform == "win32":
        logger.warning("Windows detected: using Pandas export")
        import pandas as pd

        pdf = df.toPandas()

        if partition_cols:
            for partition_value in pdf[partition_cols[0]].unique():
                subset = pdf[pdf[partition_cols[0]] == partition_value]
                partition_dir = (
                    output_dir / f"{partition_cols[0]}={partition_value}"
                )
                partition_dir.mkdir(parents=True, exist_ok=True)
                csv_path = partition_dir / "data.csv"
                subset.to_csv(str(csv_path), index=False)
                logger.info(f"Wrote partition {partition_value}")
        else:
            csv_path = output_dir / "data.csv"
            pdf.to_csv(str(csv_path), index=False)

        elapsed = time.time() - start_time
        logger.info(f"Write completed in {elapsed:.2f}s")

    else:
        path_str = str(output_dir).replace("\\", "/")
        writer = df.coalesce(1) if coalesce else df
        csv_writer = writer.write.mode("overwrite").option("header", "true")

        if partition_cols:
            csv_writer = csv_writer.partitionBy(*partition_cols)

        csv_writer.csv(path_str)

        elapsed = time.time() - start_time
        logger.info(
            f"Successfully wrote {record_count:,} records in {elapsed:.2f}s"
        )


def write_device_stats(df: DataFrame, output_path: Path) -> None:
    """Write device statistics to single CSV file."""
    logger.info("Writing device statistics")
    write_partitioned(df, output_path, coalesce=True)


def write_network_analysis(df: DataFrame, output_path: Path) -> None:
    """Write network analysis to single CSV file."""
    logger.info("Writing network analysis")
    write_partitioned(df, output_path, coalesce=True)


def write_temporal_patterns(df: DataFrame, output_path: Path) -> None:
    """Write temporal patterns to single CSV file."""
    logger.info("Writing temporal patterns")
    write_partitioned(df, output_path, coalesce=True)


def write_anomalies(df: DataFrame, output_path: Path) -> None:
    """Write anomalies partitioned by anomaly type."""
    logger.info("Writing anomalies partitioned by type")
    write_partitioned(df, output_path, partition_cols=["anomaly_type"])
