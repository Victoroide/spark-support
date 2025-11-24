import os
import sys
import tempfile
from pathlib import Path

from pyspark.sql import SparkSession

from src.config.settings import SPARK_CONFIG
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def create_spark_session() -> SparkSession:
    """Factory function to create configured SparkSession.

    Automatically detects platform and applies appropriate configuration.
    On Windows, disables Arrow and sets HADOOP_HOME fallback.

    Returns:
        SparkSession configured with settings from config
    """
    if sys.platform == "win32":
        os.environ.setdefault("HADOOP_HOME", "C:\\hadoop")

        temp_dir = Path(tempfile.gettempdir()) / "spark-temp"
        temp_dir.mkdir(exist_ok=True)
        os.environ["SPARK_LOCAL_DIRS"] = str(temp_dir)

        logger.warning("Windows: using local configuration without Hadoop")

    builder = SparkSession.builder.appName("MobileTrackingETL")

    for key, value in SPARK_CONFIG.items():
        builder = builder.config(key, value)

    builder = builder.config("spark.driver.memory", "4g")
    builder = builder.config("spark.sql.adaptive.enabled", "true")

    if sys.platform == "win32":
        builder = (
            builder.config(
                "spark.sql.execution.arrow.pyspark.enabled", "false"
            )
            .config(
                "spark.driver.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties",
            )
            .config(
                "spark.executor.extraJavaOptions",
                "-Dlog4j.configuration=file:log4j.properties",
            )
            .config("spark.ui.showConsoleProgress", "false")
        )

    spark = builder.getOrCreate()

    if sys.platform == "win32":
        spark.sparkContext.setLogLevel("ERROR")

        try:
            log4j = spark._jvm.org.apache.log4j
            log4j.LogManager.getLogger(
                "org.apache.spark.util.ShutdownHookManager"
            ).setLevel(log4j.Level.OFF)
        except:
            pass

    logger.info(
        f"Spark session created: {spark.sparkContext.appName}, "
        f"version {spark.version}"
    )

    return spark
