from pathlib import Path
from typing import Dict, Tuple
import os


LATITUDE_RANGE: Tuple[float, float] = (-90.0, 90.0)
LONGITUDE_RANGE: Tuple[float, float] = (-180.0, 180.0)
SPEED_THRESHOLD: float = 200.0
BATTERY_CRITICAL: float = 10.0
GEOGRAPHIC_JUMP_THRESHOLD: float = 500.0

# Supabase Configuration
SUPABASE_URL: str = os.getenv("SUPABASE_URL")
SUPABASE_KEY: str = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE: str = os.getenv("SUPABASE_TABLE")

# Neon PostgreSQL DataWarehouse
NEON_DATABASE_URL: str = os.getenv("NEON_DATABASE_URL")

# Data source selection: "csv" or "supabase"
DATA_SOURCE: str = "supabase"

# Supabase fetch settings
SUPABASE_BATCH_SIZE: int = 10000
SUPABASE_MAX_RECORDS: int = None  # None for all records

BASE_DIR: Path = Path(__file__).parent.parent.parent.resolve()
INPUT_CSV_PATH: Path = BASE_DIR / "locations_rows.csv"
OUTPUT_BASE_DIR: Path = BASE_DIR / "output"
OUTPUT_BASE_DIR.mkdir(exist_ok=True)

DEVICE_STATS_PATH: Path = OUTPUT_BASE_DIR / "device_stats"
NETWORK_ANALYSIS_PATH: Path = OUTPUT_BASE_DIR / "network_analysis"
TEMPORAL_PATTERNS_PATH: Path = OUTPUT_BASE_DIR / "temporal_patterns"
ANOMALIES_PATH: Path = OUTPUT_BASE_DIR / "anomalies"

SPARK_CONFIG: Dict[str, str] = {
    "spark.app.name": "MobileTrackingETL",
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
}

EXPECTED_COLUMNS: Tuple[str, ...] = (
    "id",
    "device_name",
    "latitude",
    "longitude",
    "altitude",
    "speed",
    "battery",
    "signal",
    "sim_operator",
    "network_type",
    "timestamp",
    "device_id",
)

LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL: str = "INFO"
