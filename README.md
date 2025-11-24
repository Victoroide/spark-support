# Mobile Tracking ETL - Modular Architecture

Professional PySpark ETL pipeline with modular architecture following SOLID principles for mobile device tracking data analysis.

## Architecture Overview

This project follows a strict separation of concerns with modules organized by responsibility:

```
src/
├── config/          # Configuration and constants
├── utils/           # Cross-cutting utilities (Spark, logging)
├── loaders/         # Data ingestion and schema validation
├── cleaners/        # Data quality and cleaning operations
├── transformers/    # Feature engineering and derivations
├── analyzers/       # Business logic and aggregations
├── exporters/       # Result persistence
└── main.py          # Pipeline orchestration
```

## Design Principles

### Single Responsibility Principle
Each module has one clear, testable responsibility:
- `coordinate_cleaner`: Only validates geographic coordinates
- `device_analyzer`: Only aggregates device metrics
- `result_writer`: Only handles data export

### Dependency Injection
Functions receive dependencies as parameters, enabling:
- Independent testing with mocks
- Reusability across different contexts
- Clear dependency graphs

### Interface Segregation
Modules expose minimal public APIs:
- Public functions: Clear, documented interfaces
- Private functions: Prefixed with `_` for internal use

### No Circular Dependencies
Clear import hierarchy:
```
main → exporters → analyzers → transformers → cleaners → loaders
                                    ↓
                                 config, utils
```

## Installation

```bash
pip install -r requirements.txt
```

## Windows Setup

### Quick Start

```bash
pip install -r requirements.txt
python -m src.main
```

The system automatically detects Windows and uses Pandas for CSV exports.

### Notes

- On Windows, export is slower (uses Pandas instead of native Spark)
- For better performance, install Hadoop/winutils.exe (optional)
- Development mode works without any additional setup

## Usage

### Run Complete Pipeline

```bash
python -m src.main
```

### Run from Project Root

```bash
cd /path/to/spark-support
python -m src.main
```

### Import Modules Independently

```python
from src.loaders.csv_loader import load_location_data
from src.cleaners.coordinate_cleaner import clean_coordinates
from src.utils.spark_session import create_spark_session

spark = create_spark_session()
df = load_location_data(spark, Path("data.csv"))
df_clean = clean_coordinates(df)
```

## Module Reference

### config/settings.py
Centralized configuration management:
- **LATITUDE_RANGE**, **LONGITUDE_RANGE**: Geographic validation bounds
- **SPEED_THRESHOLD**: Maximum realistic speed (200 km/h)
- **BATTERY_CRITICAL**: Critical battery threshold (10%)
- **SPARK_CONFIG**: Spark session configuration dictionary
- **Path constants**: All input/output paths as Path objects

### utils/spark_session.py
**Function:** `create_spark_session() -> SparkSession`
- Factory for configured SparkSession
- Applies settings from config
- Logs session details

### utils/logger.py
**Function:** `setup_logger(name: str) -> logging.Logger`
- Configures standardized logging
- Consistent format across modules
- Appropriate log levels per environment

### loaders/csv_loader.py
**Function:** `load_location_data(spark, file_path) -> DataFrame`
- Loads CSV with schema inference
- Validates against expected schema
- Raises `SchemaValidationError` for missing columns
- Logs warnings for extra columns

### cleaners/coordinate_cleaner.py
**Function:** `clean_coordinates(df) -> DataFrame`
- Removes records with null lat/lon
- Filters coordinates outside valid ranges
- Logs discard metrics and rates

### cleaners/timestamp_cleaner.py
**Function:** `clean_timestamps(df) -> DataFrame`
- Removes records with null timestamps
- Logs cleaning statistics

### cleaners/null_handler.py
**Function:** `remove_null_critical_fields(df, fields) -> DataFrame`
- Removes records with nulls in specified fields
- Configurable critical field list
- Detailed logging per field

### transformers/temporal_features.py
**Function:** `add_temporal_features(df) -> DataFrame`
- Adds: `date`, `hour`, `day_of_week`, `weekday_name`
- Pure transformation, no side effects
- Input DataFrame unchanged

### transformers/geospatial_calc.py
**Function:** `calculate_haversine_distance(...) -> Column`
- Haversine formula for great-circle distance
- Returns Spark Column expression

**Function:** `add_distance_from_previous(df) -> DataFrame`
- Adds distance from previous location per device
- Calculates time difference between readings
- Uses window functions for efficiency

### analyzers/device_analyzer.py
**Function:** `analyze_device_metrics(df) -> DataFrame`
- Per-device aggregations
- Output columns: `total_records`, `avg_battery`, `min_battery`,
  `avg_signal`, `avg_speed`, `max_speed`, `first_activity`,
  `last_activity`, `activity_window_hours`

### analyzers/network_analyzer.py
**Function:** `analyze_network_distribution(df) -> DataFrame`
- Groups by `sim_operator` and `network_type`
- Calculates counts and percentages
- Sorted by record count descending

### analyzers/temporal_analyzer.py
**Function:** `analyze_temporal_patterns(df) -> DataFrame`
- Hourly distribution (0-23)
- Weekday distribution (1-7)
- Unified format: `pattern_type`, `dimension`, `record_count`

### analyzers/anomaly_detector.py
**Function:** `detect_speed_anomalies(df) -> DataFrame`
- Detects speeds > 200 km/h

**Function:** `detect_battery_anomalies(df) -> DataFrame`
- Detects battery < 10%

**Function:** `detect_geographic_anomalies(df) -> DataFrame`
- Detects jumps > 500 km between consecutive readings

**Function:** `detect_all_anomalies(df) -> DataFrame`
- Combines all anomaly types
- Returns unified DataFrame with `anomaly_type` column

### exporters/result_writer.py
**Function:** `write_partitioned(df, path, partition_cols, mode, coalesce)`
- Generic CSV writer with partitioning support
- Configurable write mode
- Optional coalescing for single files
- Logs record counts and timing

**Specialized functions:**
- `write_device_stats(df, path)`: Single coalesced file
- `write_network_analysis(df, path)`: Single coalesced file
- `write_temporal_patterns(df, path)`: Single coalesced file
- `write_anomalies(df, path)`: Partitioned by anomaly_type

## Testing

### Run All Tests

```bash
pytest
```

### Run with Coverage

```bash
pytest --cov=src --cov-report=html
```

### Test Structure

```
tests/
├── test_cleaners.py      # Unit tests for cleaning modules
├── test_analyzers.py     # Unit tests for analysis modules
└── test_transformers.py  # Unit tests for transformation modules
```

Tests use pytest fixtures for Spark session management and can run
independently without full pipeline execution.

## Code Quality

### Format Code with Black

```bash
black src/ tests/
```

### Sort Imports with isort

```bash
isort src/ tests/
```

### Lint with Pylint

```bash
pylint src/
```

### Type Check with mypy

```bash
mypy src/
```

### Run All Quality Checks

```bash
black src/ tests/ && isort src/ tests/ && pylint src/ && mypy src/
```

## Configuration

Modify `src/config/settings.py` to adjust:

```python
# Geographic validation
LATITUDE_RANGE = (-90.0, 90.0)
LONGITUDE_RANGE = (-180.0, 180.0)

# Anomaly thresholds
SPEED_THRESHOLD = 200.0
BATTERY_CRITICAL = 10.0
GEOGRAPHIC_JUMP_THRESHOLD = 500.0

# Spark configuration
SPARK_CONFIG = {
    "spark.driver.memory": "4g",
    "spark.executor.memory": "4g",
    "spark.sql.shuffle.partitions": "200",
}
```

## Output Structure

```
output/
├── device_stats/
│   └── part-00000-xxx.csv
├── network_analysis/
│   └── part-00000-xxx.csv
├── temporal_patterns/
│   └── part-00000-xxx.csv
└── anomalies/
    ├── anomaly_type=impossible_speed/
    ├── anomaly_type=critical_battery/
    └── anomaly_type=geographic_jump/
```

## Adding New Analysis

To add a new analyzer without modifying existing code:

1. Create `src/analyzers/new_analyzer.py`:
```python
from pyspark.sql import DataFrame
from src.utils.logger import setup_logger

logger = setup_logger(__name__)

def analyze_new_metric(df: DataFrame) -> DataFrame:
    logger.info("Analyzing new metric")
    # Implementation
    return result_df
```

2. Add to `src/main.py`:
```python
from src.analyzers.new_analyzer import analyze_new_metric

# In main function:
new_results = analyze_new_metric(df_transformed)
```

3. Add export function if needed in `src/exporters/result_writer.py`

4. Add tests in `tests/test_analyzers.py`

## Performance Characteristics

- **Lazy Evaluation**: All transformations are lazy until actions (count, write)
- **Partitioning**: 200 shuffle partitions (configurable)
- **Coalescing**: Single file outputs for small result sets
- **Window Functions**: Efficient per-device calculations
- **Pushdown Predicates**: Filters applied early in pipeline

## Development Workflow

1. **Make changes** to specific module
2. **Run tests**: `pytest tests/test_module.py`
3. **Format code**: `black src/module.py`
4. **Sort imports**: `isort src/module.py`
5. **Check types**: `mypy src/module.py`
6. **Lint**: `pylint src/module.py`
7. **Run integration**: `python -m src.main`

## Best Practices

✅ **DO:**
- Add type hints to all function signatures
- Log at appropriate levels (INFO/WARNING/ERROR)
- Use constants from `config/settings.py`
- Write tests for new functionality
- Keep functions focused on single responsibility
- Use descriptive variable names
- Return new DataFrames, don't modify inputs

❌ **DON'T:**
- Use print statements (use logger)
- Hardcode magic numbers
- Create circular dependencies
- Mix concerns (e.g., analysis + I/O in one function)
- Use global mutable state
- Ignore type hints
- Skip tests

## Troubleshooting

### Import Errors
```
ModuleNotFoundError: No module named 'src'
```
**Solution**: Run from project root with `python -m src.main`

### Spark Configuration
```
OutOfMemoryError: Java heap space
```
**Solution**: Increase memory in `src/config/settings.py`:
```python
SPARK_CONFIG = {
    "spark.driver.memory": "8g",
    "spark.executor.memory": "8g",
}
```

### Schema Validation Errors
```
SchemaValidationError: Missing columns: ['timestamp']
```
**Solution**: Verify CSV has all expected columns defined in
`config/settings.EXPECTED_COLUMNS`

## Architecture Benefits

### Testability
- Each module independently testable
- Mock dependencies easily with fixtures
- Fast unit tests without full Spark cluster

### Maintainability
- Clear module boundaries
- Changes localized to specific modules
- Easy to understand flow from main.py

### Extensibility
- Add new analyzers without modifying existing code
- Swap implementations (e.g., different loaders)
- Configure behavior without code changes

### Reusability
- Import modules in other projects
- Compose functionality in different ways
- Build custom pipelines from existing modules

## License

Internal use only.
