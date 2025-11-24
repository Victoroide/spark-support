# Architecture Documentation

## Overview

This document describes the architectural decisions, design patterns, and principles applied to the Mobile Tracking ETL pipeline.

## Design Philosophy

### SOLID Principles

#### Single Responsibility Principle (SRP)
Each module has exactly one reason to change:

- **coordinate_cleaner**: Only changes if coordinate validation rules change
- **device_analyzer**: Only changes if device metrics requirements change
- **result_writer**: Only changes if output format requirements change

**Example:**
```python
# GOOD: Single responsibility
def clean_coordinates(df: DataFrame) -> DataFrame:
    """Only validates and filters coordinates."""
    return df.filter(valid_coordinate_condition)

# BAD: Multiple responsibilities
def clean_and_analyze_coordinates(df: DataFrame) -> Dict:
    """Validates coordinates AND calculates statistics."""
    df_clean = df.filter(valid_coordinate_condition)
    stats = df_clean.groupBy("device_id").count()  # Wrong!
    return {"clean_df": df_clean, "stats": stats}
```

#### Open/Closed Principle (OCP)
System is open for extension, closed for modification:

**Adding a new analyzer doesn't require modifying existing code:**
```python
# New file: src/analyzers/signal_analyzer.py
def analyze_signal_quality(df: DataFrame) -> DataFrame:
    """New analysis without touching other analyzers."""
    return df.groupBy("device_id").agg(avg("signal"))

# In main.py: Just add one line
signal_results = analyze_signal_quality(df_transformed)
```

#### Liskov Substitution Principle (LSP)
Functions accept base types (DataFrame) and work with any valid input:

```python
def clean_coordinates(df: DataFrame) -> DataFrame:
    """Works with any DataFrame that has lat/lon columns."""
    # Implementation doesn't care about DataFrame source
```

#### Interface Segregation Principle (ISP)
Modules expose only what clients need:

```python
# Public API (what clients see)
def analyze_device_metrics(df: DataFrame) -> DataFrame:
    """Public interface - documented and stable."""
    return _aggregate_by_device(df)

# Private implementation (internal detail)
def _aggregate_by_device(df: DataFrame) -> DataFrame:
    """Internal helper - can change without breaking clients."""
    return df.groupBy("device_id").agg(...)
```

#### Dependency Inversion Principle (DIP)
High-level modules (main) don't depend on low-level modules (loaders):

```python
# main.py depends on abstract interface, not implementation
from src.loaders.csv_loader import load_location_data

# Could swap to Parquet loader without changing main.py
from src.loaders.parquet_loader import load_location_data
```

## Module Architecture

### Dependency Graph

```
┌─────────────────────────────────────────────────┐
│                    main.py                      │
│            (Pipeline Orchestration)              │
└──────────────────┬──────────────────────────────┘
                   │
      ┌────────────┼────────────┐
      │            │            │
      ▼            ▼            ▼
┌──────────┐ ┌──────────┐ ┌──────────┐
│exporters │ │analyzers │ │transformers│
└──────────┘ └────┬─────┘ └─────┬────┘
                  │              │
                  └──────┬───────┘
                         ▼
                   ┌──────────┐
                   │ cleaners │
                   └─────┬────┘
                         ▼
                   ┌──────────┐
                   │ loaders  │
                   └─────┬────┘
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
      ┌──────────┐             ┌──────────┐
      │  config  │             │  utils   │
      └──────────┘             └──────────┘
```

### Layer Responsibilities

#### Layer 1: Configuration & Utilities (Bottom)
- **config/settings.py**: Application constants and configuration
- **utils/spark_session.py**: SparkSession factory
- **utils/logger.py**: Logging setup

**Design Decision:** Configuration is at the bottom because everything
depends on it, but it depends on nothing.

#### Layer 2: Data Loading
- **loaders/csv_loader.py**: CSV ingestion and schema validation

**Design Decision:** Loaders only concern themselves with getting data
into DataFrames, not what happens to it.

#### Layer 3: Data Cleaning
- **cleaners/coordinate_cleaner.py**: Geographic validation
- **cleaners/timestamp_cleaner.py**: Temporal validation
- **cleaners/null_handler.py**: Null value handling

**Design Decision:** Each cleaner is independent and composable.
They can be applied in any order or skipped entirely.

#### Layer 4: Data Transformation
- **transformers/temporal_features.py**: Time-based feature derivation
- **transformers/geospatial_calc.py**: Geographic calculations

**Design Decision:** Transformers add columns but never remove data.
They're pure functions that return new DataFrames.

#### Layer 5: Business Logic & Analysis
- **analyzers/device_analyzer.py**: Device-level aggregations
- **analyzers/network_analyzer.py**: Network distribution analysis
- **analyzers/temporal_analyzer.py**: Temporal pattern analysis
- **analyzers/anomaly_detector.py**: Anomaly detection

**Design Decision:** Analyzers contain business logic. They're where
domain knowledge lives. Each analyzer is independently testable.

#### Layer 6: Data Export
- **exporters/result_writer.py**: Result persistence

**Design Decision:** Export logic is separated so we can easily swap
CSV for Parquet, Delta, or database writes.

#### Layer 7: Orchestration (Top)
- **main.py**: Pipeline execution flow

**Design Decision:** Main has NO business logic, only orchestration.
It's a declarative definition of the pipeline flow.

## Key Design Patterns

### Factory Pattern
```python
# utils/spark_session.py
def create_spark_session() -> SparkSession:
    """Factory method for SparkSession creation."""
    # Centralizes configuration logic
```

**Why:** SparkSession creation is complex. Factory pattern encapsulates
this complexity and provides a single point of configuration.

### Strategy Pattern (Implicit)
```python
# Cleaners are interchangeable strategies
df = clean_coordinates(df)
df = clean_timestamps(df)
# Can swap order or add new cleaners
```

**Why:** Different cleaning strategies can be applied without changing
the core pipeline logic.

### Pipeline Pattern
```python
# main.py orchestrates a data pipeline
df → load → clean → transform → analyze → export
```

**Why:** Data flows through stages in a clear, linear fashion.
Each stage is testable in isolation.

## Data Flow

### Immutability Principle
All transformations return NEW DataFrames:

```python
# GOOD: Immutable transformation
def clean_coordinates(df: DataFrame) -> DataFrame:
    return df.filter(...)  # Returns new DataFrame

# BAD: Mutable transformation
def clean_coordinates(df: DataFrame) -> None:
    df = df.filter(...)  # Modifies input (doesn't work in Python anyway)
```

**Why:** Immutability enables:
- Safe parallel execution
- Easy testing (no side effects)
- Clear data lineage
- Spark's lazy evaluation optimization

### Lazy Evaluation
Transformations are lazy, actions are eager:

```python
# These are lazy (no computation yet)
df_clean = clean_coordinates(df)
df_transformed = add_temporal_features(df_clean)

# These trigger computation (actions)
df_clean.count()
df_transformed.write.csv(...)
```

**Why:** Spark can optimize the entire pipeline before executing.

## Error Handling Strategy

### Fail Fast Principle
```python
# loaders/csv_loader.py
if not file_path.exists():
    raise FileNotFoundError(f"CSV file not found: {file_path}")
```

**Why:** Catch errors early, before expensive Spark operations.

### Exception Hierarchy
```python
class SchemaValidationError(Exception):
    """Specific error for schema issues."""
    pass
```

**Why:** Callers can handle specific error types differently.

### Top-Level Error Handler
```python
# main.py
try:
    # Run pipeline
except Exception as e:
    logger.error(f"Pipeline failed: {e}", exc_info=True)
    return 1
finally:
    spark.stop()  # Always cleanup
```

**Why:** Ensures resources are released even on failure.

## Logging Strategy

### Structured Logging
```python
logger.info(f"Loaded {count:,} records from {path}")
logger.warning(f"Discarded {rate:.2f}% due to invalid coordinates")
logger.error(f"Pipeline failed: {error}", exc_info=True)
```

**Why:** 
- **INFO**: Normal operation flow
- **WARNING**: Suspicious but recoverable situations
- **ERROR**: Failures requiring attention

### Module-Level Loggers
```python
logger = setup_logger(__name__)
```

**Why:** Each module's logs are identifiable by name.

## Testing Strategy

### Unit Testing
Each module is tested in isolation:

```python
def test_clean_coordinates_removes_invalid(spark):
    df = create_test_df_with_invalid_coords()
    result = clean_coordinates(df)
    assert result.count() < df.count()
```

**Why:** Fast tests that don't require full pipeline execution.

### Fixture Pattern
```python
@pytest.fixture(scope="module")
def spark():
    """Shared Spark session for tests."""
    session = SparkSession.builder.master("local[2]").getOrCreate()
    yield session
    session.stop()
```

**Why:** Reuse expensive Spark session across tests.

### Dependency Injection for Testability
```python
# Functions receive dependencies as parameters
def load_location_data(spark: SparkSession, file_path: Path):
    # Can pass mock spark or test file_path
```

**Why:** Easy to test with mocks without complex setup.

## Performance Considerations

### Pushdown Predicates
Filters are applied early:

```python
# GOOD: Filter early
df.filter(valid_coords).groupBy("device_id").agg(...)

# BAD: Filter late
df.groupBy("device_id").agg(...).filter(valid_coords)
```

### Partition Management
```python
# Configure partitions based on data size
"spark.sql.shuffle.partitions": "200"
```

### Coalescing for Small Outputs
```python
df.coalesce(1).write.csv(...)  # Single file for small results
```

### Window Functions
```python
# Efficient for per-device calculations
Window.partitionBy("device_id").orderBy("timestamp")
```

## Extension Points

### Adding New Cleaner
1. Create `src/cleaners/new_cleaner.py`
2. Implement `clean_X(df: DataFrame) -> DataFrame`
3. Add to pipeline in `main.py`
4. Add tests in `tests/test_cleaners.py`

### Adding New Analyzer
1. Create `src/analyzers/new_analyzer.py`
2. Implement `analyze_X(df: DataFrame) -> DataFrame`
3. Add to pipeline in `main.py`
4. Add export logic if needed
5. Add tests in `tests/test_analyzers.py`

### Changing Output Format
1. Modify `src/exporters/result_writer.py`
2. No changes needed in other modules

### Swapping Data Source
1. Create new loader (e.g., `parquet_loader.py`)
2. Update import in `main.py`
3. No changes needed in other modules

## Code Quality Tools

### Black (Formatter)
- Line length: 88 characters
- Automatic formatting
- Consistent style across codebase

### isort (Import Sorter)
- stdlib imports first
- Third-party imports second
- Local imports last

### pylint (Linter)
- Enforces coding standards
- Catches potential bugs
- Target score: >9.0

### mypy (Type Checker)
- Validates type hints
- Catches type-related bugs
- Ensures API contracts

## Platform Compatibility

### Windows

- Automatic detection via `sys.platform`
- Export using `pandas.to_csv()` instead of `df.write.csv()`
- Warnings suppressed in Spark shutdown
- HADOOP_HOME set to fallback value if not configured

```python
# src/exporters/result_writer.py
if sys.platform == "win32":
    logger.warning("Windows detected: using Pandas export")
    pdf = df.toPandas()
    pdf.to_csv(str(csv_path), index=False)
```

### Linux/macOS

- Native Spark export operations
- Standard filesystem operations
- No platform-specific handling required

```python
# src/exporters/result_writer.py
else:
    writer.write.mode("overwrite").csv(path_str, header=True)
```

## Best Practices Summary

✅ **DO:**
- Keep functions pure (no side effects)
- Use type hints everywhere
- Log at appropriate levels
- Write tests for new functionality
- Use constants from config
- Return new DataFrames, don't modify inputs
- Handle errors explicitly
- Document public APIs with docstrings
- Normalize paths for cross-platform compatibility
- Provide fallbacks for platform-specific features

❌ **DON'T:**
- Mix concerns (analysis + I/O in one function)
- Use print statements (use logger)
- Create circular dependencies
- Hardcode magic numbers
- Use global mutable state
- Skip type hints
- Ignore test coverage
- Assume Unix-only environment
- Use platform-specific paths without normalization

## Conclusion

This architecture prioritizes:
1. **Maintainability**: Clear module boundaries and responsibilities
2. **Testability**: Independent, mockable components
3. **Extensibility**: Easy to add new functionality
4. **Reliability**: Explicit error handling and logging
5. **Performance**: Optimized Spark operations

The modular design ensures that changes are localized, tests are fast,
and the system remains comprehensible as it grows.
