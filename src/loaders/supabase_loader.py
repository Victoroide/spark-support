"""Supabase data loader for location tracking data."""

from typing import List, Dict, Any, Optional
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    TimestampType,
)

from src.config.settings import EXPECTED_COLUMNS, SUPABASE_URL, SUPABASE_KEY
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


class SupabaseConnectionError(Exception):
    """Raised when Supabase connection fails."""
    pass


class SupabaseDataError(Exception):
    """Raised when data retrieval from Supabase fails."""
    pass


def _get_supabase_client():
    """Create and return Supabase client.
    
    Returns:
        Supabase client instance
        
    Raises:
        SupabaseConnectionError: If connection fails
    """
    try:
        from supabase import create_client, Client
        client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        logger.info("Successfully connected to Supabase")
        return client
    except ImportError:
        raise SupabaseConnectionError(
            "supabase package not installed. Run: pip install supabase"
        )
    except Exception as e:
        raise SupabaseConnectionError(f"Failed to connect to Supabase: {str(e)}")


def _fetch_data_paginated(
    client,
    table_name: str,
    batch_size: int = 1000,
    max_records: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Fetch data from Supabase table with pagination.
    
    Args:
        client: Supabase client instance
        table_name: Name of the table to fetch
        batch_size: Number of records per batch
        max_records: Maximum records to fetch (None for all)
        
    Returns:
        List of records as dictionaries
    """
    all_records = []
    offset = 0
    
    logger.info(f"Starting paginated fetch from table '{table_name}'")
    
    while True:
        try:
            query = client.table(table_name).select('*').range(offset, offset + batch_size - 1)
            response = query.execute()
            
            if not response.data:
                logger.info(f"No more data at offset {offset}")
                break
                
            batch_records = response.data
            all_records.extend(batch_records)
            
            logger.info(f"Fetched {len(batch_records)} records (total: {len(all_records)})")
            
            if len(batch_records) < batch_size:
                break
                
            if max_records and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                break
                
            offset += batch_size
            
        except Exception as e:
            raise SupabaseDataError(f"Error fetching data at offset {offset}: {str(e)}")
    
    logger.info(f"Total records fetched: {len(all_records)}")
    return all_records


def _get_locations_schema() -> StructType:
    """Get the schema for locations table.
    
    Returns:
        StructType schema for locations data
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("device_name", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("altitude", DoubleType(), True),
        StructField("speed", DoubleType(), True),
        StructField("battery", IntegerType(), True),
        StructField("signal", IntegerType(), True),
        StructField("sim_operator", StringType(), True),
        StructField("network_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("device_id", StringType(), True),
    ])


def load_location_data_from_supabase(
    spark: SparkSession,
    table_name: str = "locations",
    batch_size: int = 1000,
    max_records: Optional[int] = None
) -> DataFrame:
    """Load location tracking data from Supabase.
    
    Args:
        spark: Active SparkSession instance
        table_name: Supabase table name
        batch_size: Records per fetch batch
        max_records: Max records to fetch (None for all)
        
    Returns:
        DataFrame with location data
        
    Raises:
        SupabaseConnectionError: If connection fails
        SupabaseDataError: If data retrieval fails
    """
    logger.info(f"Loading location data from Supabase table '{table_name}'")
    
    # Connect to Supabase
    client = _get_supabase_client()
    
    # Fetch data with pagination
    records = _fetch_data_paginated(
        client, 
        table_name, 
        batch_size=batch_size,
        max_records=max_records
    )
    
    if not records:
        raise SupabaseDataError(f"No data found in table '{table_name}'")
    
    # Convert to pandas DataFrame first
    logger.info("Converting to Pandas DataFrame")
    pdf = pd.DataFrame(records)
    
    # Convert timestamp strings to proper format
    if 'timestamp' in pdf.columns:
        pdf['timestamp'] = pd.to_datetime(pdf['timestamp'])
    
    # Ensure numeric columns are correct type
    numeric_columns = ['latitude', 'longitude', 'altitude', 'speed']
    for col in numeric_columns:
        if col in pdf.columns:
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce')
    
    int_columns = ['id', 'battery', 'signal']
    for col in int_columns:
        if col in pdf.columns:
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce').astype('Int64')
    
    # Convert to Spark DataFrame
    logger.info("Converting to Spark DataFrame")
    df = spark.createDataFrame(pdf)
    
    initial_count = df.count()
    logger.info(f"Loaded {initial_count:,} records from Supabase")
    
    # Validate schema
    _validate_schema(df)
    
    return df


def _validate_schema(df: DataFrame) -> None:
    """Validate DataFrame schema against expected columns.
    
    Args:
        df: DataFrame to validate
        
    Raises:
        SupabaseDataError: If required columns are missing
    """
    actual_columns = set(df.columns)
    expected_columns = set(EXPECTED_COLUMNS)
    
    missing_columns = expected_columns - actual_columns
    extra_columns = actual_columns - expected_columns
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        raise SupabaseDataError(
            f"Missing columns: {', '.join(sorted(missing_columns))}"
        )
    
    if extra_columns:
        logger.warning(f"Extra columns found (will be ignored): {extra_columns}")
    
    logger.info("Schema validation passed")


def get_table_info(table_name: str = "locations") -> Dict[str, Any]:
    """Get information about a Supabase table.
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with table info (columns, record count, sample)
    """
    client = _get_supabase_client()
    
    try:
        # Get sample record
        sample_response = client.table(table_name).select('*').limit(1).execute()
        
        # Get count
        count_response = client.table(table_name).select('count', count='exact').execute()
        
        info = {
            "table_name": table_name,
            "columns": list(sample_response.data[0].keys()) if sample_response.data else [],
            "column_count": len(sample_response.data[0].keys()) if sample_response.data else 0,
            "total_records": count_response.count if hasattr(count_response, 'count') else "unknown",
            "sample_record": sample_response.data[0] if sample_response.data else None
        }
        
        return info
        
    except Exception as e:
        raise SupabaseDataError(f"Error getting table info: {str(e)}")
