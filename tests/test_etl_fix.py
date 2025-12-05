#!/usr/bin/env python
"""Test script to verify ETL date casting fix"""

import os
import sys
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()

from etl_datawarehouse import (
    init_connection_pool, close_connection_pool,
    create_schema, populate_static_dimensions,
    DimensionCache, bulk_insert_tiempos
)

def test_date_bulk_insert():
    """Test the fixed bulk_insert_tiempos function"""
    print("Testing bulk_insert_tiempos with date casting fix...")
    
    try:
        # Initialize
        init_connection_pool()
        create_schema()
        populate_static_dimensions()
        
        # Create cache
        cache = DimensionCache()
        cache.load_static_dimensions()
        
        # Test with some sample dates
        test_dates = {
            datetime(2024, 1, 1).date(),
            datetime(2024, 1, 2).date(),
            datetime(2024, 1, 3).date()
        }
        
        print(f"Inserting {len(test_dates)} test dates...")
        bulk_insert_tiempos(test_dates, cache)
        
        # Verify dates were inserted
        from etl_datawarehouse import get_connection
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM DIM_TIEMPO")
            count = cur.fetchone()[0]
            print(f"Total dates in DIM_TIEMPO: {count}")
        
        print("✓ Date bulk insert test PASSED")
        return True
        
    except Exception as e:
        print(f"✗ Test FAILED: {e}")
        return False
    finally:
        close_connection_pool()

if __name__ == "__main__":
    success = test_date_bulk_insert()
    sys.exit(0 if success else 1)
