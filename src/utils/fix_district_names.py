"""
Fix district names in DIM_ZONAS table to use proper names instead of internal IDs.
Run this script to update existing data after fixing distrito_loader.py
"""

import psycopg2
import os
from dotenv import load_dotenv
from distrito_loader import get_distrito_manager

load_dotenv()

def fix_district_names():
    """Update DIM_ZONAS table with proper district names."""
    NEON_URL = os.getenv("NEON_DATABASE_URL")
    if not NEON_URL:
        raise ValueError("NEON_DATABASE_URL not found in environment")
    
    conn = psycopg2.connect(NEON_URL)
    cur = conn.cursor()
    
    try:
        # Get current zones
        cur.execute("SELECT zona_id, nombre FROM DIM_ZONAS ORDER BY zona_id")
        current_zones = cur.fetchall()
        
        print("Current zones in database:")
        for zona_id, nombre in current_zones:
            print(f"  ID {zona_id}: {nombre}")
        
        # Get proper names from distrito manager
        distrito_mgr = get_distrito_manager()
        all_distritos = distrito_mgr.get_all_distritos()
        
        # Create mapping from zone_id to proper name
        name_mapping = {}
        for distrito in all_distritos:
            # The ETL uses the index+1 as zona_id
            zona_id = list(all_distritos).index(distrito) + 1
            name_mapping[zona_id] = distrito['nombre']
        
        print("\nUpdating zones with proper names:")
        for zona_id, proper_name in name_mapping.items():
            cur.execute(
                "UPDATE DIM_ZONAS SET nombre = %s WHERE zona_id = %s",
                (proper_name, zona_id)
            )
            print(f"  Updated ID {zona_id}: {proper_name}")
        
        conn.commit()
        print(f"\nUpdated {len(name_mapping)} district names successfully!")
        
        # Verify the updates
        cur.execute("SELECT zona_id, nombre FROM DIM_ZONAS ORDER BY zona_id")
        updated_zones = cur.fetchall()
        print("\nUpdated zones in database:")
        for zona_id, nombre in updated_zones:
            print(f"  ID {zona_id}: {nombre}")
            
    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    fix_district_names()
