"""
Verify what case tables actually exist in the database after ETL.
"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def verify_table_case():
    """Check actual table names in database."""
    NEON_URL = os.getenv("NEON_DATABASE_URL")
    if not NEON_URL:
        raise ValueError("NEON_DATABASE_URL not found in environment")
    
    conn = psycopg2.connect(NEON_URL)
    cur = conn.cursor()
    
    try:
        print("=== TABLAS REALES EN BASE DE DATOS ===")
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        for table in tables:
            print(f"  '{table}'")
        
        # Test specific tables
        print(f"\n=== VERIFICANDO TABLAS ESPECÍFICAS ===")
        test_tables = ['DIM_OPERADOR', 'dim_operador', 'DIM_ZONAS', 'dim_zonas']
        
        for table in test_tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"  ✅ {table}: {count:,} registros")
            except Exception as e:
                print(f"  ❌ {table}: ERROR - {str(e).split('LINE')[0].strip()}")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    verify_table_case()
