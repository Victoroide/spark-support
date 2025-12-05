#!/bin/bash
# ETL Execution Script - Optimized DataWarehouse Load
# Target: <5 minutes for 373k records

set -e

echo "============================================================"
echo "INICIANDO ETL DATAWAREHOUSE"
echo "Fecha: $(date)"
echo "============================================================"

# Determine script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtual environment
if [ -f ".venv/Scripts/activate" ]; then
    echo "Activando virtual environment (Windows)..."
    source .venv/Scripts/activate
elif [ -f ".venv/bin/activate" ]; then
    echo "Activando virtual environment (Linux/Mac)..."
    source .venv/bin/activate
else
    echo "ERROR: Virtual environment no encontrado en .venv/"
    echo "Ejecuta: python -m venv .venv && pip install -r requirements.txt"
    exit 1
fi

# Validate Python environment
echo ""
echo "[1/4] Validando entorno Python..."
python --version
pip show psycopg2-binary > /dev/null 2>&1 || {
    echo "ERROR: psycopg2-binary no instalado"
    echo "Ejecuta: pip install -r requirements.txt"
    exit 1
}

# Validate Supabase connection
echo ""
echo "[2/4] Validando conexion a Supabase..."
python -c "
from supabase import create_client
from src.config.settings import SUPABASE_URL, SUPABASE_KEY
print(f'DEBUG - URL length: {len(SUPABASE_URL) if SUPABASE_URL else 0}')
print(f'DEBUG - Key length: {len(SUPABASE_KEY) if SUPABASE_KEY else 0}')
if SUPABASE_KEY:
    print(f'DEBUG - Key starts: {repr(SUPABASE_KEY[:15])}')
    print(f'DEBUG - Key ends: {repr(SUPABASE_KEY[-15:])}')
if not SUPABASE_URL or not SUPABASE_KEY:
    print('ERROR: SUPABASE_URL o SUPABASE_KEY no definidos en settings.py')
    exit(1)
try:
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    result = client.table('locations').select('id', count='exact').limit(1).execute()
    print(f'Conexion OK: {result.count:,} registros disponibles')
except Exception as e:
    print(f'ERROR: {e}')
    exit(1)
" || {
    echo "ERROR: No se pudo conectar a Supabase"
    exit 1
}

# Validate PostgreSQL connection
echo ""
echo "[3/4] Validando conexion a PostgreSQL (Neon)..."
python -c "
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
url = os.getenv('NEON_DATABASE_URL')
if not url:
    print('ERROR: NEON_DATABASE_URL no definido en .env')
    exit(1)
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute('SELECT 1')
conn.close()
print('Conexion OK')
" || {
    echo "ERROR: No se pudo conectar a PostgreSQL"
    exit 1
}

# Execute ETL with timing
echo ""
echo "[4/4] Ejecutando ETL optimizado..."
echo "============================================================"

START_TIME=$(date +%s)

# Pass arguments to ETL script (--full for full reload)
python etl_datawarehouse.py "$@"

END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))
MINUTES=$((ELAPSED / 60))
SECONDS=$((ELAPSED % 60))

echo ""
echo "============================================================"
echo "ETL COMPLETADO"
echo "============================================================"
echo "Tiempo total: ${MINUTES}m ${SECONDS}s (${ELAPSED} segundos)"
echo ""

# Performance check
if [ $ELAPSED -gt 300 ]; then
    echo "WARNING: ETL tardo mas de 5 minutos ($ELAPSED seg)"
    echo "Revisar logs para identificar cuellos de botella"
    exit 1
fi

echo "SUCCESS: ETL completado en menos de 5 minutos"
echo ""

# Final validation
echo "Validacion final..."
python -c "
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(os.getenv('NEON_DATABASE_URL'))
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM FACT_MEDICIONES')
total = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM DIM_UBICACION')
ubicaciones = cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM DIM_DISPOSITIVO')
dispositivos = cur.fetchone()[0]
conn.close()
print(f'FACT_MEDICIONES: {total:,} registros')
print(f'DIM_UBICACION: {ubicaciones:,} registros')
print(f'DIM_DISPOSITIVO: {dispositivos:,} registros')
"

echo ""
echo "ETL finalizado exitosamente."
