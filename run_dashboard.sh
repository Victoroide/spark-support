#!/bin/bash
# Dashboard Execution Script - Advanced Mobile Tracking Dashboard
# Stack: Dash + Plotly + Folium + psycopg2

set -e

echo "============================================================"
echo "INICIANDO DASHBOARD AVANZADO"
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

# Validate dependencies
echo ""
echo "Validando dependencias..."
python -c "
import dash
import dash_bootstrap_components
import plotly
import folium
import psycopg2
import pandas
print('Dash:', dash.__version__)
print('Plotly:', plotly.__version__)
print('Folium:', folium.__version__)
print('psycopg2:', psycopg2.__version__)
print('Pandas:', pandas.__version__)
print('Todas las dependencias OK')
" || {
    echo "ERROR: Faltan dependencias"
    echo "Ejecuta: pip install -r requirements.txt"
    exit 1
}

# Validate database connection
echo ""
echo "Validando conexion a PostgreSQL..."
python -c "
import psycopg2
import os
from dotenv import load_dotenv
load_dotenv()
url = os.getenv('NEON_DATABASE_URL')
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM FACT_MEDICIONES')
total = cur.fetchone()[0]
conn.close()
print(f'Conexion OK: {total:,} registros en FACT_MEDICIONES')
" || {
    echo "ERROR: No se pudo conectar a la base de datos"
    exit 1
}

# Check if port is available
PORT=8050
if command -v netstat &> /dev/null; then
    if netstat -tuln 2>/dev/null | grep -q ":$PORT "; then
        echo ""
        echo "WARNING: Puerto $PORT ya esta en uso"
        echo "Deteniendo proceso existente..."
        # Try to find and kill existing process
        PID=$(netstat -tulnp 2>/dev/null | grep ":$PORT " | awk '{print $7}' | cut -d'/' -f1)
        if [ -n "$PID" ]; then
            kill $PID 2>/dev/null || true
            sleep 2
        fi
    fi
fi

echo ""
echo "============================================================"
echo "Iniciando dashboard en http://127.0.0.1:$PORT"
echo "Presiona Ctrl+C para detener el servidor"
echo "============================================================"
echo ""

# Run dashboard
python dashboard_advanced.py
