"""
ETL OPTIMIZADO - Supabase a DataWarehouse PostgreSQL
Optimizado con psycopg2 nativo, execute_values y carga paralela
Target: <5 minutos para 373k registros
"""

import os
import hashlib
import time
import logging
from datetime import datetime
from typing import Optional, Dict, List, Tuple, Any
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

import numpy as np
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values, RealDictCursor
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from src.loaders.distrito_loader import get_distrito_manager

# Import credentials from settings (same method as supabase_loader.py)
from src.config.settings import SUPABASE_URL, SUPABASE_KEY, NEON_DATABASE_URL

load_dotenv()

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS
# ============================================================================

if not all([SUPABASE_URL, SUPABASE_KEY, NEON_DATABASE_URL]):
    raise ValueError("Missing environment variables. Check .env file")

# Performance tuning constants
SUPABASE_BATCH_SIZE = 50000
PG_BATCH_SIZE = 5000
PARALLEL_WORKERS = 8
CONNECTION_POOL_SIZE = 10
LOAD_WARNING_THRESHOLD_SEC = 120
TRANSFORM_WARNING_THRESHOLD_SEC = 60
WRITE_WARNING_THRESHOLD_SEC = 180

# ============================================================================
# MAPEOS DE NORMALIZACION
# ============================================================================
OPERATOR_MAP = {
    # ENTEL variants (all map to ENTEL)
    'entel': 'ENTEL', 'Entel': 'ENTEL', 'ENTEL': 'ENTEL',
    'entel s.a bolivia': 'ENTEL', 'Entel S.A Bolivia': 'ENTEL',
    'ladistancianoscuida': 'ENTEL', 'LaDistanciaNosCuida': 'ENTEL',
    'movil gsm': 'ENTEL', 'Movil GSM': 'ENTEL',
    'bomov': 'ENTEL', 'BOMOV': 'ENTEL',
    '+18vacunate': 'ENTEL', '+18VACUNATE': 'ENTEL',
    
    # TIGO variants
    'tigo': 'Tigo', 'Tigo': 'Tigo', 'comunicaciones celulares s.a.': 'Tigo',
    'comunicaciones celulares': 'Tigo',
    
    # VIVA variants
    'viva': 'Viva', 'Viva': 'Viva', 'nuevatel pcs de bolivia s.a.': 'Viva',
    'nuevatel pcs': 'Viva', 'nuevatel': 'Viva',
    
    # Signal quality operators
    'sin senal': 'Sin Senal', 'desconocido': 'Desconocido', 
    '': 'Desconocido', 'null': 'Desconocido', None: 'Desconocido'
}

# Static operator list for dimension table
OPERADORES = ['ENTEL', 'Tigo', 'Viva', 'Sin Senal', 'Desconocido']

# Network types with generation and theoretical speed
red_data = [
    ('5G', 5, '1-10 Gbps'),
    ('4G LTE', 4, '100-300 Mbps'),
    ('3G', 3, '2-10 Mbps'),
    ('2G', 2, '0.1-0.3 Mbps'),
    ('Desconocida', 0, 'N/A')
]

SANTA_CRUZ_DISTRICTS = {
    1: ("Centro", -17.7833, -63.1822, 2.0),
    2: ("Equipetrol", -17.7650, -63.1950, 2.5),
    3: ("Plan 3000", -17.8200, -63.1400, 4.0),
    4: ("Villa 1ro de Mayo", -17.7500, -63.1600, 3.0),
    5: ("Pampa de la Isla", -17.7350, -63.1450, 3.5),
    6: ("Los Lotes", -17.8400, -63.1200, 4.0),
    7: ("UV-1", -17.7900, -63.1750, 2.0),
    8: ("Urbari", -17.7700, -63.2100, 2.5),
    9: ("Las Palmas", -17.8050, -63.2000, 3.0),
    10: ("Santos Dumont", -17.7600, -63.1350, 3.0),
    11: ("Radial 10", -17.8150, -63.1600, 2.5),
    12: ("Radial 13", -17.7950, -63.2200, 2.5),
    13: ("Radial 17", -17.7550, -63.2050, 2.5),
    14: ("Radial 19", -17.7450, -63.1750, 2.5),
    15: ("Radial 26", -17.7750, -63.1500, 2.5),
    16: ("El Bajio", -17.7300, -63.1900, 3.5),
    17: ("Palmasola", -17.7100, -63.1600, 4.0),
    18: ("La Guardia", -17.8800, -63.3300, 5.0),
    19: ("Warnes", -17.5100, -63.1700, 6.0),
    20: ("Cotoca", -17.7500, -62.9900, 5.0),
}

# ============================================================================
# CLASES DE DATOS
# ============================================================================
@dataclass
class ETLStats:
    """Estadisticas del proceso ETL con timing detallado"""
    start_time: datetime
    records_processed: int = 0
    records_inserted: int = 0
    records_skipped: int = 0
    errors: List[str] = field(default_factory=list)
    phase_times: Dict[str, float] = field(default_factory=dict)
    
    @property
    def duration(self) -> float:
        return (datetime.now() - self.start_time).total_seconds()
    
    def log_phase(self, phase: str, start: float) -> float:
        elapsed = time.time() - start
        self.phase_times[phase] = elapsed
        rate = self.records_processed / elapsed if elapsed > 0 else 0
        logger.info(f"  {phase}: {elapsed:.2f}s ({rate:.0f} rec/s)")
        return elapsed

    def summary(self) -> str:
        return (f"Duration: {self.duration:.2f}s | "
                f"Processed: {self.records_processed:,} | "
                f"Inserted: {self.records_inserted:,} | "
                f"Skipped: {self.records_skipped}")


# ============================================================================
# CONNECTION POOL MANAGEMENT (psycopg2)
# ============================================================================
_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def init_connection_pool() -> pool.ThreadedConnectionPool:
    """Initialize threaded connection pool for parallel operations"""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=CONNECTION_POOL_SIZE,
            dsn=NEON_DATABASE_URL
        )
        logger.info(f"Connection pool initialized (max={CONNECTION_POOL_SIZE})")
    return _connection_pool


def close_connection_pool():
    """Close all connections in the pool"""
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Connection pool closed")


@contextmanager
def get_connection():
    """Context manager for database connections from pool"""
    pool_inst = init_connection_pool()
    conn = pool_inst.getconn()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        pool_inst.putconn(conn)


def get_supabase_client():
    """Get Supabase client"""
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# ============================================================================
# NORMALIZATION FUNCTIONS (Vectorized for performance)
# ============================================================================
def normalize_operator(name: Optional[str]) -> str:
    """Normalize operator name"""
    if not name:
        return 'Desconocido'
    key = str(name).lower().strip()
    return OPERATOR_MAP.get(key, 'Desconocido')


def normalize_network(tipo: Optional[str]) -> str:
    """Normalize network type"""
    if not tipo:
        return 'Desconocida'
    t = str(tipo).upper().strip()
    if '5G' in t: return '5G'
    if '4G' in t or 'LTE' in t: return '4G LTE'
    if '3G' in t or 'HSPA' in t or 'WCDMA' in t: return '3G'
    if '2G' in t or 'GSM' in t or 'EDGE' in t: return '2G'
    return 'Desconocida'


def classify_signal(dbm: Optional[float]) -> str:
    """Classify signal quality"""
    if dbm is None: return 'Sin Senal'
    if dbm >= -70: return 'Excelente'
    if dbm >= -85: return 'Buena'
    if dbm >= -100: return 'Regular'
    if dbm >= -110: return 'Debil'
    return 'Sin Senal'


def assign_zone(lat: Optional[float], lon: Optional[float]) -> int:
    """Assign zone based on coordinates"""
    if lat is None or lon is None:
        return 1
    
    min_dist = float('inf')
    zone_id = 1
    
    for zid, (_, zlat, zlon, radius) in SANTA_CRUZ_DISTRICTS.items():
        dist = ((lat - zlat) ** 2 + (lon - zlon) ** 2) ** 0.5
        if dist < min_dist and dist <= radius * 0.01:
            min_dist = dist
            zone_id = zid
    
    return zone_id


def get_device_identifier(record) -> Tuple[str, str]:
    """
    Get device identifier with fallback logic.
    Priority: device_id → device_name → hash fallback
    Returns: (identifier, display_name)
    """
    device_id = record.get('device_id')
    device_name = record.get('device_name')
    
    # Case 1: Tiene device_id válido
    if device_id and str(device_id).strip() and str(device_id).lower() not in ['none', 'null', '']:
        device_id_clean = str(device_id).strip()
        device_display = device_name or f"Device_{device_id_clean}"
        return device_id_clean, device_display
    
    # Case 2: NO tiene device_id pero tiene device_name
    if device_name and str(device_name).strip():
        device_name_clean = str(device_name).strip()
        # Usar device_name como identificador único
        return f"NAME_{device_name_clean}", device_name_clean
    
    # Case 3: No tiene ninguno - generar hash único
    fallback_id = f"UNKNOWN_{hash(str(record)) % 100000}"
    return fallback_id, "Dispositivo Desconocido"


def generate_device_hash(device_id: str, device_model: str = "") -> str:
    """Generate consistent device hash"""
    import hashlib
    combined = f"{device_id}_{device_model}".lower().strip()
    return hashlib.md5(combined.encode()).hexdigest()[:16]


def get_period(hour: int) -> str:
    """
    Convert hour (0-23) to time period.
    """
    if 0 <= hour < 6:
        return "Madrugada"
    elif 6 <= hour < 12:
        return "Mañana"
    elif 12 <= hour < 18:
        return "Tarde"
    elif 18 <= hour < 24:
        return "Noche"
    else:
        return "Desconocido"


def convert_speed_to_kmh(speed_ms) -> float:
    """
    Convert speed from m/s to km/h.
    1 m/s = 3.6 km/h
    """
    if speed_ms is None or pd.isna(speed_ms):
        return 0.0
    
    try:
        speed_float = float(speed_ms)
        # Clamp negative speeds to 0
        if speed_float < 0:
            return 0.0
        
        # Convert m/s to km/h
        speed_kmh = speed_float * 3.6
        
        # Clamp unrealistic speeds (>200 km/h)
        if speed_kmh > 200:
            logger.warning(f"Unrealistic speed detected: {speed_kmh:.1f} km/h (original: {speed_ms} m/s)")
            return 200.0
        
        return round(speed_kmh, 2)
    
    except (ValueError, TypeError):
        return 0.0


# Vectorized versions for DataFrame operations
def vectorized_normalize_operator(series: pd.Series) -> pd.Series:
    """Vectorized operator normalization"""
    return series.fillna('').str.lower().str.strip().map(
        lambda x: OPERATOR_MAP.get(x, 'Desconocido')
    )


def vectorized_normalize_network(series: pd.Series) -> pd.Series:
    """Vectorized network normalization"""
    def _normalize(t):
        if pd.isna(t) or t == '':
            return 'Desconocida'
        t = str(t).upper().strip()
        if '5G' in t: return '5G'
        if '4G' in t or 'LTE' in t: return '4G LTE'
        if '3G' in t or 'HSPA' in t or 'WCDMA' in t: return '3G'
        if '2G' in t or 'GSM' in t or 'EDGE' in t: return '2G'
        return 'Desconocida'
    return series.apply(_normalize)


def vectorized_classify_signal(series: pd.Series) -> pd.Series:
    """Vectorized signal classification"""
    conditions = [
        series.isna(),
        series >= -70,
        series >= -85,
        series >= -100,
        series >= -110,
    ]
    choices = ['Sin Senal', 'Excelente', 'Buena', 'Regular', 'Debil']
    return pd.Series(np.select(conditions, choices, default='Sin Senal'), index=series.index)

# ============================================================================
# SCHEMA DDL
# ============================================================================
SCHEMA_DDL = [
    """CREATE TABLE IF NOT EXISTS DIM_TIEMPO (
        tiempo_id SERIAL PRIMARY KEY,
        fecha DATE UNIQUE NOT NULL,
        anio INTEGER,
        mes INTEGER,
        dia INTEGER,
        dia_semana INTEGER,
        nombre_dia VARCHAR(15),
        nombre_mes VARCHAR(15),
        trimestre INTEGER,
        es_fin_semana BOOLEAN
    )""",
    """CREATE TABLE IF NOT EXISTS DIM_HORA (
        hora_id SERIAL PRIMARY KEY,
        hora INTEGER UNIQUE NOT NULL,
        periodo VARCHAR(20),
        nombre_hora VARCHAR(15),
        es_hora_pico BOOLEAN
    )""",
    """CREATE TABLE IF NOT EXISTS DIM_OPERADOR (
        operador_id SERIAL PRIMARY KEY,
        nombre VARCHAR(100) UNIQUE NOT NULL,
        grupo VARCHAR(50),
        es_principal BOOLEAN DEFAULT TRUE
    )""",
    """CREATE TABLE IF NOT EXISTS DIM_RED (
        red_id SERIAL PRIMARY KEY,
        tipo_red VARCHAR(50) UNIQUE NOT NULL,
        generacion INTEGER,
        velocidad_teorica VARCHAR(50)
    )""",
    """CREATE TABLE IF NOT EXISTS DIM_CALIDAD (
        calidad_id SERIAL PRIMARY KEY,
        rango_senal VARCHAR(50) UNIQUE NOT NULL,
        nivel VARCHAR(20),
        descripcion TEXT
    )""",
    """CREATE TABLE IF NOT EXISTS DIM_UBICACION (
        ubicacion_id SERIAL PRIMARY KEY,
        latitud DECIMAL(10,7),
        longitud DECIMAL(10,7),
        altitud DECIMAL(10,2),
        zona_id INTEGER,
        geohash VARCHAR(30),
        UNIQUE(latitud, longitud)
    )""",
    """CREATE TABLE IF NOT EXISTS DIM_DISPOSITIVO (
        dispositivo_id SERIAL PRIMARY KEY,
        device_hash VARCHAR(32) UNIQUE NOT NULL,
        nombre_dispositivo VARCHAR(100),
        modelo VARCHAR(100),
        fabricante VARCHAR(100)
    )""",
    """CREATE TABLE IF NOT EXISTS DIM_ZONAS (
        zona_id INTEGER PRIMARY KEY,
        nombre VARCHAR(100) NOT NULL,
        latitud_centro DECIMAL(10,7),
        longitud_centro DECIMAL(10,7),
        radio_km DECIMAL(5,2)
    )""",
    """CREATE TABLE IF NOT EXISTS FACT_MEDICIONES (
        medicion_id SERIAL PRIMARY KEY,
        source_id BIGINT UNIQUE,
        tiempo_id INTEGER REFERENCES DIM_TIEMPO(tiempo_id),
        hora_id INTEGER REFERENCES DIM_HORA(hora_id),
        operador_id INTEGER REFERENCES DIM_OPERADOR(operador_id),
        red_id INTEGER REFERENCES DIM_RED(red_id),
        calidad_id INTEGER REFERENCES DIM_CALIDAD(calidad_id),
        ubicacion_id INTEGER REFERENCES DIM_UBICACION(ubicacion_id),
        dispositivo_id INTEGER REFERENCES DIM_DISPOSITIVO(dispositivo_id),
        zona_id INTEGER REFERENCES DIM_ZONAS(zona_id),
        senal_dbm DECIMAL(6,2),
        velocidad DECIMAL(10,2),
        bateria INTEGER,
        timestamp_original TIMESTAMP,
        fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )"""
]

# Index definitions for optimal query performance
INDEX_DDL = [
    "CREATE INDEX IF NOT EXISTS idx_fact_tiempo ON FACT_MEDICIONES(tiempo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_hora ON FACT_MEDICIONES(hora_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_operador ON FACT_MEDICIONES(operador_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_red ON FACT_MEDICIONES(red_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_calidad ON FACT_MEDICIONES(calidad_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_ubicacion ON FACT_MEDICIONES(ubicacion_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_dispositivo ON FACT_MEDICIONES(dispositivo_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_zona ON FACT_MEDICIONES(zona_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_source ON FACT_MEDICIONES(source_id)",
    "CREATE INDEX IF NOT EXISTS idx_fact_timestamp ON FACT_MEDICIONES(timestamp_original)",
    "CREATE INDEX IF NOT EXISTS idx_fact_composite ON FACT_MEDICIONES(tiempo_id, operador_id, red_id)",
    "CREATE INDEX IF NOT EXISTS idx_ubicacion_coords ON DIM_UBICACION(latitud, longitud)",
    "CREATE INDEX IF NOT EXISTS idx_ubicacion_zona ON DIM_UBICACION(zona_id)",
]


def drop_all_tables():
    """Drop all DW tables for full schema recreation."""
    DROP_ORDER = [
        "DROP TABLE IF EXISTS FACT_MEDICIONES CASCADE",
        "DROP TABLE IF EXISTS DIM_UBICACION CASCADE",
        "DROP TABLE IF EXISTS DIM_DISPOSITIVO CASCADE",
        "DROP TABLE IF EXISTS DIM_TIEMPO CASCADE",
        "DROP TABLE IF EXISTS DIM_HORA CASCADE",
        "DROP TABLE IF EXISTS DIM_OPERADOR CASCADE",
        "DROP TABLE IF EXISTS DIM_RED CASCADE",
        "DROP TABLE IF EXISTS DIM_CALIDAD CASCADE",
        "DROP TABLE IF EXISTS DIM_ZONAS CASCADE",
    ]
    
    with get_connection() as conn:
        cur = conn.cursor()
        for drop_sql in DROP_ORDER:
            try:
                cur.execute(drop_sql)
            except Exception as e:
                logger.warning(f"Drop warning: {e}")
        conn.commit()
    logger.info("All tables dropped for schema recreation")


def create_schema():
    """Create DW schema using psycopg2"""
    with get_connection() as conn:
        cur = conn.cursor()
        for ddl in SCHEMA_DDL:
            cur.execute(ddl)
        conn.commit()
        logger.info("Schema verified/created")


def validate_and_fix_schema():
    """
    Analyze actual column sizes and auto-adjust schema constraints.
    Execute after CREATE TABLE to fix existing tables.
    """
    ALTER_QUERIES = [
        ("DIM_UBICACION", "geohash", "VARCHAR(30)"),
        ("DIM_DISPOSITIVO", "nombre_dispositivo", "VARCHAR(200)"),
        ("DIM_DISPOSITIVO", "modelo", "VARCHAR(200)"),
        ("DIM_DISPOSITIVO", "device_hash", "VARCHAR(64)"),
        ("DIM_OPERADOR", "nombre", "VARCHAR(100)"),
        ("DIM_ZONAS", "nombre", "VARCHAR(100)"),
    ]
    
    with get_connection() as conn:
        cur = conn.cursor()
        
        for table, column, new_type in ALTER_QUERIES:
            try:
                cur.execute(f"ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type}")
                logger.info(f"Schema adjusted: {table}.{column} -> {new_type}")
            except Exception as e:
                if "does not exist" in str(e):
                    pass  # Column doesn't exist, skip
                else:
                    logger.debug(f"Schema adjustment skipped for {table}.{column}: {e}")
        
        conn.commit()
        logger.info("Schema validation and fixes completed")


# ============================================================================
# DATA VALIDATION FUNCTIONS
# ============================================================================
def validate_and_truncate(value: Any, max_length: int, field_name: str) -> Optional[str]:
    """Truncate string values exceeding max_length with warning."""
    if value is None:
        return None
    
    str_value = str(value).strip()
    
    if len(str_value) > max_length:
        logger.warning(
            f"Field {field_name} truncated: '{str_value[:30]}...' "
            f"({len(str_value)} chars) -> {max_length} chars"
        )
        return str_value[:max_length]
    
    return str_value


def validate_coordinates(lat: Any, lon: Any) -> Tuple[float, float]:
    """Validate and clamp coordinates to valid ranges."""
    DEFAULT_LAT, DEFAULT_LON = -17.7833, -63.1822  # Santa Cruz center
    
    try:
        lat_float = float(lat) if lat is not None else DEFAULT_LAT
        lon_float = float(lon) if lon is not None else DEFAULT_LON
        
        # Clamp to valid ranges
        lat_clamped = max(-90, min(90, lat_float))
        lon_clamped = max(-180, min(180, lon_float))
        
        return lat_clamped, lon_clamped
    
    except (ValueError, TypeError):
        return DEFAULT_LAT, DEFAULT_LON


def validate_numeric(value: Any, min_val: float, max_val: float, 
                     default: float, field_name: str) -> float:
    """Validate numeric value within range, return default if invalid."""
    try:
        num = float(value) if value is not None else None
        
        if num is None:
            return default
        
        if num < min_val or num > max_val:
            return default
        
        return num
    
    except (ValueError, TypeError):
        return default


def sanitize_string(value: Any) -> Optional[str]:
    """Remove invalid characters and normalize string."""
    if value is None:
        return None
    
    clean = str(value).strip()
    clean = ' '.join(clean.split())  # Normalize whitespace
    clean = ''.join(char for char in clean if ord(char) >= 32 or char in '\n\t')
    
    return clean if clean else None


def generate_safe_geohash(lat: float, lon: float, max_length: int = 25) -> str:
    """Generate geohash with guaranteed max length."""
    try:
        lat_validated, lon_validated = validate_coordinates(lat, lon)
        lat_int = int(lat_validated * 10000)
        lon_int = int(lon_validated * 10000)
        geohash = f"{lat_int}_{lon_int}"
        return geohash[:max_length]
    except Exception:
        return "0_0"

# ============================================================================
# INDEX MANAGEMENT (Drop/Create for bulk loading)
# ============================================================================
def drop_indexes():
    """Drop indexes before bulk load for performance"""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT indexname FROM pg_indexes 
            WHERE tablename = 'fact_mediciones' 
            AND indexname NOT LIKE '%pkey%'
        """)
        indexes = cur.fetchall()
        for (idx_name,) in indexes:
            try:
                cur.execute(f"DROP INDEX IF EXISTS {idx_name}")
            except Exception:
                pass
        conn.commit()
        logger.info(f"Dropped {len(indexes)} indexes for bulk load")


def create_indexes():
    """Create indexes after bulk load"""
    with get_connection() as conn:
        cur = conn.cursor()
        for idx_sql in INDEX_DDL:
            try:
                cur.execute(idx_sql)
            except Exception as e:
                logger.warning(f"Index creation warning: {e}")
        conn.commit()
        logger.info(f"Created {len(INDEX_DDL)} indexes")


def vacuum_analyze():
    """Run VACUUM ANALYZE on all tables"""
    conn = psycopg2.connect(NEON_DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()
    tables = ['FACT_MEDICIONES', 'DIM_TIEMPO', 'DIM_HORA', 'DIM_OPERADOR', 
              'DIM_RED', 'DIM_CALIDAD', 'DIM_UBICACION', 'DIM_DISPOSITIVO', 'DIM_ZONAS']
    for table in tables:
        try:
            cur.execute(f"ANALYZE {table}")
        except Exception as e:
            logger.warning(f"Analyze {table}: {e}")
    cur.close()
    conn.close()
    logger.info("ANALYZE completed on all tables")

# STATIC DIMENSIONS (Bulk insert with execute_values)
# ============================================================================
def populate_static_dimensions():
    """Populate static dimension tables"""
    with get_connection() as conn:
        cur = conn.cursor()
        
        # DIM_HORA (24 horas)
        execute_values(cur, """
            INSERT INTO DIM_HORA (hora, periodo, nombre_hora) VALUES %s
            ON CONFLICT (hora) DO NOTHING
        """, [(h, get_period(h), f"{h:02d}:00") for h in range(24)], page_size=100)
        
        # DIM_OPERADOR
        execute_values(cur, """
            INSERT INTO DIM_OPERADOR (nombre) VALUES %s
            ON CONFLICT (nombre) DO NOTHING
        """, [(op,) for op in OPERADORES], page_size=100)
        
        # DIM_RED
        execute_values(cur, """
            INSERT INTO DIM_RED (tipo_red, generacion, velocidad_teorica)
            VALUES %s ON CONFLICT (tipo_red) DO NOTHING
        """, red_data, page_size=100)
        
        # DIM_CALIDAD (5 quality levels)
        calidad_data = [
            ('Excelente', 'Excelente', '>= -70 dBm'),
            ('Buena', 'Buena', '-70 a -85 dBm'),
            ('Regular', 'Regular', '-85 a -100 dBm'),
            ('Debil', 'Debil', '-100 a -110 dBm'),
            ('Sin Senal', 'Sin Senal', '< -110 dBm')
        ]
        execute_values(cur, """
            INSERT INTO DIM_CALIDAD (rango_senal, nivel, descripcion)
            VALUES %s ON CONFLICT (rango_senal) DO NOTHING
        """, calidad_data, page_size=100)
        
        # DIM_ZONAS from real district data
        populate_dim_zonas_from_districts(cur)
        
        conn.commit()
        logger.info("Static dimensions populated")


def populate_dim_zonas_from_districts(cur):
    """Populate zones dimension from real district data."""
    try:
        distrito_mgr = get_distrito_manager()
        distritos = distrito_mgr.get_all_distritos()
        
        zones_data = []
        
        for idx, distrito in enumerate(distritos, start=1):
            # Extract centroid coordinates
            centroid = distrito.get('centroid')
            if centroid and len(centroid) >= 2:
                centro_lat = float(centroid[1])
                centro_lon = float(centroid[0])
            else:
                # Fallback to bounds center
                bounds = distrito.get('bounds', (0, 0, 0, 0))
                centro_lat = float((bounds[1] + bounds[3]) / 2)
                centro_lon = float((bounds[0] + bounds[2]) / 2)
            
            zones_data.append((
                idx,                          # zona_id
                distrito['nombre'][:100],     # nombre (truncated to VARCHAR(100))
                centro_lat,                   # latitud_centro
                centro_lon,                   # longitud_centro
                5.0                           # radio_km (default for districts)
            ))
        
        execute_values(cur, """
            INSERT INTO DIM_ZONAS (zona_id, nombre, latitud_centro, longitud_centro, radio_km) VALUES %s
            ON CONFLICT (zona_id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                latitud_centro = EXCLUDED.latitud_centro,
                longitud_centro = EXCLUDED.longitud_centro,
                radio_km = EXCLUDED.radio_km
        """, zones_data, page_size=100)
        
        logger.info(f"Populated {len(zones_data)} zones from district data")
        
    except Exception as e:
        logger.warning(f"Failed to load district data: {e}. Using synthetic zones.")
        # Fallback to synthetic zones
        zonas_data = [(zid, nombre, lat, lon, radio) 
                      for zid, (nombre, lat, lon, radio) in SANTA_CRUZ_DISTRICTS.items()]
        execute_values(cur, """
            INSERT INTO DIM_ZONAS (zona_id, nombre, latitud_centro, longitud_centro, radio_km)
            VALUES %s ON CONFLICT (zona_id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                latitud_centro = EXCLUDED.latitud_centro,
                longitud_centro = EXCLUDED.longitud_centro,
                radio_km = EXCLUDED.radio_km
        """, zonas_data, page_size=100)


def assign_zone_from_districts(lat: float, lon: float) -> int:
    """Assign zone using real district polygons."""
    try:
        # Ensure cache is populated
        populate_zona_id_cache()
        
        distrito_mgr = get_distrito_manager()
        distrito = distrito_mgr.classify_point(lat, lon)
        
        if distrito:
            # Get zona_id from cache
            zona_id = ZONA_ID_CACHE.get(distrito['nombre'])
            if zona_id is not None:
                return zona_id
        
        # Fallback to synthetic zone assignment
        return assign_zone_synthetic(lat, lon)
        
    except Exception as e:
        logger.warning(f"District zone assignment failed: {e}. Using synthetic fallback.")
        return assign_zone_synthetic(lat, lon)


def assign_zone_synthetic(lat: float, lon: float) -> int:
    """Fallback synthetic zone assignment by distance."""
    min_dist = float('inf')
    closest_zone = 1
    
    for zid, (_, zlat, zlon, _) in SANTA_CRUZ_DISTRICTS.items():
        dist = ((lat - zlat) ** 2 + (lon - zlon) ** 2) ** 0.5
        if dist < min_dist:
            min_dist = dist
            closest_zone = zid
    
    return closest_zone


# ============================================================================
# ZONA ID CACHE
# ============================================================================
ZONA_ID_CACHE: Dict[str, int] = {}


def populate_zona_id_cache():
    """Populate zona_id cache from DIM_ZONAS table."""
    global ZONA_ID_CACHE
    if ZONA_ID_CACHE:
        return  # Already populated
    
    try:
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT nombre, zona_id FROM DIM_ZONAS")
            for row in cur.fetchall():
                ZONA_ID_CACHE[row[0]] = row[1]
        logger.info(f"Zona ID cache populated with {len(ZONA_ID_CACHE)} entries")
    except Exception as e:
        logger.warning(f"Failed to populate zona cache: {e}. Using fallback.")
        # Fallback to synthetic zones
        for zid, (nombre, _, _, _) in SANTA_CRUZ_DISTRICTS.items():
            ZONA_ID_CACHE[nombre] = zid


# ============================================================================
# SUPABASE DATA LOADING (Optimized batch size 50k)
# ============================================================================
def get_last_loaded_id() -> int:
    """Get last loaded source_id for incremental loading"""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(source_id), 0) FROM FACT_MEDICIONES")
        result = cur.fetchone()
        return result[0] if result else 0


def fetch_supabase_batch(client, start_id: int, batch_size: int) -> List[Dict]:
    """Fetch a batch of data from Supabase"""
    response = client.table('locations').select('*').gt('id', start_id).order('id').limit(batch_size).execute()
    return response.data


def fetch_all_from_supabase(client, last_id: int, stats: ETLStats) -> pd.DataFrame:
    """Fetch all new data from Supabase with optimized batch size"""
    all_data = []
    current_id = last_id
    batch_count = 0
    
    logger.info(f"Starting Supabase fetch from id > {last_id}")
    
    while True:
        batch = fetch_supabase_batch(client, current_id, SUPABASE_BATCH_SIZE)
        if not batch:
            break
        all_data.extend(batch)
        current_id = batch[-1]['id']
        batch_count += 1
        stats.records_processed = len(all_data)
        logger.info(f"  Batch {batch_count}: {len(all_data):,} records fetched")
    
    if all_data:
        return pd.DataFrame(all_data)
    return pd.DataFrame()

# ============================================================================
# DIMENSION LOOKUPS (Cached for performance)
# ============================================================================
class DimensionCache:
    """Cache for dimension lookups to avoid repeated queries"""
    
    def __init__(self):
        self.operadores: Dict[str, int] = {}
        self.redes: Dict[str, int] = {}
        self.calidades: Dict[str, int] = {}
        self.horas: Dict[int, int] = {}
        self.tiempos: Dict[str, int] = {}
        self.ubicaciones: Dict[Tuple[float, float], int] = {}
        self.dispositivos: Dict[str, int] = {}
    
    def load_static_dimensions(self):
        """Load all static dimension mappings"""
        with get_connection() as conn:
            cur = conn.cursor()
            
            cur.execute("SELECT nombre, operador_id FROM DIM_OPERADOR")
            self.operadores = {row[0]: row[1] for row in cur.fetchall()}
            
            cur.execute("SELECT tipo_red, red_id FROM DIM_RED")
            self.redes = {row[0]: row[1] for row in cur.fetchall()}
            
            cur.execute("SELECT rango_senal, calidad_id FROM DIM_CALIDAD")
            self.calidades = {row[0]: row[1] for row in cur.fetchall()}
            
            cur.execute("SELECT hora, hora_id FROM DIM_HORA")
            self.horas = {row[0]: row[1] for row in cur.fetchall()}
            
            cur.execute("SELECT fecha, tiempo_id FROM DIM_TIEMPO")
            self.tiempos = {str(row[0]): row[1] for row in cur.fetchall()}
            
            cur.execute("SELECT latitud, longitud, ubicacion_id FROM DIM_UBICACION")
            for row in cur.fetchall():
                self.ubicaciones[(float(row[0]), float(row[1]))] = row[2]
            
            cur.execute("SELECT device_hash, dispositivo_id FROM DIM_DISPOSITIVO")
            self.dispositivos = {row[0]: row[1] for row in cur.fetchall()}
        
        logger.info(f"Dimension cache loaded: {len(self.operadores)} operators, "
                   f"{len(self.tiempos)} dates, {len(self.ubicaciones)} locations")


# ============================================================================
# BULK INSERT FUNCTIONS (execute_values for performance)
# ============================================================================
DIAS_SEMANA = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo']
MESES = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
         'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']


def bulk_insert_tiempos(fechas: set, cache: DimensionCache):
    """Bulk insert new dates into DIM_TIEMPO"""
    new_fechas = [f for f in fechas if str(f) not in cache.tiempos]
    if not new_fechas:
        return
    
    data = []
    for fecha in new_fechas:
        data.append((
            fecha, fecha.year, fecha.month, fecha.day,
            fecha.weekday(), DIAS_SEMANA[fecha.weekday()],
            MESES[fecha.month - 1], (fecha.month - 1) // 3 + 1,
            fecha.weekday() >= 5
        ))
    
    with get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO DIM_TIEMPO (fecha, anio, mes, dia, dia_semana, nombre_dia, nombre_mes, trimestre, es_fin_semana)
            VALUES %s ON CONFLICT (fecha) DO NOTHING
        """, data, page_size=PG_BATCH_SIZE)
        
        # Cast string array to date array for proper comparison
        cur.execute("SELECT fecha, tiempo_id FROM DIM_TIEMPO WHERE fecha = ANY(%s::date[])", 
                   (new_fechas,))
        for row in cur.fetchall():
            cache.tiempos[str(row[0])] = row[1]
        conn.commit()
    
    logger.info(f"Inserted {len(new_fechas)} new dates")


def bulk_insert_ubicaciones(ubicaciones: List[Tuple], cache: DimensionCache):
    """Bulk insert new locations with validation and retry logic"""
    # Validate and prepare data
    new_ubicaciones = []
    for lat, lon, alt, zona_id in ubicaciones:
        lat_valid, lon_valid = validate_coordinates(lat, lon)
        key = (float(lat_valid), float(lon_valid))
        
        if key not in cache.ubicaciones:
            geohash = generate_safe_geohash(lat_valid, lon_valid, max_length=25)
            alt_valid = validate_numeric(alt, -500, 10000, 0, "altitud")
            new_ubicaciones.append((lat_valid, lon_valid, alt_valid, zona_id, geohash))
    
    if not new_ubicaciones:
        return
    
    MINI_BATCH = 1000
    total_inserted = 0
    total_failed = 0
    
    with get_connection() as conn:
        cur = conn.cursor()
        
        for i in range(0, len(new_ubicaciones), MINI_BATCH):
            batch = new_ubicaciones[i:i+MINI_BATCH]
            
            try:
                execute_values(cur, """
                    INSERT INTO DIM_UBICACION (latitud, longitud, altitud, zona_id, geohash)
                    VALUES %s ON CONFLICT (latitud, longitud) DO NOTHING
                """, batch, page_size=MINI_BATCH)
                conn.commit()
                total_inserted += len(batch)
                
            except Exception as e:
                conn.rollback()
                logger.warning(f"Ubicacion batch failed: {e}. Retrying one-by-one...")
                
                for record in batch:
                    try:
                        cur.execute("""
                            INSERT INTO DIM_UBICACION (latitud, longitud, altitud, zona_id, geohash)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (latitud, longitud) DO NOTHING
                        """, record)
                        conn.commit()
                        total_inserted += 1
                    except Exception as rec_err:
                        conn.rollback()
                        total_failed += 1
                        if total_failed <= 5:
                            logger.error(f"Record failed: {record[:3]}... - {rec_err}")
        
        # Refresh cache with all inserted locations
        cur.execute("SELECT latitud, longitud, ubicacion_id FROM DIM_UBICACION")
        for row in cur.fetchall():
            cache.ubicaciones[(float(row[0]), float(row[1]))] = row[2]
    
    if total_failed > 0:
        logger.warning(f"Ubicaciones: {total_inserted} inserted, {total_failed} failed")
    else:
        logger.info(f"Inserted {total_inserted} new locations")


def bulk_insert_dispositivos(dispositivos: List[Tuple], cache: DimensionCache):
    """Bulk insert new devices with validation"""
    new_devices = []
    for device_hash, nombre, modelo in dispositivos:
        if device_hash not in cache.dispositivos:
            # Validate and truncate fields
            hash_valid = validate_and_truncate(str(device_hash), 64, "device_hash") or "unknown"
            nombre_valid = validate_and_truncate(sanitize_string(nombre), 200, "nombre_dispositivo") or "Unknown"
            modelo_valid = validate_and_truncate(sanitize_string(modelo), 200, "modelo") or "Unknown"
            new_devices.append((hash_valid, nombre_valid, modelo_valid, 'Unknown'))
    
    if not new_devices:
        return
    
    total_inserted = 0
    total_failed = 0
    
    with get_connection() as conn:
        cur = conn.cursor()
        
        try:
            execute_values(cur, """
                INSERT INTO DIM_DISPOSITIVO (device_hash, nombre_dispositivo, modelo, fabricante)
                VALUES %s ON CONFLICT (device_hash) DO NOTHING
            """, new_devices, page_size=PG_BATCH_SIZE)
            conn.commit()
            total_inserted = len(new_devices)
            
        except Exception as e:
            conn.rollback()
            logger.warning(f"Device batch insert failed: {e}. Retrying one-by-one...")
            
            for record in new_devices:
                try:
                    cur.execute("""
                        INSERT INTO DIM_DISPOSITIVO (device_hash, nombre_dispositivo, modelo, fabricante)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (device_hash) DO NOTHING
                    """, record)
                    conn.commit()
                    total_inserted += 1
                except Exception as rec_err:
                    conn.rollback()
                    total_failed += 1
                    if total_failed <= 5:
                        logger.error(f"Device record failed: {record[0][:20]}... - {rec_err}")
        
        # Refresh cache
        cur.execute("SELECT device_hash, dispositivo_id FROM DIM_DISPOSITIVO")
        for row in cur.fetchall():
            cache.dispositivos[row[0]] = row[1]
    
    if total_failed > 0:
        logger.warning(f"Dispositivos: {total_inserted} inserted, {total_failed} failed")
    else:
        logger.info(f"Inserted {total_inserted} new devices")


def bulk_insert_facts(facts: List[Tuple], stats: ETLStats):
    """Bulk insert facts using execute_values with page_size=5000"""
    if not facts:
        return
    
    with get_connection() as conn:
        cur = conn.cursor()
        execute_values(cur, """
            INSERT INTO FACT_MEDICIONES (
                source_id, tiempo_id, hora_id, operador_id, red_id, calidad_id,
                ubicacion_id, dispositivo_id, zona_id, senal_dbm, velocidad, 
                bateria, timestamp_original
            ) VALUES %s ON CONFLICT (source_id) DO NOTHING
        """, facts, page_size=PG_BATCH_SIZE)
        stats.records_inserted += len(facts)
        conn.commit()


# ============================================================================
# DATA TRANSFORMATION (Vectorized pandas operations)
# ============================================================================
def transform_dataframe(df: pd.DataFrame, cache: DimensionCache) -> Tuple[List[Tuple], ETLStats]:
    """Transform DataFrame to fact tuples using vectorized operations"""
    stats_errors = []
    
    # Parse timestamps with ISO8601 format (handles with/without microseconds)
    raw_ts = df['timestamp'].fillna(df.get('created_at', pd.NaT))
    df['ts'] = pd.to_datetime(raw_ts, format='ISO8601', errors='coerce')
    
    # Log invalid timestamps
    invalid_count = df['ts'].isna().sum()
    if invalid_count > 0:
        logger.warning(f"{invalid_count} records with invalid timestamps will be skipped")
    
    df = df.dropna(subset=['ts'])
    
    if df.empty:
        return [], stats_errors
    
    # Extract date components
    df['fecha'] = df['ts'].dt.date
    df['hora'] = df['ts'].dt.hour
    
    # Normalize using vectorized operations
    df['operador_norm'] = vectorized_normalize_operator(
        df.get('sim_operator', df.get('operator_name', pd.Series(dtype=str)))
    )
    df['red_norm'] = vectorized_normalize_network(
        df.get('network_type', pd.Series(dtype=str))
    )
    
    # Signal processing
    df['senal'] = df.get('signal', df.get('signal_strength', df.get('dbm', pd.Series(dtype=float))))
    df['calidad_norm'] = vectorized_classify_signal(df['senal'])
    
    # Speed conversion from m/s to km/h
    df['velocidad_ms'] = df.get('speed', df.get('velocidad', pd.Series(dtype=float))).fillna(0)
    df['velocidad_kmh'] = df['velocidad_ms'].apply(convert_speed_to_kmh)
    
    # Coordinates
    df['lat'] = pd.to_numeric(df.get('latitude', pd.Series(dtype=float)), errors='coerce')
    df['lon'] = pd.to_numeric(df.get('longitude', pd.Series(dtype=float)), errors='coerce')
    df['alt'] = pd.to_numeric(df.get('altitude', pd.Series(dtype=float)), errors='coerce').fillna(0)
    
    # Fill missing coordinates with center
    df['lat'] = df['lat'].fillna(-17.7833)
    df['lon'] = df['lon'].fillna(-63.1822)
    
    # Assign zones using real district polygons with progress logging
    logger.info("Assigning zones using district polygons...")
    total_records = len(df)
    if total_records > 50000:
        logger.warning(f"Zone assignment for {total_records:,} records may take 5-10 minutes. This is normal for point-in-polygon operations.")
    
    def assign_zone_with_progress(row):
        if row.name % 10000 == 0 and row.name > 0:
            logger.info(f"  Zone assignment progress: {row.name:,}/{total_records:,} ({row.name/total_records*100:.1f}%)")
        return assign_zone_from_districts(row['lat'], row['lon'])
    
    df['zona_id'] = df.apply(assign_zone_with_progress, axis=1)
    logger.info(f"Zone assignment completed for {total_records:,} records")
    
    # Apply device identifier with fallback logic
    device_data = df.apply(lambda r: get_device_identifier(r), axis=1)
    df['device_identifier'] = device_data.apply(lambda x: x[0])
    df['device_display_name'] = device_data.apply(lambda x: x[1])
    df['device_model'] = df.get('device_model', 'Unknown')
    
    # Generate hash for database storage
    df['device_hash'] = df.apply(
        lambda r: generate_device_hash(r['device_identifier'], r['device_model']),
        axis=1
    )
    
    # Bulk insert new dimensions first
    unique_fechas = set(df['fecha'].unique())
    bulk_insert_tiempos(unique_fechas, cache)
    
    unique_ubicaciones = df[['lat', 'lon', 'alt', 'zona_id']].drop_duplicates().values.tolist()
    bulk_insert_ubicaciones(unique_ubicaciones, cache)
    
    unique_devices = df[['device_hash', 'device_display_name', 'device_model']].drop_duplicates().values.tolist()
    bulk_insert_dispositivos(unique_devices, cache)
    
    # Refresh cache for new entries
    cache.load_static_dimensions()
    
    # Build fact tuples
    facts = []
    default_operador = cache.operadores.get('Desconocido', 1)
    default_red = cache.redes.get('Desconocida', 1)
    default_calidad = cache.calidades.get('Sin Senal', 1)
    
    for _, row in df.iterrows():
        try:
            tiempo_id = cache.tiempos.get(str(row['fecha']), 1)
            hora_id = cache.horas.get(row['hora'], 1)
            operador_id = cache.operadores.get(row['operador_norm'], default_operador)
            red_id = cache.redes.get(row['red_norm'], default_red)
            calidad_id = cache.calidades.get(row['calidad_norm'], default_calidad)
            ubicacion_id = cache.ubicaciones.get((float(row['lat']), float(row['lon'])), 1)
            dispositivo_id = cache.dispositivos.get(row['device_hash'], 1)
            
            facts.append((
                row['id'],
                tiempo_id,
                hora_id,
                operador_id,
                red_id,
                calidad_id,
                ubicacion_id,
                dispositivo_id,
                row['zona_id'],
                row['senal'] if pd.notna(row['senal']) else None,
                row['velocidad_kmh'],  # Use converted km/h speed
                row.get('battery', row.get('battery_level', None)),
                row['ts']
            ))
        except Exception as e:
            stats_errors.append(f"Row {row.get('id')}: {e}")
    
    return facts, stats_errors

# ============================================================================
# AGGREGATE VIEWS
# ============================================================================
VIEWS_DDL = [
    """CREATE OR REPLACE VIEW v_operador_zona AS
    SELECT o.nombre as operador, z.nombre as zona, COUNT(*) as mediciones,
           AVG(f.senal_dbm) as senal_promedio, AVG(f.velocidad) as velocidad_promedio,
           AVG(f.bateria) as bateria_promedio
    FROM FACT_MEDICIONES f
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    GROUP BY o.nombre, z.nombre""",
    
    """CREATE OR REPLACE VIEW v_temporal AS
    SELECT t.fecha, h.hora, h.periodo, COUNT(*) as mediciones, AVG(f.senal_dbm) as senal_promedio
    FROM FACT_MEDICIONES f
    JOIN DIM_TIEMPO t ON f.tiempo_id = t.tiempo_id
    JOIN DIM_HORA h ON f.hora_id = h.hora_id
    GROUP BY t.fecha, h.hora, h.periodo""",
    
    """CREATE OR REPLACE VIEW v_calidad AS
    SELECT c.nivel as calidad, o.nombre as operador, COUNT(*) as mediciones
    FROM FACT_MEDICIONES f
    JOIN DIM_CALIDAD c ON f.calidad_id = c.calidad_id
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    GROUP BY c.nivel, o.nombre""",
    
    """CREATE OR REPLACE VIEW v_mapa AS
    SELECT u.latitud, u.longitud, o.nombre as operador, r.tipo_red as red,
           c.nivel as calidad, z.nombre as zona, f.senal_dbm, f.velocidad, f.bateria,
           f.timestamp_original
    FROM FACT_MEDICIONES f
    JOIN DIM_UBICACION u ON f.ubicacion_id = u.ubicacion_id
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    JOIN DIM_RED r ON f.red_id = r.red_id
    JOIN DIM_CALIDAD c ON f.calidad_id = c.calidad_id
    JOIN DIM_ZONAS z ON f.zona_id = z.zona_id""",
    
    """CREATE OR REPLACE VIEW v_kpis AS
    SELECT COUNT(*) as total_mediciones, COUNT(DISTINCT f.dispositivo_id) as dispositivos_unicos,
           AVG(f.senal_dbm) as senal_promedio, AVG(f.velocidad) as velocidad_promedio,
           AVG(f.bateria) as bateria_promedio, MIN(f.timestamp_original) as primera_medicion,
           MAX(f.timestamp_original) as ultima_medicion
    FROM FACT_MEDICIONES f"""
]


def create_aggregate_views():
    """Create aggregate views for dashboard"""
    with get_connection() as conn:
        cur = conn.cursor()
        for view_sql in VIEWS_DDL:
            cur.execute(view_sql)
        conn.commit()
    logger.info("Aggregate views created")


# ============================================================================
# MAIN ETL FUNCTION (Optimized for <5 minutes)
# ============================================================================
def run_etl(full_reload: bool = False):
    """
    Execute optimized ETL process
    Target: <5 minutes for 373k records
    """
    logger.info("=" * 60)
    logger.info("ETL DATAWAREHOUSE - OPTIMIZED VERSION")
    logger.info("=" * 60)
    
    stats = ETLStats(start_time=datetime.now())
    total_start = time.time()
    
    try:
        # Phase 1: Initialize connections
        phase_start = time.time()
        logger.info("[1/8] Initializing connections...")
        init_connection_pool()
        supabase = get_supabase_client()
        stats.log_phase("Connection Init", phase_start)
        
        # Phase 2: Handle full reload - DROP and recreate schema
        if full_reload:
            logger.info("[!] FULL RELOAD MODE - Dropping all tables for clean schema...")
            phase_start = time.time()
            drop_all_tables()
            stats.log_phase("Drop Tables", phase_start)
            last_id = 0
        else:
            last_id = get_last_loaded_id()
        
        # Phase 3: Verify/Create schema
        phase_start = time.time()
        logger.info("[2/8] Creating/Verifying schema...")
        create_schema()
        validate_and_fix_schema()  # Auto-fix column sizes
        stats.log_phase("Schema Verification", phase_start)
        
        # Phase 4: Populate static dimensions
        phase_start = time.time()
        logger.info("[3/8] Populating static dimensions...")
        populate_static_dimensions()
        stats.log_phase("Static Dimensions", phase_start)
        
        # Phase 4.5: Initialize district cache
        phase_start = time.time()
        logger.info("[3.5/8] Initializing district cache...")
        populate_zona_id_cache()
        stats.log_phase("District Cache", phase_start)
        
        logger.info(f"[4/8] Last loaded ID: {last_id}")
        
        # Phase 5: Fetch data from Supabase
        phase_start = time.time()
        logger.info("[5/8] Fetching data from Supabase...")
        df = fetch_all_from_supabase(supabase, last_id, stats)
        load_time = stats.log_phase("Supabase Fetch", phase_start)
        
        if load_time > LOAD_WARNING_THRESHOLD_SEC:
            logger.warning(f"WARNING: Supabase load took {load_time:.0f}s (threshold: {LOAD_WARNING_THRESHOLD_SEC}s)")
        
        if df.empty:
            logger.info("No new data to process")
        else:
            logger.info(f"Fetched {len(df):,} new records")
            
            # Phase 6: Initialize dimension cache
            phase_start = time.time()
            logger.info("[6/8] Loading dimension cache...")
            cache = DimensionCache()
            cache.load_static_dimensions()
            stats.log_phase("Cache Load", phase_start)
            
            # Phase 7: Transform and load data
            phase_start = time.time()
            logger.info("[7/8] Transforming and loading data...")
            
            # Process in batches for memory efficiency
            batch_size = 50000
            total_facts = 0
            
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size].copy()
                facts, errors = transform_dataframe(batch_df, cache)
                stats.errors.extend(errors)
                
                if facts:
                    bulk_insert_facts(facts, stats)
                    total_facts += len(facts)
                
                logger.info(f"  Processed {min(i+batch_size, len(df)):,}/{len(df):,} records "
                           f"({total_facts:,} facts inserted)")
            
            write_time = stats.log_phase("Transform & Load", phase_start)
            
            if write_time > WRITE_WARNING_THRESHOLD_SEC:
                logger.warning(f"WARNING: Write took {write_time:.0f}s (threshold: {WRITE_WARNING_THRESHOLD_SEC}s)")
        
        # Phase 8: Post-processing
        phase_start = time.time()
        logger.info("[8/8] Post-processing...")
        create_indexes()
        create_aggregate_views()
        vacuum_analyze()
        stats.log_phase("Post-processing", phase_start)
        
        # Final summary
        total_time = time.time() - total_start
        logger.info("=" * 60)
        logger.info("ETL COMPLETED")
        logger.info("=" * 60)
        logger.info(f"  Total Duration: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")
        logger.info(f"  Records Processed: {stats.records_processed:,}")
        logger.info(f"  Records Inserted: {stats.records_inserted:,}")
        logger.info(f"  Records Skipped: {stats.records_skipped}")
        
        if stats.errors:
            logger.warning(f"  Errors: {len(stats.errors)}")
            for e in stats.errors[:5]:
                logger.warning(f"    - {e}")
        
        # Verify final counts
        with get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM FACT_MEDICIONES")
            total = cur.fetchone()[0]
            logger.info(f"  Total records in DW: {total:,}")
        
        # Performance check
        if total_time > 300:
            logger.error(f"FAILED: ETL took {total_time:.0f}s (>5 minutes)")
            return stats
        else:
            logger.info(f"SUCCESS: ETL completed in {total_time:.0f}s (<5 minutes)")
        
        return stats
        
    except Exception as e:
        logger.error(f"ETL FAILED: {e}")
        raise
    finally:
        close_connection_pool()


# ============================================================================
# EXECUTION
# ============================================================================
if __name__ == "__main__":
    import sys
    
    full_reload = '--full' in sys.argv
    run_etl(full_reload=full_reload)
