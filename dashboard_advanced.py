"""
Dashboard Avanzado - Visualizacion de Datos de Tracking Movil
Stack: Dash + Plotly Express + Folium + psycopg2 (NO SQLAlchemy)
Optimizado para 373k+ registros con clustering y WebGL
"""

import os
import io
import base64
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from contextlib import contextmanager

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from folium.plugins import MarkerCluster, HeatMap
from dash import Dash, html, dcc, Input, Output, State, callback
import dash_bootstrap_components as dbc
from dotenv import load_dotenv
from src.loaders.distrito_loader import get_distrito_manager

load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

NEON_URL = os.getenv("NEON_DATABASE_URL")
if not NEON_URL:
    raise ValueError("NEON_DATABASE_URL not found in environment")

# Performance constants
MAP_POINT_LIMIT = 10000
SCATTER_POINT_LIMIT = 5000
QUERY_TIMEOUT_MS = 30000
CONNECTION_POOL_SIZE = 5

# Color mapping for operators
OPERATOR_COLORS = {
    'Entel': '#0066CC',
    'Tigo': '#00AA00',
    'Viva': '#CC0000',
    'Sin Senal': '#999999',
    'Desconocido': '#666666'
}

# ============================================================================
# CONNECTION POOL (psycopg2)
# ============================================================================
_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def get_connection_pool() -> pool.ThreadedConnectionPool:
    """Get or create connection pool"""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=CONNECTION_POOL_SIZE,
            dsn=NEON_URL
        )
        logger.info(f"Connection pool created (max={CONNECTION_POOL_SIZE})")
    return _connection_pool


@contextmanager
def get_connection():
    """Context manager for database connections"""
    pool_inst = get_connection_pool()
    conn = pool_inst.getconn()
    try:
        yield conn
    finally:
        pool_inst.putconn(conn)


def execute_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Execute query and return DataFrame"""
    with get_connection() as conn:
        return pd.read_sql_query(query, conn, params=params)


def execute_scalar(query: str, params: tuple = None) -> Any:
    """Execute query and return single value"""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        result = cur.fetchone()
        return result[0] if result else None


# ============================================================================
# DATA QUERIES (Optimized with LIMIT and server-side aggregation)
# ============================================================================
def fetch_kpis(filters: Dict = None) -> Dict[str, Any]:
    """Fetch KPI summary statistics"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT 
        COUNT(*) as total_mediciones,
        COUNT(DISTINCT f.dispositivo_id) as dispositivos_unicos,
        ROUND(AVG(f.bateria)::numeric, 1) as bateria_promedio,
        ROUND(AVG(f.senal_dbm)::numeric, 0) as senal_promedio,
        ROUND(AVG(f.velocidad)::numeric, 1) as velocidad_promedio,
        ROUND(AVG(COALESCE(u.altitud, 0))::numeric, 0) as altitud_promedio
    FROM FACT_MEDICIONES f
    LEFT JOIN DIM_UBICACION u ON f.ubicacion_id = u.ubicacion_id
    LEFT JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    """
    
    df = execute_query(query)
    if df.empty:
        return {
            'total_mediciones': 0,
            'dispositivos_unicos': 0,
            'bateria_promedio': 0,
            'senal_promedio': 0,
            'velocidad_promedio': 0,
            'altitud_promedio': 0
        }
    
    return df.iloc[0].to_dict()


def fetch_operator_distribution(filters: Dict = None) -> pd.DataFrame:
    """Fetch records by operator"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT o.nombre as operador, COUNT(*) as count
    FROM FACT_MEDICIONES f
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    GROUP BY o.nombre
    ORDER BY count DESC
    """
    return execute_query(query)


def fetch_zone_distribution(filters: Dict = None) -> pd.DataFrame:
    """Fetch records by zone"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT z.nombre as zona, COUNT(*) as count
    FROM FACT_MEDICIONES f
    JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    LEFT JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    {where_clause}
    GROUP BY z.nombre
    ORDER BY count DESC
    LIMIT 15
    """
    return execute_query(query)


def fetch_hourly_pattern(filters: Dict = None) -> pd.DataFrame:
    """Fetch hourly activity pattern"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT h.hora, COUNT(*) as count
    FROM FACT_MEDICIONES f
    JOIN DIM_HORA h ON f.hora_id = h.hora_id
    LEFT JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    GROUP BY h.hora
    ORDER BY h.hora
    """
    return execute_query(query)


def fetch_quality_distribution(filters: Dict = None) -> pd.DataFrame:
    """Fetch quality by operator"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT c.nivel as calidad, o.nombre as operador, COUNT(*) as count
    FROM FACT_MEDICIONES f
    JOIN DIM_CALIDAD c ON f.calidad_id = c.calidad_id
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    GROUP BY c.nivel, o.nombre
    ORDER BY count DESC
    """
    return execute_query(query)


def fetch_speed_altitude(filters: Dict = None) -> pd.DataFrame:
    """Fetch speed vs altitude for scatter plot"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT f.velocidad as velocidad_kmh, u.altitud, o.nombre as operador
    FROM FACT_MEDICIONES f
    JOIN DIM_UBICACION u ON f.ubicacion_id = u.ubicacion_id
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    AND f.velocidad > 0
    LIMIT {SCATTER_POINT_LIMIT}
    """
    return execute_query(query)


def fetch_heatmap_data(filters: Dict = None) -> pd.DataFrame:
    """Fetch heatmap data (hour vs day)"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT h.hora, t.nombre_dia, t.dia_semana, COUNT(*) as count
    FROM FACT_MEDICIONES f
    JOIN DIM_HORA h ON f.hora_id = h.hora_id
    JOIN DIM_TIEMPO t ON f.tiempo_id = t.tiempo_id
    LEFT JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    GROUP BY h.hora, t.nombre_dia, t.dia_semana
    ORDER BY t.dia_semana, h.hora
    """
    return execute_query(query)


def fetch_map_data(filters: Dict = None) -> pd.DataFrame:
    """Fetch location data for map with clustering"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT 
        u.latitud, u.longitud, o.nombre as operador_nombre,
        d.nombre_dispositivo as dispositivo_nombre,
        f.bateria as medida_bateria, f.senal_dbm as medida_senal, f.velocidad as velocidad_kmh,
        f.timestamp_original, z.nombre as zona_nombre, r.tipo_red as red_tipo
    FROM FACT_MEDICIONES f
    JOIN DIM_UBICACION u ON f.ubicacion_id = u.ubicacion_id
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_RED r ON f.red_id = r.red_id
    LEFT JOIN DIM_DISPOSITIVO d ON f.dispositivo_id = d.dispositivo_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    ORDER BY f.timestamp_original DESC
    LIMIT {MAP_POINT_LIMIT}
    """
    return execute_query(query)


def fetch_anomalies(filters: Dict = None) -> pd.DataFrame:
    """Fetch anomaly records (high speed, low battery, etc)"""
    where_clause = build_where_clause(filters)
    
    query = f"""
    SELECT 
        f.medicion_id, t.fecha, h.hora, o.nombre as operador,
        d.nombre_dispositivo as dispositivo, f.velocidad,
        f.bateria, f.senal_dbm,
        CASE 
            WHEN f.velocidad > 120 THEN 'Velocidad Alta'
            WHEN f.bateria < 10 THEN 'Bateria Critica'
            WHEN f.senal_dbm < -100 THEN 'Senal Debil'
            ELSE 'Normal'
        END as tipo_anomalia
    FROM FACT_MEDICIONES f
    JOIN DIM_TIEMPO t ON f.tiempo_id = t.tiempo_id
    JOIN DIM_HORA h ON f.hora_id = h.hora_id
    JOIN DIM_OPERADOR o ON f.operador_id = o.operador_id
    LEFT JOIN DIM_DISPOSITIVO d ON f.dispositivo_id = d.dispositivo_id
    LEFT JOIN DIM_ZONAS z ON f.zona_id = z.zona_id
    {where_clause}
    AND (f.velocidad > 120 OR f.bateria < 10 OR f.senal_dbm < -100)
    ORDER BY t.fecha DESC, h.hora DESC
    LIMIT 100
    """
    return execute_query(query)


def fetch_filter_options() -> Dict[str, List]:
    """Fetch options for filter dropdowns"""
    operators = execute_query("SELECT DISTINCT nombre FROM DIM_OPERADOR ORDER BY nombre")
    zones = execute_query("SELECT DISTINCT nombre FROM DIM_ZONAS ORDER BY nombre")
    
    return {
        'operators': operators['nombre'].tolist() if not operators.empty else [],
        'zones': zones['nombre'].tolist() if not zones.empty else []
    }


def build_where_clause(filters: Dict = None) -> str:
    """Build WHERE clause from filters"""
    if not filters:
        return "WHERE 1=1"
    
    conditions = ["1=1"]
    
    if filters.get('operators'):
        ops = ", ".join([f"'{o}'" for o in filters['operators']])
        conditions.append(f"o.nombre IN ({ops})")
    
    if filters.get('zones'):
        zones = ", ".join([f"'{z}'" for z in filters['zones']])
        conditions.append(f"z.nombre IN ({zones})")
    
    if filters.get('hora_min') is not None and filters.get('hora_max') is not None:
        conditions.append(f"f.hora_id BETWEEN {filters['hora_min']} AND {filters['hora_max']}")
    
    if filters.get('bateria_min') is not None and filters.get('bateria_max') is not None:
        conditions.append(f"f.bateria BETWEEN {filters['bateria_min']} AND {filters['bateria_max']}")
    
    if filters.get('velocidad_min') is not None and filters.get('velocidad_max') is not None:
        conditions.append(f"f.velocidad BETWEEN {filters['velocidad_min']} AND {filters['velocidad_max']}")
    
    return "WHERE " + " AND ".join(conditions)


# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_custom_folium_map(df_filtered):
    """
    Create customized Folium map without clustering.
    Uses real district boundaries and custom styling.
    """
    # Get distrito manager with error handling
    try:
        distrito_mgr = get_distrito_manager()
        logger.info("Using real district boundaries for map")
    except Exception as e:
        logger.warning(f"Failed to load district boundaries: {e}. Using map without district overlays.")
        distrito_mgr = None
    
    # Get bounds from distrito manager with better fallback
    try:
        if distrito_mgr:
            bounds = distrito_mgr.get_bounds()
            center_lat = (bounds[1] + bounds[3]) / 2
            center_lon = (bounds[0] + bounds[2]) / 2
            # Calculate appropriate zoom based on bounds
            lat_diff = bounds[3] - bounds[1]
            lon_diff = bounds[2] - bounds[0]
            zoom = 12 if lat_diff < 0.2 and lon_diff < 0.2 else 11
        else:
            raise Exception("No district manager")
    except:
        # Fallback to Santa Cruz center
        center_lat, center_lon = -17.7833, -63.1822
        zoom = 12
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles='OpenStreetMap',  # Use default tiles as fallback
        control_scale=True,
        prefer_canvas=True  # Better performance for many markers
    )
    
    # Add custom tile layers
    folium.TileLayer(
        tiles='https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png',
        attr='CartoDB.DarkMatter',
        name='Dark Mode',
        overlay=False,
        control=True
    ).add_to(m)
    
    folium.TileLayer(
        tiles='https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png',
        attr='CartoDB.Positron',
        name='Light Mode',
        overlay=False,
        control=True
    ).add_to(m)
    
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Satellite',
        overlay=False,
        control=True
    ).add_to(m)
    
    # Add district boundaries as overlays
    if distrito_mgr:
        add_district_boundaries(m, distrito_mgr)
    
    # Add markers WITH clustering (no sampling)
    add_markers_with_cluster(m, df_filtered)
    
    # Add layer control
    folium.LayerControl().add_to(m)
    
    # Add custom legend
    add_custom_legend(m)
    
    return m


def add_district_boundaries(m, distrito_mgr):
    """Add district polygon boundaries to map."""
    try:
        distritos = distrito_mgr.get_all_distritos()
        
        # Create feature group for districts
        distrito_layer = folium.FeatureGroup(name='Distritos', show=True)
        
        for distrito in distritos:
            polygon = distrito.get('polygon')
            if not polygon:
                continue
                
            # Convert Shapely polygon to GeoJSON format
            geojson = {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [list(polygon.exterior.coords)]
                },
                "properties": {
                    "name": distrito['nombre'],
                    "tipo": distrito['tipo']
                }
            }
            
            folium.GeoJson(
                geojson,
                style_function=lambda feature: {
                    'fillColor': '#3388ff',
                    'color': '#ffffff',
                    'weight': 2,
                    'fillOpacity': 0.1
                },
                highlight_function=lambda feature: {
                    'fillColor': '#ffff00',
                    'fillOpacity': 0.3
                },
                tooltip=folium.Tooltip(
                    f"<b>{distrito['nombre']}</b><br>Tipo: {distrito['tipo']}"
                )
            ).add_to(distrito_layer)
        
        distrito_layer.add_to(m)
        
    except Exception as e:
        logger.warning(f"Failed to add district boundaries: {e}")


def add_markers_with_cluster(m, df):
    """
    Add markers WITH MarkerCluster for optimal performance.
    Shows ALL data points without sampling.
    """
    if df.empty:
        return
    
    logger.info(f"Adding {len(df)} markers with MarkerCluster (no sampling)")
    
    # Create marker cluster for optimal performance
    marker_cluster = MarkerCluster(
        name='Mediciones',
        show=True,
        options={
            'spiderfyOnMaxZoom': True,
            'showCoverageOnHover': True,
            'zoomToBoundsOnClick': True,
            'maxClusterRadius': 80,
            'chunkedLoading': True
        }
    ).add_to(m)
    
    # Color mapping by operator
    operator_colors = {
        'ENTEL': '#0066cc',
        'TIGO': '#00cc66',
        'VIVA': '#cc0000',
        'SIN SEÑAL': '#999999',
        'DESCONOCIDO': '#666666'
    }
    
    # Add ALL markers to cluster (no sampling)
    for idx, row in df.iterrows():
        color = operator_colors.get(row['operador_nombre'], '#666666')
        
        # Custom icon based on signal quality
        if row.get('medida_senal', -100) > -70:
            icon_color = 'green'
        elif row.get('medida_senal', -100) > -90:
            icon_color = 'orange'
        else:
            icon_color = 'red'
        
        popup_html = f"""
        <div style="font-family: Arial; width: 200px;">
            <h4 style="margin: 0 0 10px 0; color: {color};">
                {row.get('operador_nombre', 'N/A')}
            </h4>
            <table style="width: 100%; font-size: 12px;">
                <tr><td><b>Dispositivo:</b></td><td>{row.get('dispositivo_nombre', 'N/A')}</td></tr>
                <tr><td><b>Velocidad:</b></td><td>{row.get('velocidad_kmh', 0):.1f} km/h</td></tr>
                <tr><td><b>Batería:</b></td><td>{row.get('medida_bateria', 0):.0f}%</td></tr>
                <tr><td><b>Señal:</b></td><td>{row.get('medida_senal', -999):.0f} dBm</td></tr>
                <tr><td><b>Red:</b></td><td>{row.get('red_tipo', 'N/A')}</td></tr>
                <tr><td><b>Distrito:</b></td><td>{row.get('zona_nombre', 'N/A')}</td></tr>
            </table>
        </div>
        """
        
        folium.CircleMarker(
            location=[row['latitud'], row['longitud']],
            radius=4,
            popup=folium.Popup(popup_html, max_width=250),
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            weight=1
        ).add_to(marker_cluster)


def add_custom_legend(m):
    """Add custom legend to map."""
    legend_html = '''
    <div style="position: fixed; 
                bottom: 50px; right: 50px; width: 200px; 
                background-color: white; z-index:9999; 
                border:2px solid grey; border-radius: 5px;
                padding: 10px; font-size: 12px;">
        <h4 style="margin: 0 0 10px 0;">Operadores</h4>
        <p><span style="color: #0066cc;">●</span> Entel</p>
        <p><span style="color: #00cc66;">●</span> Tigo</p>
        <p><span style="color: #cc0000;">●</span> Viva</p>
        <p><span style="color: #999999;">●</span> Sin Señal</p>
        <h4 style="margin: 10px 0;">Calidad Señal</h4>
        <p><span style="color: green;">●</span> Excelente (&gt; -70 dBm)</p>
        <p><span style="color: orange;">●</span> Regular (-70 a -90 dBm)</p>
        <p><span style="color: red;">●</span> Mala (&lt; -90 dBm)</p>
    </div>
    '''
    m.get_root().html.add_child(folium.Element(legend_html))


def render_folium_in_dash(map_obj):
    """Render Folium map in Dash."""
    map_html = map_obj._repr_html_()
    
    return map_html


def create_operator_chart(df: pd.DataFrame) -> go.Figure:
    """Create horizontal bar chart for operator distribution"""
    if df.empty:
        return go.Figure()
    
    colors = [OPERATOR_COLORS.get(op, '#666666') for op in df['operador']]
    
    fig = px.bar(
        df,
        x='count',
        y='operador',
        orientation='h',
        title='Distribucion por Operador',
        labels={'count': 'Registros', 'operador': 'Operador'},
        color='operador',
        color_discrete_map=OPERATOR_COLORS
    )
    fig.update_layout(showlegend=False, height=300)
    return fig


def create_zone_chart(df: pd.DataFrame) -> go.Figure:
    """Create bar chart for zone distribution"""
    if df.empty:
        return go.Figure()
    
    fig = px.bar(
        df,
        x='zona',
        y='count',
        title='Distribucion por Zona (Top 15)',
        labels={'count': 'Registros', 'zona': 'Zona'},
        color='count',
        color_continuous_scale='Viridis'
    )
    fig.update_layout(showlegend=False, height=350, xaxis_tickangle=-45)
    return fig


def create_hourly_chart(df: pd.DataFrame) -> go.Figure:
    """Create line chart for hourly pattern"""
    if df.empty:
        return go.Figure()
    
    fig = px.line(
        df,
        x='hora',
        y='count',
        title='Patron de Actividad por Hora',
        labels={'count': 'Registros', 'hora': 'Hora del Dia'},
        markers=True
    )
    fig.update_layout(height=300)
    fig.update_xaxes(tickmode='linear', tick0=0, dtick=2)
    return fig


def create_quality_chart(df: pd.DataFrame) -> go.Figure:
    """Create stacked bar chart for quality distribution"""
    if df.empty:
        return go.Figure()
    
    fig = px.bar(
        df,
        x='calidad',
        y='count',
        color='operador',
        title='Distribucion de Calidad por Operador',
        labels={'count': 'Registros', 'calidad': 'Calidad', 'operador': 'Operador'},
        color_discrete_map=OPERATOR_COLORS,
        barmode='stack'
    )
    fig.update_layout(height=350)
    return fig


def create_scatter_chart(df: pd.DataFrame) -> go.Figure:
    """Create scatter plot for speed vs altitude"""
    if df.empty:
        return go.Figure()
    
    fig = px.scatter(
        df,
        x='altitud',
        y='velocidad_kmh',
        color='operador',
        title='Velocidad vs Altitud',
        labels={'velocidad_kmh': 'Velocidad (km/h)', 'altitud': 'Altitud (m)', 'operador': 'Operador'},
        color_discrete_map=OPERATOR_COLORS,
        opacity=0.6
    )
    fig.update_layout(height=350)
    return fig


def create_heatmap_chart(df: pd.DataFrame) -> go.Figure:
    """Create heatmap for hour vs day"""
    if df.empty:
        return go.Figure()
    
    # Pivot data
    pivot = df.pivot_table(
        values='count',
        index='nombre_dia',
        columns='hora',
        fill_value=0
    )
    
    # Reorder days
    day_order = ['Lunes', 'Martes', 'Miercoles', 'Jueves', 'Viernes', 'Sabado', 'Domingo']
    pivot = pivot.reindex([d for d in day_order if d in pivot.index])
    
    fig = px.imshow(
        pivot,
        title='Heatmap de Actividad (Hora vs Dia)',
        labels={'x': 'Hora', 'y': 'Dia', 'color': 'Registros'},
        color_continuous_scale='YlOrRd',
        aspect='auto'
    )
    fig.update_layout(height=350)
    return fig


# ============================================================================
# DASH APP
# ============================================================================
app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.FLATLY,
        dbc.icons.BOOTSTRAP
    ],
    suppress_callback_exceptions=True
)

app.title = "Mobile Tracking Dashboard - Advanced"

# Load initial filter options
logger.info("Loading filter options...")
filter_options = fetch_filter_options()

# ============================================================================
# LAYOUT
# ============================================================================
# Sidebar with filters
sidebar = dbc.Card([
    dbc.CardHeader([
        html.I(className="bi bi-funnel me-2"),
        "Filtros"
    ]),
    dbc.CardBody([
        # Operator filter
        html.Label("Operador", className="fw-bold"),
        dcc.Dropdown(
            id='filter-operator',
            options=[{'label': op, 'value': op} for op in filter_options['operators']],
            multi=True,
            placeholder="Todos los operadores"
        ),
        html.Hr(),
        
        # Zone filter
        html.Label("Zona/Distrito", className="fw-bold"),
        dcc.Dropdown(
            id='filter-zone',
            options=[{'label': z, 'value': z} for z in filter_options['zones']],
            multi=True,
            placeholder="Todas las zonas"
        ),
        html.Hr(),
        
        # Hour range
        html.Label("Rango Horario", className="fw-bold"),
        dcc.RangeSlider(
            id='filter-hora',
            min=0,
            max=23,
            step=1,
            value=[0, 23],
            marks={i: str(i) for i in range(0, 24, 4)},
            tooltip={'placement': 'bottom', 'always_visible': False},
            updatemode='mouseup'
        ),
        html.Hr(),
        
        # Battery range
        html.Label("Bateria (%)", className="fw-bold"),
        dcc.RangeSlider(
            id='filter-bateria',
            min=0,
            max=100,
            step=5,
            value=[0, 100],
            marks={i: str(i) for i in range(0, 101, 25)},
            tooltip={'placement': 'bottom', 'always_visible': False},
            updatemode='mouseup'
        ),
        html.Hr(),
        
        # Speed range
        html.Label("Velocidad (km/h)", className="fw-bold"),
        dcc.RangeSlider(
            id='filter-velocidad',
            min=0,
            max=120,
            step=10,
            value=[0, 120],
            marks={i: str(i) for i in range(0, 121, 30)},
            tooltip={'placement': 'bottom', 'always_visible': False},
            updatemode='mouseup'
        ),
        html.Hr(),
        
        # Apply button
        dbc.Button(
            [html.I(className="bi bi-check-circle me-2"), "Aplicar Filtros"],
            id='btn-apply-filters',
            color='primary',
            className='w-100'
        ),
        
        # Loading indicator
        dbc.Spinner(html.Div(id='loading-output'), color='primary', size='sm')
    ])
], className="h-100")

# KPI Cards
def create_kpi_cards(kpis: Dict) -> dbc.Row:
    return dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.I(className="bi bi-database-fill text-primary", style={"fontSize": "1.8rem"}),
                    html.H3(f"{kpis.get('total_mediciones', 0):,}", className="text-primary mb-0 mt-1"),
                    html.P("Total Mediciones", className="text-muted small mb-0")
                ])
            ], className="text-center h-100 shadow-sm")
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.I(className="bi bi-phone-fill text-success", style={"fontSize": "1.8rem"}),
                    html.H3(f"{kpis.get('dispositivos_unicos', 0)}", className="text-success mb-0 mt-1"),
                    html.P("Dispositivos", className="text-muted small mb-0")
                ])
            ], className="text-center h-100 shadow-sm")
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.I(className="bi bi-battery-half text-warning", style={"fontSize": "1.8rem"}),
                    html.H3(f"{kpis.get('bateria_promedio', 0):.1f}%", className="text-warning mb-0 mt-1"),
                    html.P("Bateria Prom.", className="text-muted small mb-0")
                ])
            ], className="text-center h-100 shadow-sm")
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.I(className="bi bi-reception-4 text-info", style={"fontSize": "1.8rem"}),
                    html.H3(f"{kpis.get('senal_promedio', 0):.0f}", className="text-info mb-0 mt-1"),
                    html.P("Senal (dBm)", className="text-muted small mb-0")
                ])
            ], className="text-center h-100 shadow-sm")
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.I(className="bi bi-speedometer text-danger", style={"fontSize": "1.8rem"}),
                    html.H3(f"{kpis.get('velocidad_promedio', 0):.1f}", className="text-danger mb-0 mt-1"),
                    html.P("Velocidad (km/h)", className="text-muted small mb-0")
                ])
            ], className="text-center h-100 shadow-sm")
        ], width=2),
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.I(className="bi bi-geo-alt-fill text-secondary", style={"fontSize": "1.8rem"}),
                    html.H3(f"{kpis.get('altitud_promedio', 0):.0f}m", className="text-secondary mb-0 mt-1"),
                    html.P("Altitud Prom.", className="text-muted small mb-0")
                ])
            ], className="text-center h-100 shadow-sm")
        ], width=2),
    ], className="g-2 mb-3")

# Main layout
app.layout = dbc.Container([
    # Header
    dbc.Navbar([
        dbc.Container([
            dbc.Row([
                dbc.Col([
                    html.I(className="bi bi-broadcast-pin me-2", style={"fontSize": "1.5rem"}),
                    dbc.NavbarBrand("Mobile Tracking Dashboard - Advanced", className="fs-4 fw-bold"),
                ], className="d-flex align-items-center"),
            ]),
        ], fluid=True),
    ], color="primary", dark=True, className="mb-3"),
    
    # Main content
    dbc.Row([
        # Sidebar
        dbc.Col([sidebar], width=2),
        
        # Main area
        dbc.Col([
            # KPIs
            html.Div(id='kpi-cards'),
            
            # Map section
            dbc.Card([
                dbc.CardHeader([
                    html.I(className="bi bi-map me-2"),
                    "Mapa de Ubicaciones (Folium + MarkerCluster)"
                ]),
                dbc.CardBody([
                    html.Iframe(
                        id='folium-map',
                        srcDoc='',
                        style={'width': '100%', 'height': '500px', 'border': 'none'}
                    )
                ])
            ], className="shadow-sm mb-3"),
            
            # Charts row 1
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([html.I(className="bi bi-bar-chart me-2"), "Por Operador"]),
                        dbc.CardBody([dcc.Graph(id='chart-operator')])
                    ], className="shadow-sm")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([html.I(className="bi bi-geo me-2"), "Por Zona"]),
                        dbc.CardBody([dcc.Graph(id='chart-zone')])
                    ], className="shadow-sm")
                ], width=6),
            ], className="g-3 mb-3"),
            
            # Charts row 2
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([html.I(className="bi bi-clock me-2"), "Patron Horario"]),
                        dbc.CardBody([dcc.Graph(id='chart-hourly')])
                    ], className="shadow-sm")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([html.I(className="bi bi-signal me-2"), "Calidad por Operador"]),
                        dbc.CardBody([dcc.Graph(id='chart-quality')])
                    ], className="shadow-sm")
                ], width=6),
            ], className="g-3 mb-3"),
            
            # Charts row 3
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([html.I(className="bi bi-scatter-chart me-2"), "Velocidad vs Altitud"]),
                        dbc.CardBody([dcc.Graph(id='chart-scatter')])
                    ], className="shadow-sm")
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader([html.I(className="bi bi-grid-3x3 me-2"), "Heatmap Actividad"]),
                        dbc.CardBody([dcc.Graph(id='chart-heatmap')])
                    ], className="shadow-sm")
                ], width=6),
            ], className="g-3 mb-3"),
            
            # Anomalies table
            dbc.Card([
                dbc.CardHeader([
                    html.I(className="bi bi-exclamation-triangle me-2"),
                    "Tabla de Anomalias"
                ]),
                dbc.CardBody([
                    html.Div(id='anomalies-table')
                ])
            ], className="shadow-sm mb-3"),
            
        ], width=10),
    ]),
    
    # Store for caching
    dcc.Store(id='store-filters'),
    
    # Footer
    html.Footer([
        html.Hr(),
        html.P([
            html.I(className="bi bi-graph-up me-2"),
            "Mobile Tracking Dashboard - Advanced",
            html.Span(" | ", className="mx-2"),
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            html.Span(" | ", className="mx-2"),
            "Stack: Dash + Plotly + Folium + psycopg2"
        ], className="text-center text-muted small")
    ])
    
], fluid=True, className="bg-light")


# ============================================================================
# CALLBACKS
# ============================================================================
@callback(
    [
        Output('kpi-cards', 'children'),
        Output('folium-map', 'srcDoc'),
        Output('chart-operator', 'figure'),
        Output('chart-zone', 'figure'),
        Output('chart-hourly', 'figure'),
        Output('chart-quality', 'figure'),
        Output('chart-scatter', 'figure'),
        Output('chart-heatmap', 'figure'),
        Output('anomalies-table', 'children'),
        Output('loading-output', 'children'),
    ],
    [Input('btn-apply-filters', 'n_clicks')],
    [
        State('filter-operator', 'value'),
        State('filter-zone', 'value'),
        State('filter-hora', 'value'),
        State('filter-bateria', 'value'),
        State('filter-velocidad', 'value'),
    ],
    prevent_initial_call=False
)
def update_dashboard(n_clicks, operators, zones, hora_range, bateria_range, velocidad_range):
    """Master callback to update all dashboard components"""
    start_time = datetime.now()
    
    # Build filters dict
    filters = {}
    if operators:
        filters['operators'] = operators
    if zones:
        filters['zones'] = zones
    if hora_range:
        filters['hora_min'] = hora_range[0]
        filters['hora_max'] = hora_range[1]
    if bateria_range:
        filters['bateria_min'] = bateria_range[0]
        filters['bateria_max'] = bateria_range[1]
    if velocidad_range:
        filters['velocidad_min'] = velocidad_range[0]
        filters['velocidad_max'] = velocidad_range[1]
    
    logger.info(f"Updating dashboard with filters: {filters}")
    
    # Fetch all data
    kpis = fetch_kpis(filters)
    df_operators = fetch_operator_distribution(filters)
    df_zones = fetch_zone_distribution(filters)
    df_hourly = fetch_hourly_pattern(filters)
    df_quality = fetch_quality_distribution(filters)
    df_scatter = fetch_speed_altitude(filters)
    df_heatmap = fetch_heatmap_data(filters)
    df_map = fetch_map_data(filters)
    df_anomalies = fetch_anomalies(filters)
    
    # Create visualizations
    kpi_cards = create_kpi_cards(kpis)
    map_obj = create_custom_folium_map(df_map)
    map_html = render_folium_in_dash(map_obj)
    fig_operator = create_operator_chart(df_operators)
    fig_zone = create_zone_chart(df_zones)
    fig_hourly = create_hourly_chart(df_hourly)
    fig_quality = create_quality_chart(df_quality)
    fig_scatter = create_scatter_chart(df_scatter)
    fig_heatmap = create_heatmap_chart(df_heatmap)
    
    # Anomalies table
    if not df_anomalies.empty:
        anomalies_table = dbc.Table.from_dataframe(
            df_anomalies,
            striped=True,
            bordered=True,
            hover=True,
            responsive=True,
            className="table-sm"
        )
    else:
        anomalies_table = html.P("No se encontraron anomalias", className="text-muted")
    
    elapsed = (datetime.now() - start_time).total_seconds()
    loading_msg = f"Actualizado en {elapsed:.2f}s"
    logger.info(loading_msg)
    
    return (
        kpi_cards,
        map_html,
        fig_operator,
        fig_zone,
        fig_hourly,
        fig_quality,
        fig_scatter,
        fig_heatmap,
        anomalies_table,
        loading_msg
    )


# ============================================================================
# RUN SERVER
# ============================================================================
if __name__ == '__main__':
    logger.info("=" * 60)
    logger.info("STARTING ADVANCED DASHBOARD")
    logger.info("=" * 60)
    logger.info("Open browser at: http://127.0.0.1:8050")
    logger.info("Press Ctrl+C to stop")
    
    app.run(debug=False, host='127.0.0.1', port=8050)
