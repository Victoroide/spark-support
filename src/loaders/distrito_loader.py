"""
Load and process Santa Cruz district/municipality data from GeoJSON.
Provides geometric operations for point-in-polygon classification.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

# Try to import shapely, use fallback if not available
try:
    from shapely.geometry import Point, Polygon, shape
    from shapely.prepared import prep
    SHAPELY_AVAILABLE = True
except ImportError:
    logger.warning("Shapely not available, using simplified point-in-polygon")
    SHAPELY_AVAILABLE = False


class DistritoManager:
    """Manage Santa Cruz district/municipality geometries."""
    
    def __init__(self, json_path: str = "distrito_municipal_santacruz.json"):
        self.json_path = Path(json_path)
        self.distritos = {}
        self.prepared_polygons = {}
        self.load_distritos()
    
    def load_distritos(self):
        """Load district data from GeoJSON."""
        if not self.json_path.exists():
            logger.error(f"District JSON not found: {self.json_path}")
            raise FileNotFoundError(f"Required file not found: {self.json_path}")
        
        with open(self.json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # GeoJSON FeatureCollection format
        if 'features' in data:
            for feature in data['features']:
                self._process_feature(feature)
        else:
            logger.error(f"Unknown JSON structure: expecting GeoJSON FeatureCollection")
            raise ValueError("Unable to parse district JSON structure")
        
        # Prepare polygons for fast point-in-polygon queries
        if SHAPELY_AVAILABLE:
            for distrito_id, info in self.distritos.items():
                if 'polygon' in info:
                    self.prepared_polygons[distrito_id] = prep(info['polygon'])
        
        logger.info(f"Loaded {len(self.distritos)} districts from {self.json_path}")
    
    def _format_district_id(self, distrito_id):
        """Format internal district ID to readable format."""
        if '.' in str(distrito_id):
            # Extract number after last dot: distrito_municipal_santacruz.1 -> 1
            number = str(distrito_id).split('.')[-1]
            return f"Distrito {number}"
        return str(distrito_id)

    def _process_feature(self, feature):
        """Process GeoJSON feature."""
        properties = feature.get('properties', {})
        geometry = feature.get('geometry', {})
        
        # Extract ID from feature.id or properties
        distrito_id = (
            feature.get('id') or
            properties.get('id') or 
            properties.get('distrito_id') or 
            properties.get('OBJECTID') or
            len(self.distritos) + 1
        )
        
        # Extract name from properties with dynamic formatting
        nombre = (
            properties.get('nombre') or
            properties.get('NOMBRE') or
            properties.get('name') or
            properties.get('DISTRITO') or
            self._format_district_id(distrito_id)
        )
        
        # Extract type
        tipo = (
            properties.get('tipo') or
            properties.get('type') or
            properties.get('TIPO') or
            'DISTRITO'
        )
        
        # Create polygon from geometry
        if SHAPELY_AVAILABLE:
            polygon = shape(geometry)
            
            # Handle MultiPolygon by taking the first polygon
            if geometry.get('type') == 'MultiPolygon':
                # MultiPolygon coordinates: [[[polygon1]], [[polygon2]], ...]
                coords = geometry['coordinates']
                if coords and len(coords) > 0:
                    # Take the first polygon from the MultiPolygon
                    first_polygon_coords = coords[0][0] if len(coords[0]) > 0 else coords[0]
                    polygon = Polygon(first_polygon_coords)
            
            self.distritos[distrito_id] = {
                'id': distrito_id,
                'nombre': nombre,
                'tipo': tipo,
                'polygon': polygon,
                'bounds': polygon.bounds,  # (minx, miny, maxx, maxy)
                'centroid': polygon.centroid.coords,
                'properties': properties
            }
        else:
            # Fallback without shapely - store raw coordinates
            self.distritos[distrito_id] = {
                'id': distrito_id,
                'nombre': nombre,
                'tipo': tipo,
                'geometry': geometry,
                'properties': properties
            }
    
    def classify_point(self, lat: float, lon: float) -> Optional[Dict]:
        """
        Classify point into district.
        Returns: distrito dict or None if not found
        """
        if SHAPELY_AVAILABLE:
            point = Point(lon, lat)  # Shapely uses (lon, lat) order
            
            for distrito_id, prepared in self.prepared_polygons.items():
                if prepared.contains(point):
                    return self.distritos[distrito_id]
            
            # Fallback: find nearest district if not inside any
            return self._find_nearest_distrito(lat, lon)
        else:
            # Simple bounding box check as fallback
            return self._classify_point_bbox(lat, lon)
    
    def _find_nearest_distrito(self, lat: float, lon: float) -> Optional[Dict]:
        """Find nearest district to point (fallback)."""
        if not SHAPELY_AVAILABLE:
            return None
            
        point = Point(lon, lat)
        
        min_distance = float('inf')
        nearest = None
        
        for distrito_id, info in self.distritos.items():
            distance = point.distance(info['polygon'])
            if distance < min_distance:
                min_distance = distance
                nearest = info
        
        return nearest
    
    def _classify_point_bbox(self, lat: float, lon: float) -> Optional[Dict]:
        """Simple bounding box classification (fallback without shapely)."""
        for distrito in self.distritos.values():
            geometry = distrito.get('geometry')
            if not geometry:
                continue
                
            # Simple check: if point is within bounding box of coordinates
            coords = geometry.get('coordinates', [])
            if not coords:
                continue
                
            # For MultiPolygon, take first polygon
            if geometry.get('type') == 'MultiPolygon':
                if coords and len(coords) > 0:
                    coords = coords[0]
            
            # Extract min/max bounds
            flat_coords = []
            if isinstance(coords, list) and len(coords) > 0:
                flat_coords = coords[0] if isinstance(coords[0], list) else coords
            
            if not flat_coords:
                continue
                
            lons = [c[0] for c in flat_coords]
            lats = [c[1] for c in flat_coords]
            
            if (min(lons) <= lon <= max(lons) and min(lats) <= lat <= max(lats)):
                return distrito
        
        return None
    
    def get_distrito_by_id(self, distrito_id) -> Optional[Dict]:
        """Get distrito by ID."""
        return self.distritos.get(distrito_id)
    
    def get_distrito_by_name(self, nombre: str) -> Optional[Dict]:
        """Get distrito by name (case-insensitive)."""
        nombre_lower = nombre.lower()
        for distrito in self.distritos.values():
            if distrito['nombre'].lower() == nombre_lower:
                return distrito
        return None
    
    def get_all_distritos(self) -> List[Dict]:
        """Get all distritos as list."""
        return list(self.distritos.values())
    
    def get_bounds(self) -> Tuple[float, float, float, float]:
        """Get bounding box of all districts (minx, miny, maxx, maxy)."""
        if not SHAPELY_AVAILABLE:
            # Default Santa Cruz bounds
            return (-63.2, -17.9, -63.1, -17.7)
            
        all_bounds = [d['bounds'] for d in self.distritos.values()]
        
        min_lon = min(b[0] for b in all_bounds)
        min_lat = min(b[1] for b in all_bounds)
        max_lon = max(b[2] for b in all_bounds)
        max_lat = max(b[3] for b in all_bounds)
        
        return min_lon, min_lat, max_lon, max_lat


# Singleton instance
_DISTRITO_MGR = None

def get_distrito_manager() -> DistritoManager:
    """Singleton for distrito manager."""
    global _DISTRITO_MGR
    if _DISTRITO_MGR is None:
        _DISTRITO_MGR = DistritoManager()
    return _DISTRITO_MGR
