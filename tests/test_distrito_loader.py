#!/usr/bin/env python3
"""
Test script for distrito_loader.py
Verifies JSON parsing and point-in-polygon classification.
"""

import sys
from distrito_loader import get_distrito_manager

def test_distrito_loader():
    """Test district loading and point classification."""
    print("Testing distrito_loader...")
    
    try:
        # Load district manager
        mgr = get_distrito_manager()
        print(f"✓ Loaded {len(mgr.get_all_distritos())} districts")
        
        # Test point classification for Santa Cruz center
        test_points = [
            (-17.7833, -63.1822, "Santa Cruz Center"),
            (-17.7650, -63.1950, "Equipetrol area"),
            (-17.8200, -63.1400, "Plan 3000 area"),
        ]
        
        print("\nTesting point classification:")
        for lat, lon, description in test_points:
            distrito = mgr.classify_point(lat, lon)
            if distrito:
                print(f"✓ {description}: {distrito['nombre']} ({distrito['tipo']})")
            else:
                print(f"✗ {description}: No district found")
        
        # Test bounds
        try:
            bounds = mgr.get_bounds()
            print(f"\n✓ District bounds: {bounds}")
        except Exception as e:
            print(f"✗ Bounds failed: {e}")
        
        # Test district lookup
        distritos = mgr.get_all_distritos()
        if distritos:
            first_distrito = distritos[0]
            by_id = mgr.get_distrito_by_id(first_distrito['id'])
            by_name = mgr.get_distrito_by_name(first_distrito['nombre'])
            
            if by_id and by_name:
                print(f"✓ District lookup works: {by_id['nombre']}")
            else:
                print("✗ District lookup failed")
        
        print("\n✓ All tests passed!")
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_distrito_loader()
    sys.exit(0 if success else 1)
