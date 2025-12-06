"""
Buscar device_id para dispositivo específico desde Supabase
Uso: python find_device_id.py "HUAWEI ELE-L29"
"""

import sys
import os
from supabase import create_client
from dotenv import load_dotenv
import pandas as pd

def find_device_id(device_name: str, supabase_url: str, supabase_key: str):
    """Buscar device_id para un nombre de dispositivo específico"""
    
    if not supabase_url or not supabase_key:
        print("Error: Faltan credenciales de Supabase")
        return
    
    supabase = create_client(supabase_url, supabase_key)
    
    print(f"Buscando device_id para: {device_name}")
    print("=" * 60)
    
    try:
        # Primero obtener el conteo total
        count_response = supabase.table('locations').select('device_id, device_name', count='exact').eq('device_name', device_name).execute()
        total_count = count_response.count if count_response.count else 0
        
        print(f"Total de registros encontrados: {total_count:,}")
        print("=" * 60)
        
        # Consulta principal - búsqueda exacta sin límite
        response = supabase.table('locations').select('device_id, device_name').eq('device_name', device_name).execute()
        
        if response.data:
            df = pd.DataFrame(response.data)
            
            # Obtener device_ids únicos
            unique_device_ids = df['device_id'].unique()
            
            print(f"Encontrados {len(unique_device_ids)} device_ids únicos:")
            print()
            
            for i, device_id in enumerate(unique_device_ids, 1):
                count = len(df[df['device_id'] == device_id])
                print(f"{i}. device_id: {device_id} ({count:,} registros)")
            
            print()
            print("Resumen:")
            print(f"- Dispositivo: {device_name}")
            print(f"- Total de registros: {len(df):,}")
            print(f"- Device IDs únicos: {len(unique_device_ids)}")
            
            # Si hay múltiples device_ids, mostrar ejemplo de cada uno
            if len(unique_device_ids) > 1:
                print("\nEjemplos de registros por device_id:")
                for device_id in unique_device_ids:
                    sample = df[df['device_id'] == device_id].iloc[0]
                    print(f"  - {device_id}: {sample['device_name']}")
            
            return unique_device_ids
            
        else:
            print(f"No se encontró el dispositivo '{device_name}' en la base de datos")
            
            # Búsqueda aproximada
            print(f"\nBuscando dispositivos similares con '{device_name.split()[0]}':")
            similar_response = supabase.table('locations').select('device_id, device_name').ilike('device_name', f'%{device_name.split()[0]}%').limit(50).execute()
            
            if similar_response.data:
                df_similar = pd.DataFrame(similar_response.data)
                similar_devices = df_similar['device_name'].value_counts().head(15)
                
                print(f"\nDispositivos {device_name.split()[0]} encontrados:")
                for device, count in similar_devices.items():
                    device_ids = df_similar[df_similar['device_name'] == device]['device_id'].unique()
                    print(f"- {device}: {count:,} registros (device_ids: {', '.join(device_ids)})")
            else:
                print("No se encontraron dispositivos similares")
            
            return []
    
    except Exception as e:
        print(f"Error en la consulta: {e}")
        return []

def main():
    """Función principal"""
    # Cargar variables de entorno
    load_dotenv()
    
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Faltan credenciales de Supabase en .env")
        return
    
    if len(sys.argv) > 1:
        device_name = " ".join(sys.argv[1:])
    else:
        device_name = "HUAWEI ELE-L29"  # Default
    
    device_ids = find_device_id(device_name, SUPABASE_URL, SUPABASE_KEY)
    
    if device_ids:
        print(f"\nRESULTADO FINAL:")
        print(f"Device IDs para '{device_name}': {', '.join(device_ids)}")
        
        # Buscar todos los registros para este device_id por si hay variaciones de nombre
        if len(device_ids) == 1:
            device_id = device_ids[0]
            print(f"\nVerificando variaciones de nombre para device_id: {device_id}")
            print("=" * 60)
            
            # Crear nueva conexión a Supabase
            supabase_check = create_client(SUPABASE_URL, SUPABASE_KEY)
            
            name_variations_response = supabase_check.table('locations').select('device_name', count='exact').eq('device_id', device_id).execute()
            
            if name_variations_response.data:
                df_variations = pd.DataFrame(name_variations_response.data)
                name_counts = df_variations['device_name'].value_counts()
                
                print(f"Total de registros para este device_id: {name_variations_response.count:,}")
                print("\nVariaciones de nombre encontradas:")
                for name, count in name_counts.items():
                    print(f"- '{name}': {count:,} registros")
                
                if len(name_counts) > 1:
                    print(f"\nNOTA: Hay {len(name_counts)} variaciones de nombre para el mismo device_id")
                    print("Esto podría explicar la diferencia en conteos.")

if __name__ == "__main__":
    main()
