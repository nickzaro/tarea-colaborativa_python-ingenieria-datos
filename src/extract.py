import pandas as pd
from concurrent.futures import ProcessPoolExecutor
import os

def load_fragment(file_path):
    """
    Carga un fragmento individual. 
    Retorna None si el archivo no existe para manejarlo en load_source_data.
    """
    if not os.path.exists(file_path):
        print(f"Error: Archivo no encontrado en {file_path}.")
        return None
    
    try:
        print(f"Procesando: {file_path}")
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error al leer {file_path}: {e}")
        return None

def load_source_data(file_list):
    """
    Carga de datos usando ProcessPoolExecutor.
    """
    print(f"Iniciando extracción paralela con {len(file_list)} workers...")
    
    with ProcessPoolExecutor(max_workers=len(file_list)) as executor:
        # map garantiza que el orden de los resultados sea el mismo que file_list
        results = list(executor.map(load_fragment, file_list))
    
    # Filtramos los posibles None (archivos que fallaron)
    valid_results = [df for df in results if df is not None]
    
    if len(valid_results) != len(file_list):
        print("No se cargaron todos los archivos.")
        raise FileNotFoundError(f"Faltan archivos: se esperaban {len(file_list)} pero se cargaron {len(valid_results)}.")

    print(f"Concatenando {len(valid_results)} fragmentos exitosos...")
    full_df = pd.concat(valid_results, ignore_index=True)
    return full_df