import pandas as pd
from scripts.sql_tools import save_df_to_sql, load_df_from_sql
import os

REQUIRED_COLUMNS = ['show_id', 'type', 'title', 'director', 'cast', 'country',
                       'date_added', 'release_year', 'rating', 'duration',
                       'listed_in', 'description']

def load_fragment(file_path):
    """
    Carga un fragmento individual de forma secuencial.
    """
    if not os.path.exists(file_path):
        print(f"Error: Archivo no encontrado en {file_path}.")
        return None
    
    try:
        print(f"Procesando: {file_path}")
        # Leemos el CSV directamente
        return pd.read_csv(file_path, names=REQUIRED_COLUMNS, header=0)
    except Exception as e:
        print(f"Error al leer {file_path}: {e}")
        return None

def load_source_data(table_list):
    """
    Carga los tablas en una sola tabla maestra
    """
    print(f"Iniciando la union de {len(table_list)} tablas...")
    
    valid_results = []
    
    # Usamos un bucle for tradicional
    for table in table_list:
        df_fragmento = load_df_from_sql(table)
        
        if df_fragmento is not None:
            valid_results.append(df_fragmento)
    
    # Verificamos si logramos cargar todo
    if len(valid_results) != len(table_list):
        print("Atención: No se cargaron todos las tablas.")
        raise FileNotFoundError("Faltan tablas por cargar")

    if not valid_results:
        print("No se cargó ninguna tabla válida.")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    print(f"Concatenando {len(valid_results)} tablas exitosos...")
    full_df = pd.concat(valid_results, ignore_index=True)
    
    return full_df