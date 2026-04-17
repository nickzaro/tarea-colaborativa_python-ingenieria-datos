import pandas as pd
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

def load_source_data(file_list):
    """
    Carga de datos de forma secuencial (uno por uno).
    """
    print(f"Iniciando extracción secuencial de {len(file_list)} archivos...")
    
    valid_results = []
    
    # Usamos un bucle for tradicional
    for file_path in file_list:
        df_fragmento = load_fragment(file_path)
        
        if df_fragmento is not None:
            valid_results.append(df_fragmento)
    
    # Verificamos si logramos cargar todo
    if len(valid_results) != len(file_list):
        print("Atención: No se cargaron todos los archivos esperados.")
        raise FileNotFoundError("Faltan fragmentos críticos")

    if not valid_results:
        print("No se cargó ningún dato válido.")
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    print(f"Concatenando {len(valid_results)} fragmentos exitosos...")
    full_df = pd.concat(valid_results, ignore_index=True)
    
    return full_df