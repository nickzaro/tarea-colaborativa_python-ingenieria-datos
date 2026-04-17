import os
import pandas as pd
import sqlite3

DATA_BASE = "output/netflix_dw.db"
def get_conn():
    """
    Crea la conexión a SQLite. 
    """
    return sqlite3.connect(DATA_BASE)


def save_df_to_sql(df, table_name):
    """Guarda un DataFrame en SQLite"""
    if df.empty:
        print("El DataFrame está vacío. Nada que guardar.")
        return

    
    folder = os.path.dirname(DATA_BASE)
    if folder and not os.path.exists(folder):
        os.makedirs(folder)
        print(f"Carpeta '{folder}' creada.")

    conn = get_conn()
    try:
        df.to_sql(table_name, con=conn, if_exists="replace", index=False)
        print(f"Datos guardados en la tabla '{table_name}'.")
    except Exception as e:
        error_detalle = f"Error al guardar en SQLite: {e}"
        print(f"{error_detalle}")
        raise RuntimeError(error_detalle) from e
    finally:
        conn.close()

def load_df_from_sql(table_name):
    """
    Lee una tabla de SQLite y la devuelve como un DataFrame de Pandas.
    """
    if not os.path.exists(DATA_BASE):
        error_msg = f"Base de datos no encontrada en la ruta: {DATA_BASE}"
        print(f"{error_msg}")
        raise FileNotFoundError(error_msg) # Lanzamos excepción específica

    conn = get_conn()
    try:
        print(f"Leyendo tabla '{table_name}' desde SQLite...")
        # read_sql_query convierte el resultado de la consulta directamente en DataFrame
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, con=conn)
        print(f"Se cargaron {len(df)} filas exitosamente.")
        return df
    except Exception as e:
        error_detalle = f"Error crítico al leer la tabla '{table_name}': {str(e)}"
        print(f"{error_detalle}")
        raise RuntimeError(error_detalle) from e
    finally:
        conn.close()
        print("Conexión a SQLite cerrada.")