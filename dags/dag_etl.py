import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --- Módulos internos del DAG ---
from scripts.extract      import load_source_data
from scripts.sql_tools    import save_df_to_sql, load_df_from_sql
import scripts.sql_tools   as sql_tools
import scripts.transform   as transform_module
import scripts.load        as load_module
import scripts.characterize as characterize_module
import scripts.visualize   as visualize_module

# Rutas de los fragmentos CSV (montados en el contenedor via volume)
CSV_FILES = [
    'data/netflix_titles_1.csv',
    'data/netflix_titles_2.csv',
    'data/netflix_titles_3.csv',
    'data/netflix_titles_4.csv',
]

# Tablas SQLite intermedias que conectan cada fase del pipeline
TABLE_EXTRACT     = "netflix_extract"     # salida de Extract
TABLE_TRANSFORM   = "netflix_transform"   # salida de Transform
TABLE_LOAD        = "dim_netflix_titles"  # salida de Load (Data Warehouse)
TABLE_CHARACTERIZE = "netflix_features"  # salida de Characterize


# ==============================================================================
# WRAPPERS  (cada uno corresponde a un único Task de Airflow)
# ==============================================================================

def wrapper_extract():
    """
    TASK 1 — Extract
    Lee los 4 fragmentos CSV y los consolida en 'netflix_extract' (SQLite).
    """
    print("\n" + "=" * 40)
    print("INICIANDO FASE DE EXTRACCIÓN (EXTRACT)")
    print("=" * 40)
    try:
        df = load_source_data(CSV_FILES)
        save_df_to_sql(df, TABLE_EXTRACT)
        print(f"Extract: {len(df)} registros guardados en '{TABLE_EXTRACT}'.")
    except Exception as e:
        print(f"[ERROR] Extract falló: {e}")
        raise


def wrapper_transform():
    """
    TASK 2 — Transform
    Lee 'netflix_extract', limpia y normaliza, escribe en 'netflix_transform'.
    """
    try:
        transform_module.run_transform(
            source_table=TABLE_EXTRACT,
            target_table=TABLE_TRANSFORM,
            sql_tools=sql_tools,
        )
    except Exception as e:
        print(f"[ERROR] Transform falló: {e}")
        raise


def wrapper_load():
    """
    TASK 3 — Load
    Lee 'netflix_transform', valida calidad, exporta CSV, genera metadatos
    y publica en 'dim_netflix_titles' (Data Warehouse).
    """
    try:
        load_module.run_load(
            source_table=TABLE_TRANSFORM,
            sql_tools=sql_tools,
        )
    except Exception as e:
        print(f"[ERROR] Load falló: {e}")
        raise


def wrapper_characterize():
    """
    TASK 4 — Characterize
    Lee 'dim_netflix_titles', aplica Feature Engineering (columnas derivadas)
    y escribe el resultado en 'netflix_features'.
    """
    try:
        characterize_module.run_characterize(
            source_table=TABLE_LOAD,
            target_table=TABLE_CHARACTERIZE,
            sql_tools=sql_tools,
        )
    except Exception as e:
        print(f"[ERROR] Characterize falló: {e}")
        raise


def wrapper_visualize():
    """
    TASK 5 — Visualize
    Lee 'netflix_features' y genera los 6 gráficos PNG en la carpeta output/.
    """
    try:
        visualize_module.run_visualize(
            source_table=TABLE_CHARACTERIZE,
            sql_tools=sql_tools,
        )
    except Exception as e:
        print(f"[ERROR] Visualize falló: {e}")
        raise


# ==============================================================================
# DEFINICIÓN DEL DAG
# ==============================================================================

with DAG(
    dag_id='dag_etl',
    description='Pipeline completo: Extract → Transform → Load → Characterize → Visualize',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',   # Ejecución automática diaria
    catchup=False,
    tags=['etl', 'netflix', 'sqlite'],
) as dag:

    task_extraer = PythonOperator(
        task_id='extraer',
        python_callable=wrapper_extract,
    )

    task_transformar = PythonOperator(
        task_id='transformar',
        python_callable=wrapper_transform,
    )

    task_cargar = PythonOperator(
        task_id='cargar',
        python_callable=wrapper_load,
    )

    task_caracterizar = PythonOperator(
        task_id='caracterizar',
        python_callable=wrapper_characterize,
    )

    task_visualizar = PythonOperator(
        task_id='visualizar',
        python_callable=wrapper_visualize,
    )

    # Cadena completa del pipeline:
    # Extract → Transform → Load → Characterize → Visualize
    task_extraer >> task_transformar >> task_cargar >> task_caracterizar >> task_visualizar
