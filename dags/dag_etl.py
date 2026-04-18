import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# Importamos tu lógica migrada
from scripts.extract import load_fragment, load_source_data
from scripts.sql_tools import save_df_to_sql, load_df_from_sql

CSV_FILES = [
        'data/netflix_titles_1.csv', 'data/netflix_titles_2.csv', 
        'data/netflix_titles_3.csv', 'data/netflix_titles_4.csv'
    ]

TABLE_EXTRACT_LIST = ["netflix_extract_1", "netflix_extract_2", "netflix_extract_3", "netflix_extract_4"]

TABLE_EXTRACT = "netflix_extract"     # salida de Extract


def wrapper_extract_part_1():
    
    try:
        df_limpio = load_fragment(CSV_FILES[0])
        save_df_to_sql(df_limpio, TABLE_EXTRACT_LIST[0])
    except Exception as e:
        exit(1)

def wrapper_extract_part_2():
    
    try:
        df_limpio = load_fragment(CSV_FILES[1])
        save_df_to_sql(df_limpio, TABLE_EXTRACT_LIST[1])
    except Exception as e:
        exit(1)

def wrapper_extract_part_3():
    
    try:
        df_limpio = load_fragment(CSV_FILES[2])
        save_df_to_sql(df_limpio, TABLE_EXTRACT_LIST[2])
    except Exception as e:
        exit(1)

def wrapper_extract_part_4():
    
    try:
        df_limpio = load_fragment(CSV_FILES[3])
        save_df_to_sql(df_limpio, TABLE_EXTRACT_LIST[3])
    except Exception as e:
        exit(1)

# Lee los tablas temporales, los une y guarda en una tabla de sqlite
def wrapper_extract():
    try:
        full_df = load_source_data(TABLE_EXTRACT_LIST)
        save_df_to_sql(full_df, TABLE_EXTRACT)
    except Exception as e:
        exit(1)   


with DAG(
    dag_id='dag_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily', # Se corre diario automáticamente
    catchup=False
) as dag:

    task_extraer_part_1 = PythonOperator(
        task_id='extraer_part_1',
        python_callable=wrapper_extract_part_1
    )

    task_extraer_part_2 = PythonOperator(
        task_id='extraer_part_2',
        python_callable=wrapper_extract_part_2
    )

    task_extraer_part_3 = PythonOperator(
        task_id='extraer_part_3',
        python_callable=wrapper_extract_part_3
    )

    task_extraer_part_4 = PythonOperator(
        task_id='extraer_part_4',
        python_callable=wrapper_extract_part_4
    )

    task_extraer = PythonOperator(
        task_id='extraer',
        python_callable=wrapper_extract
    )

    [task_extraer_part_1, task_extraer_part_2, task_extraer_part_3, task_extraer_part_4] >> task_extraer