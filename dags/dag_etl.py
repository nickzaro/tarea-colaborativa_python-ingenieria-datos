import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
# Importamos tu lógica migrada
from scripts.extract import load_source_data
from scripts.sql_tools import save_df_to_sql

csv_files = [
        'data/netflix_titles_1.csv', 'data/netflix_titles_2.csv', 
        'data/netflix_titles_3.csv', 'data/netflix_titles_4.csv'
    ]

# Lee los archivos por partes, los une y guarda en una tabla de sqlite
def wrapper_extract():
    table_name = "netflix_extract"
    # Ejecutamos tu lógica de transformación
    try:
        df_limpio = load_source_data(csv_files)
        save_df_to_sql(df_limpio, table_name)
    except Exception as e:
        exit(1)

with DAG(
    dag_id='dag_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily', # Se corre diario automáticamente
    catchup=False
) as dag:

    
    task_extraer = PythonOperator(
        task_id='extraer',
        python_callable=wrapper_extract
    )