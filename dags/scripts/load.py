import json
import os
from datetime import datetime

import pandas as pd
def execute_quality_gate(df: pd.DataFrame) -> bool:
    if df.empty:
        print("[FALLO] Calidad de Datos: El DataFrame está vacío.")
        return False

    if df["show_id"].isnull().any():
        print("[FALLO] Calidad de Datos: Se encontraron IDs primarios nulos.")
        return False

    return True

def publish_to_data_warehouse(df: pd.DataFrame, sql_tools) -> None:
    target_table = "dim_netflix_titles"
    print(f"[Data Warehouse] Inyectando {len(df)} registros en '{target_table}'...")
    sql_tools.save_df_to_sql(df, target_table)
    print("[Data Warehouse] Carga transaccional completada.")


def run_load(source_table: str, sql_tools) -> None:
    print("\n" + "=" * 40)
    print("INICIANDO FASE DE CARGA (LOAD)")
    print("=" * 40)

    # 1. Leer datos desde la tabla producida por Transform
    df = sql_tools.load_df_from_sql(source_table)
    print(f"Registros recibidos de Transform: {len(df)}")

    # 2. Compuerta de calidad — falla rápido si los datos no son válidos
    if not execute_quality_gate(df):
        raise ValueError("ALERTA: Los datos no pasaron la compuerta de calidad.")

    csv_path = "output/netflix_curado.csv"
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    # 3. Exportación plana a CSV (para consumo visual / archivado)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    print(f"Dataset curado exportado en CSV: {csv_path}")

    # 4. Metadatos de gobernanza
    generate_governance_metadata(df, csv_path)

    # 5. Carga final al Data Warehouse SQLite
    publish_to_data_warehouse(df, sql_tools)

    print("=" * 40)
    print("FASE DE CARGA COMPLETADA EXITOSAMENTE")
    print("=" * 40 + "\n")
