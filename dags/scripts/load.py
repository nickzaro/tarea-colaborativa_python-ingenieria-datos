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

def generate_governance_metadata(df: pd.DataFrame, output_path: str) -> None:
    metadata_dir = "output/metadata"
    os.makedirs(metadata_dir, exist_ok=True)

    metadata = {
        "execution_date":        datetime.now().isoformat(),
        "total_records_inserted": len(df),
        "columns":               list(df.columns),
        "target_system":         output_path,
        "memory_usage_mb":       round(
            df.memory_usage(deep=True).sum() / (1024 * 1024), 2
        ),
    }

    file_timestamp   = datetime.now().strftime('%Y%m%d_%H%M%S')
    metadata_path    = f"{metadata_dir}/load_log_{file_timestamp}.json"

    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)

    print(f"[Auditoría] Metadatos guardados en: {metadata_path}")

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
