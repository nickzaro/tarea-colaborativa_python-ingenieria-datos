import pandas as pd
import sqlite3
import json
from datetime import datetime
import os

def execute_quality_gate(df: pd.DataFrame) -> bool:

    if df.empty:
        print("[FALLO] Calidad de Datos Fallida: El DataFrame esta vacio.")
        return False
    
    if df['show_id'].isnull().any():
        print("[FALLO] Calidad de Datos Fallida: Se encontraron IDs primarios nulos.")
        return False
        
    return True

def generate_governance_metadata(df: pd.DataFrame, output_path: str) -> None:

    metadata_dir = "output/metadata"
    os.makedirs(metadata_dir, exist_ok=True)
    
    # [Clean Code] Creación de objetos limpios orientados a diccionario (Clave-Valor)
    metadata = {
        "execution_date": datetime.now().isoformat(),
        "total_records_inserted": len(df),
        "columns": list(df.columns),
        "target_system": output_path,
        "memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2)
    }
    
    file_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    metadata_path = f"{metadata_dir}/load_log_{file_timestamp}.json"
    
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)
        
    print(f"[Auditoria] Metadatos de la carga guardados en: {metadata_path}")
