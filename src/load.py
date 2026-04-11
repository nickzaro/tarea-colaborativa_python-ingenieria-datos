import pandas as pd
import sqlite3
import json
from datetime import datetime
import os

# ==============================================================================
# MÓDULO DE CARGA (LOAD)
# ==============================================================================

def execute_quality_gate(df: pd.DataFrame) -> bool:
    """
    [SRP] Single Responsibility Principle: Única tarea de validar las reglas de negocio.
    [Clean Code] Fail Fast: Si el dato está mal o impuro (nulo/vacío), detiene el proceso 
    de inmediato sin intentar hacer reparaciones que puedan corromper la BD final.
    """
    if df.empty:
        print("[FALLO] Calidad de Datos Fallida: El DataFrame esta vacio.")
        return False
    
    if df['show_id'].isnull().any():
        print("[FALLO] Calidad de Datos Fallida: Se encontraron IDs primarios nulos.")
        return False
        
    return True

def generate_governance_metadata(df: pd.DataFrame, output_path: str) -> None:
    """
    [SRP] Single Responsibility Principle: Función dedicada exclusivamente a la auditoría.
    [Clean Code] Meaningful Names: El nombre de la función expresa claramente la acción.
    """
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

def publish_to_data_warehouse(df: pd.DataFrame, db_path: str = "output/netflix_dw.db") -> None:
    """
    [OCP] Open/Closed Principle: Esta función está abierta para agregar otros 
    motores de bases de datos, pero la lógica dentro de ella jamás rompe a los demás.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # [Clean Code] Uso del bloque Try-Except-Finally para Safe Disposal (cerrar la DB sí o sí).
    conn = sqlite3.connect(db_path)
    try:
        print(f"[Data Warehouse] Inyectando {len(df)} registros analiticos...")
        df.to_sql("dim_netflix_titles", con=conn, if_exists="replace", index=False)
        print("[Data Warehouse] Carga transaccional completada.")
    except Exception as e:
        print(f"[Data Warehouse] Error durante la transaccion SQL: {e}")
    finally:
        conn.close()

def save_data(df: pd.DataFrame, name_dataframe: str) -> None:
    """
    [Patrón Fachada / Facade Pattern]: Orquestador aislado invocado por job.py.
    [Clean Code] Type Hinting implementado para indicar entrada y salida esperada (None).
    """
    print("\n" + "="*40)
    print("INICIANDO FASE DE CARGA (LOAD)")
    print("="*40)
    
    # 1. Pipeline Defensivo: Compuerta de calidad
    if not execute_quality_gate(df):
        raise ValueError("ALERTA: Los datos curados no pasaron los criterios de calidad.")
    
    os.makedirs(os.path.dirname(name_dataframe), exist_ok=True)

    # 2. Exportacion Plana hacia Output Visual
    print(f"Exportando dataset curado plano CSV en: {name_dataframe}")
    df.to_csv(name_dataframe, index=False, encoding='utf-8')
    
    # 3. Pipeline de Gobernanza
    generate_governance_metadata(df, name_dataframe)
    
    # 4. Pipeline Transaccional SQL
    publish_to_data_warehouse(df)
    
    print("="*40)
    print("FASE DE CARGA COMPLETADA EXITOSAMENTE")
    print("="*40 + "\n")