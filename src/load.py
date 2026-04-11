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
