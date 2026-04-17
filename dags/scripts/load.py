import json
import os
from datetime import datetime

import pandas as pd

def publish_to_data_warehouse(df: pd.DataFrame, sql_tools) -> None:

    target_table = "dim_netflix_titles"
    print(f"[Data Warehouse] Inyectando {len(df)} registros en '{target_table}'...")
    sql_tools.save_df_to_sql(df, target_table)
    print("[Data Warehouse] Carga transaccional completada.")