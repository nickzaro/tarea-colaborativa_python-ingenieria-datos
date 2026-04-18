import pandas as pd


# ==============================================================================
# MÓDULO DE TRANSFORMACIÓN (TRANSFORM) — Adaptado para Apache Airflow
# ==============================================================================


def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    [SRP] Limpieza de valores nulos y duplicados.

    Estrategia:
      - Duplicados          → se eliminan.
      - director/cast/country nulos → se rellenan con 'Unknown'.
      - rating/duration/date_added nulos → se eliminan (datos críticos).
    """
    clean_df = df.copy()

    # Eliminar filas completamente duplicadas
    clean_df = clean_df.drop_duplicates()

    # Rellenar columnas opcionales con valor centinela
    cols_to_fill = ['director', 'cast', 'country']
    for col in cols_to_fill:
        clean_df[col] = clean_df[col].fillna('Unknown')

    # Eliminar filas con datos estructurales faltantes
    clean_df = clean_df.dropna(subset=["rating", "duration", "date_added"])

    return clean_df


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    [SRP] Normalización de tipos y formato de texto.

    Transformaciones aplicadas:
      - date_added  → datetime (elimina espacios y convierte tipo).
      - type/rating → capitalize (primera letra mayúscula).
      - show_id     → strip (sin espacios en blanco al inicio/fin).
    """
    norm_df = df.copy()

    norm_df["date_added"] = pd.to_datetime(
        norm_df["date_added"].astype(str).str.strip()
    )
    norm_df["type"]    = norm_df["type"].str.capitalize()
    norm_df["rating"]  = norm_df["rating"].str.capitalize()
    norm_df["show_id"] = norm_df["show_id"].str.strip()

    return norm_df


def run_transform(source_table: str, target_table: str, sql_tools) -> None:
    """
    [Patrón Fachada] Orquestador de la fase Transform.

    Parámetros
    ----------
    source_table : str
        Nombre de la tabla SQLite que contiene los datos crudos (Extract).
    target_table : str
        Nombre de la tabla SQLite donde se guardarán los datos transformados.
    sql_tools : module
        Módulo con las funciones load_df_from_sql / save_df_to_sql.
    """
    print("\n" + "=" * 40)
    print("INICIANDO FASE DE TRANSFORMACIÓN (TRANSFORM)")
    print("=" * 40)

    # 1. Leer datos desde la tabla producida por Extract
    df_raw = sql_tools.load_df_from_sql(source_table)
    print(f"Registros recibidos de Extract: {len(df_raw)}")

    # 2. Limpiar valores nulos y duplicados
    df_clean = clean_missing_values(df_raw)
    print(f"Registros tras limpieza: {len(df_clean)}")

    # 3. Normalizar columnas
    df_norm = normalize_columns(df_clean)
    print(f"Registros normalizados: {len(df_norm)}")

    # 4. Persistir resultado en SQLite para que Load pueda leerlo
    sql_tools.save_df_to_sql(df_norm, target_table)

    print("=" * 40)
    print("FASE DE TRANSFORMACIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 40 + "\n")
