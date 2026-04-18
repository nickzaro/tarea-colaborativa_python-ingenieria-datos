import pandas as pd
from datetime import date


# ==============================================================================
# MÓDULO DE CARACTERIZACIÓN (CHARACTERIZE) — Adaptado para Apache Airflow
# Feature Engineering: crea columnas derivadas necesarias para las visualizaciones.
# ==============================================================================


def generate_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    [SRP] Feature Engineering sobre el DataFrame normalizado de Netflix.

    Columnas nuevas generadas:
      - year_added          : año en que el título fue agregado a Netflix.
      - month_added         : nombre del mes en que fue agregado.
      - main_genre          : primer género listado (simplificación para gráficos).
      - duration_num        : valor numérico extraído de la columna 'duration'.
      - movie_duration_min  : duración en minutos (solo para películas).
      - tv_show_seasons     : número de temporadas (solo para series).
      - content_age         : años transcurridos desde el estreno original.
    """
    df_features = df.copy()

    # Año y mes de incorporación a la plataforma
    df_features['year_added']  = df_features['date_added'].dt.year
    df_features['month_added'] = df_features['date_added'].dt.month_name()

    # Primer género listado
    df_features['main_genre'] = df_features['listed_in'].apply(
        lambda x: x.split(",")[0].strip()
    )

    # Valor numérico de duración ("90 min" → 90.0 | "2 Seasons" → 2.0)
    df_features['duration_num'] = (
        df_features['duration'].str.extract(r'(\d+)').astype(float)
    )

    # Separar duración según tipo de contenido
    df_features['movie_duration_min'] = df_features.apply(
        lambda x: x['duration_num'] if x['type'] == 'Movie' else None, axis=1
    )
    df_features['tv_show_seasons'] = df_features.apply(
        lambda x: x['duration_num'] if x['type'] == 'Tv show' else None, axis=1
    )

    # Antigüedad del contenido respecto al año actual
    current_year = date.today().year
    df_features['content_age'] = current_year - df_features['release_year']

    # Optimización de memoria: columnas categóricas
    categorical_cols = ['type', 'rating', 'main_genre']
    for col in categorical_cols:
        if col in df_features.columns:
            df_features[col] = df_features[col].astype('category')

    return df_features


def run_characterize(source_table: str, target_table: str, sql_tools) -> None:
    """
    [Patrón Fachada] Orquestador de la fase Characterize.

    Parámetros
    ----------
    source_table : str
        Tabla SQLite con los datos del paso Load (dim_netflix_titles).
    target_table : str
        Tabla SQLite donde se guardarán los datos con features generadas.
    sql_tools : module
        Módulo con load_df_from_sql / save_df_to_sql.
    """
    print("\n" + "=" * 40)
    print("INICIANDO FASE DE CARACTERIZACIÓN (CHARACTERIZE)")
    print("=" * 40)

    # 1. Leer datos del Data Warehouse
    df = sql_tools.load_df_from_sql(source_table)

    # La columna date_added viene como string desde SQLite, hay que reconvertirla
    df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

    print(f"Registros recibidos de Load: {len(df)}")

    # 2. Aplicar Feature Engineering
    df_features = generate_features(df)
    print(f"Features generadas. Columnas totales: {len(df_features.columns)}")

    # 3. Persistir en SQLite para que Visualize pueda leerlo
    sql_tools.save_df_to_sql(df_features, target_table)

    print("=" * 40)
    print("FASE DE CARACTERIZACIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 40 + "\n")
