import pandas as pd

def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Rellena o elimina valores nulos"""

    # hacemos una copia para no alterar el dataframe original
    clean_df = df.copy()

    # Eliminar duplicados
    clean_df = clean_df.drop_duplicates()

    # rellenamos valores nulos en columnas de texto con 'desconocido'
    cols_to_fill = ['director', 'cast', 'country']
    for col in cols_to_fill:
        clean_df[col] = clean_df[col].fillna('Unknown')

    # si no hay fecha o rating se eliminaran por completo
    clean_df = clean_df.dropna(subset=["rating", "duration", "date_added","date_added"])

    return clean_df

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """normaliza los tipos de datos y el formato del texto"""
    norm_df = df.copy()

    # convertimos la columna de texto 'date_added' a un tipo datetime real
    norm_df["date_added"] = pd.to_datetime(norm_df["date_added"].astype(str).str.strip())

    # pasamos a minusculas usando capitalize
    norm_df["type"] = norm_df["type"].str.capitalize()
    norm_df["rating"] = norm_df["rating"].str.capitalize()

    # eliminamos los espacios vacíos de inicio y fin en el id
    norm_df["show_id"] = norm_df["show_id"].str.strip()

    return norm_df
