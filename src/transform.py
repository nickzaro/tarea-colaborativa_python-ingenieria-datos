import pandas as pd

def clean_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Rellena o elimina valores nulos"""

    # hacemos una copia para no alterar el dataframe original
    clean_df = df.copy()

    # rellenamos valores nulos en columnas de texto con 'desconocido'
    cols_to_fill = ['director', 'cast', 'country']
    for col in cols_to_fill:
        clean_df[col] = clean_df[col].fillna('Desconocido')

    # si no hay fecha o rating se eliminaran por completo
    clean_df = clean_df.dropna(subset=['date_added', 'rating'])

    return clean_df