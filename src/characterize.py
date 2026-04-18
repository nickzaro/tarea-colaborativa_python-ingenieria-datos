import pandas as pd
from datetime import date

def generate_features(normalized_df):
    """
    Realiza Feature Engineering sobre el DataFrame de Netflix.
    Crea nuevas columnas para análisis de tiempo, géneros y duraciones.
    """
    # Copia de seguridad para evitar advertencias de SettingWithCopy
    df_features = normalized_df.copy()
)

    # Diferencia entre el año actual y el año de estreno original
    current_year = date.today().year
    df_features['content_age'] = current_year - df_features['release_year']

    # Esto reduce el peso del archivo final y acelera los gráficos
    categorical_cols = ['type', 'rating', 'main_genre']
    for col in categorical_cols:
        if col in df_features.columns:
            df_features[col] = df_features[col].astype('category')

    return df_features