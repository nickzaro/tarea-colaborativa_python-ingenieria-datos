
def save_data(df, name_dataframe):
    print(f"Guardando el DataFrame que contiene {len(df)} filas, como {name_dataframe}")
    df.to_csv(name_dataframe, index=False, encoding='utf-8')    