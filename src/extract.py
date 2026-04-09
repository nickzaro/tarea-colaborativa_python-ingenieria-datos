import pandas as pd
def load_source_data(path):
    df = pd.read_csv(path)
    print(f"Cargado el archivo:{path}")
    return df
