from pandas import DataFrame

from src.extract import load_source_data
from src.transform import clean_missing_values, normalize_columns, generate_features
from src.visualize import plot_content_distribution

def run_pipeline():
    csv_files = [
        'data/netflix_titles_1.csv', 'data/netflix_titles_2.csv', 
        'data/netflix_titles_3.csv', 'data/netflix_titles_4.csv'
    ]

    try:
        # Cargamos en paralelo
        raw_df: DataFrame = load_source_data(csv_files)
        # 1
        print("Transforming data...")
        clean_df = clean_missing_values(raw_df)
        normalized_df = normalize_columns(clean_df)
        # 2
        final_df = generate_features(normalized_df)
        # 3
        plot_content_distribution(final_df)
        print("Termino correctamente el pipeline.")

    except FileNotFoundError as e:
        print(f"ERROR CRÍTICO EN EXTRACCIÓN: {e}")
        print("Termino incorrectamente el pipeline, se detuvo porque el dataset está incompleto.")
    except Exception as e:
        print(f"Ocurrió un error inesperado: {e}")
        print("Termino incorrectamente el pipeline.")
if __name__ == "__main__":
    run_pipeline()