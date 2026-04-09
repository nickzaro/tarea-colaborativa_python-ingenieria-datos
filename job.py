from pandas import DataFrame

from src.extract import load_source_data
from src.transform import clean_missing_values, normalize_columns, generate_features
from src.visualize import plot_content_distribution

def run_pipeline():
    # Extraction
    raw_df: DataFrame = load_source_data('data/netflix_titles.csv')
    
    if raw_df is not None:
        # Transformation
        # 1
        clean_df = clean_missing_values(raw_df)
        normalized_df = normalize_columns(clean_df)
        # 2
        final_df = generate_features(normalized_df)
        # 3
        # Visualization
        plot_content_distribution(final_df)
        
        print("Pipeline execution completed successfully.")

if __name__ == "__main__":
    run_pipeline()
