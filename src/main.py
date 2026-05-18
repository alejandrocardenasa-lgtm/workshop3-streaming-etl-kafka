from extract import extract_csv_files
from clean import clean_datasets
from transform import transform_datasets
from load import load_processed_data

def main():

    print("Starting ETL pipeline...\n")

    # Extract phase
    datasets = extract_csv_files()

    for year, df in datasets.items():
        print(f"{year} extracted -> Shape: {df.shape}")

    # Cleaning phase
    cleaned_datasets = clean_datasets(datasets)

    for year, df in cleaned_datasets.items():
        print(f"{year} cleaned -> Shape: {df.shape}")

    # Transformation phase
    final_df = transform_datasets(cleaned_datasets)

    print("\nUnified dataset created successfully.")
    print(f"Final dataset shape: {final_df.shape}")

    print("\nETL pipeline completed successfully.")

    # Load 
    load_processed_data(final_df)


if __name__ == "__main__":
    main()