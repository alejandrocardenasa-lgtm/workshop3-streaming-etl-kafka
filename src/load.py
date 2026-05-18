from pathlib import Path


PROCESSED_DATA_PATH = Path("data/processed")


def load_processed_data(df):

    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

    output_path = PROCESSED_DATA_PATH / "final_happiness_dataset.csv"

    df.to_csv(output_path, index=False)

    print(f"\nProcessed dataset saved successfully at: {output_path}")