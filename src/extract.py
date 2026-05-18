import pandas as pd
from pathlib import Path


RAW_DATA_PATH = Path("data/raw")


def extract_csv_files():

    datasets = {}

    for year in range(2015, 2020):

        file_path = RAW_DATA_PATH / f"{year}.csv"

        datasets[str(year)] = pd.read_csv(file_path)

    return datasets