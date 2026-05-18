def clean_datasets(datasets):

    cleaned_datasets = {}

    for year, df in datasets.items():

        df = df.copy()

        # Remove duplicated records
        df = df.drop_duplicates()

        # Remove missing value from 2018 dataset
        if year == "2018":
            df = df.dropna(subset=["Perceptions of corruption"])

        cleaned_datasets[year] = df

    return cleaned_datasets