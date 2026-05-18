import pandas as pd


def transform_datasets(datasets):

    transformed_datasets = []

    column_mappings = {

        "2015": {
            "Country": "country",
            "Happiness Rank": "happiness_rank",
            "Happiness Score": "happiness_score",
            "Economy (GDP per Capita)": "gdp",
            "Family": "family",
            "Health (Life Expectancy)": "health",
            "Freedom": "freedom",
            "Generosity": "generosity",
            "Trust (Government Corruption)": "corruption"
        },

        "2016": {
            "Country": "country",
            "Happiness Rank": "happiness_rank",
            "Happiness Score": "happiness_score",
            "Economy (GDP per Capita)": "gdp",
            "Family": "family",
            "Health (Life Expectancy)": "health",
            "Freedom": "freedom",
            "Generosity": "generosity",
            "Trust (Government Corruption)": "corruption"
        },

        "2017": {
            "Country": "country",
            "Happiness.Rank": "happiness_rank",
            "Happiness.Score": "happiness_score",
            "Economy..GDP.per.Capita.": "gdp",
            "Family": "family",
            "Health..Life.Expectancy.": "health",
            "Freedom": "freedom",
            "Generosity": "generosity",
            "Trust..Government.Corruption.": "corruption"
        },

        "2018": {
            "Country or region": "country",
            "Overall rank": "happiness_rank",
            "Score": "happiness_score",
            "GDP per capita": "gdp",
            "Social support": "family",
            "Healthy life expectancy": "health",
            "Freedom to make life choices": "freedom",
            "Generosity": "generosity",
            "Perceptions of corruption": "corruption"
        },

        "2019": {
            "Country or region": "country",
            "Overall rank": "happiness_rank",
            "Score": "happiness_score",
            "GDP per capita": "gdp",
            "Social support": "family",
            "Healthy life expectancy": "health",
            "Freedom to make life choices": "freedom",
            "Generosity": "generosity",
            "Perceptions of corruption": "corruption"
        }
    }

    selected_columns = [
        "country",
        "year",
        "happiness_rank",
        "happiness_score",
        "gdp",
        "family",
        "health",
        "freedom",
        "generosity",
        "corruption"
    ]

    for year, df in datasets.items():

        df = df.copy()

        # Standardize column names
        df = df.rename(columns=column_mappings[year])

        # Add year column
        df["year"] = int(year)

        # Select unified schema columns
        df = df[selected_columns]

        # Standardize data types
        df["country"] = df["country"].astype(str)

        df["year"] = df["year"].astype(int)

        numerical_columns = [
            "happiness_rank",
            "happiness_score",
            "gdp",
            "family",
            "health",
            "freedom",
            "generosity",
            "corruption"
        ]

        df[numerical_columns] = df[numerical_columns].astype(float)

        transformed_datasets.append(df)

    # Merge datasets
    final_df = pd.concat(transformed_datasets, ignore_index=True)

    return final_df