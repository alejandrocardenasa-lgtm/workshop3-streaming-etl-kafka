import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px


# PostgreSQL connection configuration
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "happiness_db",
    "user": "admin",
    "password": "admin"
}


# Load data directly from PostgreSQL
def load_data():

    connection = psycopg2.connect(**DB_CONFIG)

    query = """
    SELECT
        fp.prediction_id,
        dc.country_name,
        dd.year,
        fp.actual_score,
        fp.predicted_score,
        fp.prediction_error,
        fp.prediction_timestamp
    FROM fact_predictions fp
    JOIN dim_country dc
        ON fp.country_id = dc.country_id
    JOIN dim_date dd
        ON fp.date_id = dd.date_id
    ORDER BY fp.prediction_timestamp;
    """

    df = pd.read_sql(query, connection)

    connection.close()

    return df


# Streamlit page configuration
st.set_page_config(
    page_title="Happiness Prediction Dashboard",
    layout="wide"
)


# Dashboard title
st.title("Happiness Prediction Dashboard")

st.write(
    "Dashboard connected directly to PostgreSQL prediction database."
)


# Load prediction data
df = load_data()


# =========================
# KPI SECTION
# =========================

# Calculate average prediction error
avg_error = df["prediction_error"].mean()

# Create KPI cards
col1, col2, col3 = st.columns(3)

col1.metric(
    "Average Prediction Error",
    round(avg_error, 3)
)

col2.metric(
    "Total Predictions",
    len(df)
)

col3.metric(
    "Countries",
    df["country_name"].nunique()
)

st.divider()


# =========================
# PREDICTIONS BY COUNTRY
# =========================

# Count predictions by country
predictions_by_country = (
    df.groupby("country_name")["prediction_id"]
    .count()
    .reset_index()
    .rename(columns={"prediction_id": "total_predictions"})
    .sort_values("total_predictions", ascending=False)
)


# =========================
# PREDICTION TREND
# =========================

# Average predicted score by year
trend = (
    df.groupby("year")["predicted_score"]
    .mean()
    .reset_index()
)


# =========================
# DASHBOARD LAYOUT
# =========================

# Create 2 columns
left_col, right_col = st.columns(2)


# LEFT COLUMN
with left_col:

    # Bar chart: predictions by country
    fig_country = px.bar(
        predictions_by_country.head(15),
        x="country_name",
        y="total_predictions",
        title="Predictions by Country"
    )

    st.plotly_chart(
        fig_country,
        use_container_width=True
    )


# RIGHT COLUMN
with right_col:

    # Scatter plot: predicted vs actual score
    fig_scatter = px.scatter(
        df,
        x="actual_score",
        y="predicted_score",
        hover_data=["country_name", "year"],
        title="Predicted vs Actual Happiness Score"
    )

    st.plotly_chart(
        fig_scatter,
        use_container_width=True
    )


# SECOND ROW
left_col2, right_col2 = st.columns(2)


# LEFT SIDE
with left_col2:

    # Line chart: prediction trend over time
    fig_trend = px.line(
        trend,
        x="year",
        y="predicted_score",
        markers=True,
        title="Prediction Trends Over Time"
    )

    st.plotly_chart(
        fig_trend,
        use_container_width=True
    )


# RIGHT SIDE
with right_col2:

    # Prediction table
    st.subheader("Prediction Data")

    st.dataframe(
        df.head(20),
        use_container_width=True
    )