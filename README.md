# Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

## Happiness Prediction Streaming ETL Pipeline

**Author:** Gonoalejo
**Course:** ETL — Data Engineering and Artificial Intelligence
**Workshop:** Workshop 3 — Streaming ETL with Apache Kafka and Machine Learning

---

## 1. Project Description

This project implements a complete end-to-end **Streaming ETL pipeline** using historical World Happiness Report datasets, Apache Kafka, PostgreSQL, Machine Learning, and Streamlit.

The main goal of the project is to move from a traditional batch ETL process into a streaming architecture where records are sent one by one through Kafka, validated by a consumer, used for real-time machine learning inference, stored in PostgreSQL, and analyzed through a live dashboard.

The project is not focused on building the most complex machine learning model. Instead, the main focus is pipeline integration, reliability, traceability, validation, and real-time data processing.

---

## 2. Main Objective

The objective of this workshop is to design and implement a streaming ETL pipeline capable of generating real-time predictions using Apache Kafka and a pre-trained machine learning model.

The project includes:

* Exploratory Data Analysis before cleaning and transformation
* Batch ETL pipeline
* Data cleaning and harmonization
* Feature engineering
* Regression model training
* Model serialization as `model.pkl`
* Kafka producer
* Kafka consumer
* Event validation
* Raw event storage
* Prediction storage in PostgreSQL
* Dimensional database design
* Dashboard connected directly to PostgreSQL

---

## 3. Technologies Used

| Technology           | Purpose                                                  |
| -------------------- | -------------------------------------------------------- |
| Python               | Main programming language                                |
| Pandas               | Data extraction, profiling, cleaning, and transformation |
| NumPy                | Numerical operations                                     |
| Matplotlib / Seaborn | EDA visualizations                                       |
| Scikit-learn         | Regression model training and evaluation                 |
| Joblib               | Model serialization                                      |
| Apache Kafka         | Streaming platform                                       |
| Kafka-Python         | Python producer and consumer                             |
| PostgreSQL           | Raw and analytical prediction storage                    |
| Psycopg2             | Python connection to PostgreSQL                          |
| Docker Compose       | Container orchestration                                  |
| Streamlit            | Dashboard application                                    |
| Plotly               | Interactive dashboard charts                             |
| GitHub               | Repository and project delivery                          |

---

## 4. Project Structure

```text
workshop3-streaming-etl-kafka/
│
├── dashboards/
│   ├── dashboard.py
│   └── screenshots/
│
├── kafka/
│   ├── producer.py
│   └── consumer.py
│
├── models/
│   └── model.pkl
│
├── notebooks/
│   ├── eda.ipynb
│   └── model_training.ipynb
│
├── sql/
│   └── create_tables.sql
│
├── src/
│   ├── extract.py
│   ├── clean.py
│   ├── transform.py
│   ├── load.py
│   └── main.py
│
├── docker-compose.yml
├── requirements.txt
├── README.md
└── ETL-G01_2026-1_Workshop-3.pdf
```

---

## 5. General Architecture

```text
Historical CSV Files
        ↓
Exploratory Data Analysis
        ↓
Batch ETL Pipeline
Extract → Clean → Transform → Load
        ↓
Processed Unified Dataset
        ↓
Machine Learning Training
        ↓
Serialized Model: model.pkl
        ↓
Kafka Producer
        ↓
Kafka Topic: happiness-predictions
        ↓
Kafka Consumer
        ↓
Raw Event Storage
        ↓
Event Validation
        ↓
Real-Time Prediction
        ↓
PostgreSQL Analytical Model
        ↓
Streamlit Dashboard
```

---

## 6. Dataset Description

The project uses World Happiness Report CSV files from five different years:

* `2015.csv`
* `2016.csv`
* `2017.csv`
* `2018.csv`
* `2019.csv`

Each file contains information related to happiness score and socioeconomic indicators such as:

* GDP
* Family / social support
* Health / life expectancy
* Freedom
* Generosity
* Corruption perception
* Happiness score
* Country information

The most important challenge was that the datasets did not share exactly the same schema. Different years used different column names for the same concepts.

---

## 7. Exploratory Data Analysis — EDA

The first step of the project was the **Exploratory Data Analysis**, developed in:

```text
notebooks/eda.ipynb
```

The EDA was performed before cleaning and transformation. This step was important because it allowed us to understand the structure and quality of the data before designing the ETL logic.

### 7.1 Dataset Shapes

The datasets had the following initial shapes:

| Year | Rows | Columns |
| ---- | ---: | ------: |
| 2015 |  158 |      12 |
| 2016 |  157 |      13 |
| 2017 |  155 |      12 |
| 2018 |  156 |       9 |
| 2019 |  156 |       9 |

This showed that the datasets had similar row counts but different numbers of columns.

---

### 7.2 Schema Comparison

During EDA, each dataset column structure was reviewed.

The main finding was that the datasets used different column names for the same information.

Examples:

| Concept                 | 2015 / 2016                     | 2017                            | 2018 / 2019                    | Unified Name      |
| ----------------------- | ------------------------------- | ------------------------------- | ------------------------------ | ----------------- |
| Country                 | `Country`                       | `Country`                       | `Country or region`            | `country`         |
| Rank                    | `Happiness Rank`                | `Happiness.Rank`                | `Overall rank`                 | `happiness_rank`  |
| Score                   | `Happiness Score`               | `Happiness.Score`               | `Score`                        | `happiness_score` |
| GDP                     | `Economy (GDP per Capita)`      | `Economy..GDP.per.Capita.`      | `GDP per capita`               | `gdp`             |
| Family / Social Support | `Family`                        | `Family`                        | `Social support`               | `family`          |
| Health                  | `Health (Life Expectancy)`      | `Health..Life.Expectancy.`      | `Healthy life expectancy`      | `health`          |
| Freedom                 | `Freedom`                       | `Freedom`                       | `Freedom to make life choices` | `freedom`         |
| Corruption              | `Trust (Government Corruption)` | `Trust..Government.Corruption.` | `Perceptions of corruption`    | `corruption`      |

This schema comparison justified the need for a transformation and harmonization phase.

---

### 7.3 Missing Values Analysis

Missing values were analyzed for every dataset.

Result:

* 2015: no missing values
* 2016: no missing values
* 2017: no missing values
* 2018: one missing value in `Perceptions of corruption`
* 2019: no missing values

Since only one missing value was found in the 2018 dataset, specifically in the corruption perception feature, the incomplete row was removed during the cleaning phase.

This decision was made because removing one row did not significantly affect the dataset size and avoided introducing artificial values through imputation.

---

### 7.4 Duplicate Analysis

Duplicate rows were checked in every dataset.

Result:

| Year | Duplicate Rows |
| ---- | -------------: |
| 2015 |              0 |
| 2016 |              0 |
| 2017 |              0 |
| 2018 |              0 |
| 2019 |              0 |

Even though no duplicates were found, the cleaning pipeline still includes `drop_duplicates()` to make the ETL process more robust and reusable.

---

### 7.5 Data Type Analysis

The EDA also reviewed data types.

Main findings:

* Country columns were text/object columns.
* Rank columns were integer values.
* Score and indicator columns were numeric float values.
* The datasets were mostly consistent in terms of numerical data types.

Even though the data types were mostly correct, the transformation phase standardizes the final types to guarantee consistency in the unified dataset.

---

### 7.6 Statistical Summary

Descriptive statistics were generated for all years.

The analysis showed that the happiness score values were within an expected range, approximately between 2 and 8. Numerical indicators such as GDP, health, family, freedom, generosity, and corruption were also reviewed.

This helped verify that the data was valid for machine learning preparation.

---

### 7.7 Outlier Analysis

A boxplot was created using the 2015 dataset as a representative outlier analysis.

The purpose was to visually inspect numerical distributions such as:

* Happiness Score
* GDP
* Family
* Health
* Freedom
* Generosity

Some values appeared distant from the majority, especially in generosity and social indicators, but they were considered valid country-level variations rather than data quality errors.

---

### 7.8 EDA Conclusions

The EDA showed that the main data quality problem was not duplicated records or corrupted data types. The main problem was schema inconsistency across years.

The EDA directly guided the cleaning and transformation decisions:

* Remove only the missing value found in 2018.
* Keep duplicate removal as a robust cleaning step.
* Standardize all column names.
* Add a `year` column.
* Select only the columns needed for machine learning and streaming.
* Merge all years into one analytical dataset.

---

## 8. Batch ETL Pipeline

The ETL pipeline was implemented in the `src/` folder using a modular architecture.

```text
src/
├── extract.py
├── clean.py
├── transform.py
├── load.py
└── main.py
```

The `main.py` script orchestrates the complete batch ETL process.

---

## 9. Extract Phase

File:

```text
src/extract.py
```

The extraction phase reads the five CSV files from the raw data folder and loads them into Pandas DataFrames.

The function returns a dictionary where each key is the year and each value is the corresponding DataFrame.

This allows the next pipeline steps to process each year independently before merging them.

---

## 10. Cleaning Phase

File:

```text
src/clean.py
```

The cleaning phase applies the following operations:

### 10.1 Remove Duplicates

All datasets are processed with:

```python
df = df.drop_duplicates()
```

This removes complete duplicate rows if they exist.

Even though the EDA showed zero duplicates, this step is kept because it makes the pipeline more reliable if new files are added later.

---

### 10.2 Remove Missing Value from 2018

The EDA found one missing value in:

```text
Perceptions of corruption
```

Only the 2018 dataset contained this issue, so the cleaning step removes only rows with missing values in that specific column:

```python
df = df.dropna(subset=["Perceptions of corruption"])
```

After cleaning, the 2018 dataset changed from:

```text
(156, 9)
```

to:

```text
(155, 9)
```

This confirms that only the incomplete row was removed.

---

## 11. Transformation and Harmonization Phase

File:

```text
src/transform.py
```

The transformation phase is one of the most important parts of the project because the original CSV files did not share the same schema.

The transformation process includes:

* Standardizing column names
* Adding a `year` column
* Selecting a unified schema
* Standardizing data types
* Merging all yearly datasets

---

### 11.1 Unified Analytical Schema

The final unified schema is:

| Column            | Description                           |
| ----------------- | ------------------------------------- |
| `country`         | Country name                          |
| `year`            | Dataset year                          |
| `happiness_rank`  | Happiness ranking position            |
| `happiness_score` | Actual happiness score                |
| `gdp`             | GDP contribution                      |
| `family`          | Family / social support contribution  |
| `health`          | Health / life expectancy contribution |
| `freedom`         | Freedom contribution                  |
| `generosity`      | Generosity contribution               |
| `corruption`      | Corruption perception contribution    |

---

### 11.2 Schema Harmonization

Different column names were mapped into the same standardized names.

Example:

```text
Economy (GDP per Capita)
Economy..GDP.per.Capita.
GDP per capita
```

were standardized as:

```text
gdp
```

This same logic was applied to score, rank, health, family/social support, freedom, generosity, and corruption.

---

### 11.3 Year Column

A new `year` column was added to preserve temporal information after merging the datasets.

This is important because once all datasets are merged, the model and dashboard still need to know which year each record belongs to.

---

### 11.4 Data Type Standardization

The transformation phase also standardizes data types.

* `country` is converted to string.
* `year` is converted to integer.
* Numerical columns are converted to float.

This ensures consistency before model training and Kafka streaming.

---

### 11.5 Final Dataset Shape

After extraction, cleaning, transformation, and merging, the final dataset had:

```text
781 rows × 10 columns
```

This dataset was saved as:

```text
data/processed/final_happiness_dataset.csv
```

---

## 12. Load Phase

File:

```text
src/load.py
```

The load phase stores the final processed dataset in the `data/processed/` folder.

The processed dataset is later used by:

* the machine learning training notebook
* the Kafka producer

---

## 13. Feature Engineering

Feature engineering was performed in:

```text
notebooks/model_training.ipynb
```

Before training the model, descriptive statistics and visualizations were generated to understand relationships between variables.

---

### 13.1 Features Selected

The selected features were:

* `gdp`
* `family`
* `health`
* `freedom`
* `generosity`
* `corruption`

The target variable was:

* `happiness_score`

---

### 13.2 Feature Selection Justification

These features were selected because they are meaningful indicators of happiness and are consistently available across the unified dataset.

They also match the JSON structure required later by the Kafka producer and consumer.

---

### 13.3 Avoiding Target Leakage

The column `happiness_rank` was not used as a feature.

Reason:

`happiness_rank` is directly related to the happiness score. Using it could introduce target leakage because the ranking depends on the target variable itself.

The `country` column was also excluded from the model because it is categorical and the workshop focuses on simple pipeline integration rather than advanced encoding or model optimization.

The `year` column was kept in the dataset for analytical context but was not used as a main predictive feature.

---

### 13.4 Scaling Decision

Feature scaling was not applied.

Reason:

The selected model was Linear Regression and the numerical values were already in relatively controlled ranges. Also, the workshop focuses on the integration of ETL, Kafka, ML, and databases rather than model optimization.

---

## 14. Machine Learning Model Training

Notebook:

```text
notebooks/model_training.ipynb
```

---

### 14.1 Model Used

The model selected was:

```text
Linear Regression
```

The workshop suggested simple regression models such as:

* Linear Regression
* Decision Tree Regressor
* Random Forest Regressor

Linear Regression was selected because it is simple, interpretable, fast, and suitable for demonstrating real-time inference in a streaming pipeline.

---

### 14.2 Train/Test Split

The data was split into:

* 70% training data
* 30% testing data

The resulting split was:

```text
X_train: 546 rows × 6 features
X_test: 235 rows × 6 features
```

---

### 14.3 Model Evaluation Metrics

The model was evaluated using:

* MAE
* RMSE
* R²

Approximate results:

| Metric | Result | Interpretation                                              |
| ------ | -----: | ----------------------------------------------------------- |
| MAE    |   0.43 | Average prediction error was around 0.43 points             |
| RMSE   |   0.56 | Overall prediction error remained relatively low            |
| R²     |   0.75 | The model explained around 75% of happiness score variation |

---

### 14.4 Model Evaluation Explanation

The MAE result means that, on average, predictions are approximately 0.43 points away from the real happiness score.

The RMSE value shows that the model does not produce very large errors in most cases.

The R² value of approximately 0.75 means that the selected features explain around 75% of the variation in happiness score.

This performance is good enough for the objective of the workshop because the main goal is not maximum ML accuracy, but building a reliable streaming ETL pipeline.

---

### 14.5 Model Serialization

The trained model was saved as:

```text
models/model.pkl
```

This serialized model is loaded by the Kafka consumer to generate real-time predictions.

---

## 15. Apache Kafka Streaming Pipeline

The streaming part of the project is implemented in the `kafka/` folder.

```text
kafka/
├── producer.py
└── consumer.py
```

---

## 16. Kafka Topic

The required Kafka topic is:

```text
happiness-predictions
```

The topic receives JSON events generated by the producer.

---

## 17. Kafka Producer

File:

```text
kafka/producer.py
```

The producer reads the processed dataset and streams records one by one into Kafka.

Each row is converted into the required JSON format.

---

### 17.1 Event Format

Example event:

```json
{
  "country": "Colombia",
  "year": 2019,
  "gdp": 1.2,
  "family": 0.8,
  "health": 0.9,
  "freedom": 0.6,
  "generosity": 0.3,
  "corruption": 0.1,
  "actual_happiness_score": 6.2
}
```

---

### 17.2 Producer Responsibilities

The producer:

* Reads `final_happiness_dataset.csv`
* Builds a JSON event for each row
* Sends each event to Kafka
* Uses the topic `happiness-predictions`
* Streams data one record at a time

---

## 18. Kafka Consumer

File:

```text
kafka/consumer.py
```

The consumer receives events from Kafka and performs the streaming inference process.

---

### 18.1 Consumer Responsibilities

The consumer:

* Receives Kafka events
* Stores the original raw event in PostgreSQL
* Validates the event schema
* Validates numerical values
* Loads the serialized model
* Generates predictions
* Stores prediction results
* Handles invalid events without crashing the pipeline

---

### 18.2 Raw Event Storage Requirement

Before validation or prediction, every original Kafka event is stored in the table:

```text
raw_happiness_events
```

This table stores the message exactly as it arrived from Kafka.

This supports:

* traceability
* auditing
* debugging
* future reprocessing

Invalid records are also stored but marked with a processing status.

---

### 18.3 Event Validation

The consumer validates:

* Missing fields
* Invalid data types
* Negative numerical values
* Missing features required by the model

Required fields:

* `country`
* `year`
* `gdp`
* `family`
* `health`
* `freedom`
* `generosity`
* `corruption`
* `actual_happiness_score`

---

### 18.4 Processing Statuses

The following statuses are used:

| Status             | Meaning                                         |
| ------------------ | ----------------------------------------------- |
| `VALID`            | The event has correct schema and values         |
| `INVALID_SCHEMA`   | The event is missing required fields            |
| `INVALID_VALUES`   | The event contains invalid data types or values |
| `PREDICTION_ERROR` | An error occurred during prediction or storage  |

Invalid records are skipped from prediction but do not crash the pipeline.

---

### 18.5 Prediction Generation

The consumer loads the trained model:

```python
model = joblib.load(MODEL_PATH)
```

Then it creates the input feature dataframe using the same feature order used during training:

```text
gdp, family, health, freedom, generosity, corruption
```

This is important because feature ordering must remain consistent between training and real-time inference.

---

## 19. PostgreSQL Database Design

The project uses PostgreSQL as the analytical database.

The SQL schema is defined in:

```text
sql/create_tables.sql
```

The database contains:

* raw table
* fact table
* dimension tables

---

### 19.1 Raw Table

```text
raw_happiness_events
```

Stores original Kafka messages.

Columns:

| Column               | Description                     |
| -------------------- | ------------------------------- |
| `raw_event_id`       | Unique raw event identifier     |
| `original_message`   | Original JSON Kafka message     |
| `processing_status`  | Processing result               |
| `error_message`      | Error message if applicable     |
| `received_timestamp` | Timestamp when event was stored |

---

### 19.2 Dimension Tables

#### dim_country

Stores country information.

| Column         | Description        |
| -------------- | ------------------ |
| `country_id`   | Country identifier |
| `country_name` | Country name       |

#### dim_date

Stores year information.

| Column    | Description       |
| --------- | ----------------- |
| `date_id` | Date identifier   |
| `year`    | Year of the event |

#### dim_raw_event

Stores metadata related to raw events.

| Column               | Description           |
| -------------------- | --------------------- |
| `raw_event_id`       | Original raw event ID |
| `processing_status`  | Event status          |
| `received_timestamp` | Raw event timestamp   |

---

### 19.3 Fact Table

```text
fact_predictions
```

Stores prediction results.

| Column                 | Description                                            |
| ---------------------- | ------------------------------------------------------ |
| `prediction_id`        | Unique prediction identifier                           |
| `raw_event_id`         | Link to original Kafka event                           |
| `country_id`           | Link to country dimension                              |
| `date_id`              | Link to date dimension                                 |
| `actual_score`         | Real happiness score                                   |
| `predicted_score`      | Model predicted score                                  |
| `prediction_error`     | Absolute difference between actual and predicted score |
| `prediction_timestamp` | Timestamp when prediction was stored                   |

---

### 19.4 Traceability

Each prediction is linked back to the exact raw Kafka event that generated it through:

```text
raw_event_id
```

This allows complete traceability from raw event to final prediction.

---

## 20. Docker Compose

The project uses Docker Compose to run:

* Zookeeper
* Kafka
* PostgreSQL

PostgreSQL is exposed on port:

```text
5433
```

This was done because port `5432` was already being used by another local PostgreSQL container.

---

## 21. Dashboard and KPIs

The dashboard was created using Streamlit.

File:

```text
dashboards/dashboard.py
```

The dashboard connects directly to PostgreSQL using `psycopg2`.

Important:

The dashboard does not read CSV files. It queries the prediction database directly.

---

### 21.1 Dashboard Query

The dashboard joins:

* `fact_predictions`
* `dim_country`
* `dim_date`

This allows the dashboard to display prediction results with country and year information.

---

### 21.2 KPI 1 — Average Prediction Error

This KPI shows the average value of `prediction_error`.

It measures how far predictions are from the actual happiness score on average.

---

### 21.3 KPI 2 — Total Predictions

This KPI shows how many prediction records were stored in the database.

It helps verify that the streaming pipeline processed the expected number of events.

---

### 21.4 KPI 3 — Countries

This KPI shows how many unique countries were processed.

---

### 21.5 Predictions by Country

A bar chart shows how many prediction records were generated per country.

---

### 21.6 Predicted vs Actual Happiness Score

A scatter plot compares the actual score against the predicted score.

This visualization helps evaluate whether the model predictions follow the real happiness score pattern.

---

### 21.7 Prediction Trends Over Time

A line chart shows the average predicted happiness score by year.

This allows analysis of prediction behavior across time.

---

### 21.8 Prediction Data Table

The dashboard also includes a table showing prediction records with:

* prediction ID
* country
* year
* actual score
* predicted score
* prediction error
* timestamp

---

## 22. Dashboard Screenshots

Dashboard screenshots should be stored in:

```text
dashboards/screenshots/
```

Suggested screenshots:

* `dashboard_main.png`
* `dashboard_kpis.png`
* `dashboard_scatter.png`
* `dashboard_trends.png`

In the final repository, these screenshots support the dashboard deliverable.

---

## 23. Final Results

The final project successfully achieved:

* EDA before cleaning and transformation
* Batch ETL pipeline
* Unified analytical schema
* Processed dataset with 781 records and 10 columns
* Linear Regression model training
* Model serialization as `model.pkl`
* Kafka producer streaming events one by one
* Kafka consumer performing validation and inference
* Raw event storage before prediction
* Invalid event handling without crashing the pipeline
* PostgreSQL analytical schema
* Prediction storage with traceability
* Streamlit dashboard connected directly to PostgreSQL

---

## 24. Challenges and Solutions

### 24.1 Kafka Container on Mac M2

Kafka initially had container issues on Mac M2.

Solution:

The Docker Compose file used the ARM platform configuration:

```yaml
platform: linux/arm64
```

---

### 24.2 PostgreSQL Port Conflict

Port `5432` was already being used by another PostgreSQL container.

Solution:

This project exposed PostgreSQL using port `5433` externally.

---

### 24.3 NumPy Float Insert Error

During prediction storage, PostgreSQL produced an error when receiving NumPy float values.

Solution:

Predicted values and prediction errors were converted to standard Python floats before insertion.

---

### 24.4 Transaction Error Handling

When an insert failed, the PostgreSQL transaction entered an aborted state.

Solution:

The consumer uses `connection.rollback()` inside the exception handling block before storing prediction errors.

---

## 25. Execution Instructions

### 25.1 Clone Repository

```bash
git clone https://github.com/alejandrocardenasa-lgtm/workshop3-streaming-etl-kafka.git
cd workshop3-streaming-etl-kafka
```

---

### 25.2 Create Virtual Environment

```bash
python3 -m venv etl_env
```

---

### 25.3 Activate Virtual Environment

```bash
source etl_env/bin/activate
```

---

### 25.4 Install Dependencies

```bash
pip install -r requirements.txt
```

---

### 25.5 Start Docker Services

```bash
docker compose up -d
```

Verify containers:

```bash
docker ps
```

Expected containers:

* `zookeeper`
* `kafka`
* `postgres_happiness`

---

### 25.6 Create PostgreSQL Tables

```bash
docker exec -i postgres_happiness psql -U admin -d happiness_db < sql/create_tables.sql
```

Verify tables:

```bash
docker exec -it postgres_happiness psql -U admin -d happiness_db
```

Inside PostgreSQL:

```sql
\dt
```

Expected tables:

* `raw_happiness_events`
* `dim_country`
* `dim_date`
* `dim_raw_event`
* `fact_predictions`

Exit with:

```sql
\q
```

---

### 25.7 Create Kafka Topic

```bash
docker exec -it kafka kafka-topics --create \
  --topic happiness-predictions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Verify topic:

```bash
docker exec -it kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

---

### 25.8 Run Batch ETL Pipeline

```bash
python src/main.py
```

Expected final result:

```text
Final dataset shape: (781, 10)
ETL pipeline completed successfully.
```

---

### 25.9 Train Model

Open and run:

```text
notebooks/model_training.ipynb
```

This notebook trains the model and saves:

```text
models/model.pkl
```

---

### 25.10 Run Kafka Consumer

Open one terminal and run:

```bash
python kafka/consumer.py
```

The consumer will wait for Kafka events.

---

### 25.11 Run Kafka Producer

Open another terminal and run:

```bash
python kafka/producer.py
```

The producer will send records one by one to Kafka.

---

### 25.12 Verify PostgreSQL Data

Enter PostgreSQL:

```bash
docker exec -it postgres_happiness psql -U admin -d happiness_db
```

Check raw events:

```sql
SELECT raw_event_id, processing_status, received_timestamp
FROM raw_happiness_events
LIMIT 10;
```

Check predictions:

```sql
SELECT *
FROM fact_predictions
LIMIT 10;
```

Exit:

```sql
\q
```

---

### 25.13 Run Dashboard

```bash
streamlit run dashboards/dashboard.py
```

The dashboard will open in the browser and display KPIs and charts connected directly to PostgreSQL.

---

## 26. Repository Deliverables

This repository includes:

* ETL notebooks
* EDA notebook
* Model training notebook
* Kafka producer script
* Kafka consumer script
* Serialized model
* SQL database schema
* Streamlit dashboard
* Dashboard screenshots folder
* requirements.txt
* Docker Compose file
* README.md

---

## 27. Conclusion

This project demonstrates a complete real-time data engineering and machine learning workflow.

It starts with raw historical CSV files, performs EDA, cleans and harmonizes the data, trains a regression model, streams events through Kafka, validates incoming records, stores raw and prediction data in PostgreSQL, and visualizes the final results in a dashboard connected directly to the database.

The final result is an integrated streaming ETL pipeline capable of generating real-time predictions while maintaining traceability, data quality, and analytical usability.
