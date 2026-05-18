# Workshop 3 — Streaming ETL Pipeline with Kafka and Machine Learning

# Happiness Prediction Streaming Architecture

## Author

Alejandro Cardenas

---

# Project Overview

This project implements a complete end-to-end Streaming ETL pipeline capable of processing happiness data, generating machine learning predictions in real time, storing analytical results in PostgreSQL, and visualizing KPIs through a live dashboard.

The project combines concepts from:

* Data Engineering
* ETL Pipelines
* Apache Kafka Streaming
* Machine Learning
* PostgreSQL Data Warehousing
* Real-Time Analytics
* Dashboard Visualization

The objective of the project is to simulate a modern real-time analytics architecture where data is continuously streamed, validated, transformed, predicted, stored, and analyzed.

The pipeline was developed entirely in Python using Kafka, PostgreSQL, Scikit-learn, Docker Compose, and Streamlit.

---

# Project Objectives

The main objectives of the project were:

* Build an ETL pipeline for happiness data
* Clean and standardize raw datasets
* Train a regression model capable of predicting happiness scores
* Stream records using Apache Kafka
* Validate events before prediction
* Store raw Kafka events for traceability
* Store prediction results in analytical tables
* Build a live dashboard connected directly to PostgreSQL
* Generate analytical KPIs from streaming predictions

---

# Technologies Used

| Technology     | Purpose                            |
| -------------- | ---------------------------------- |
| Python         | Main programming language          |
| Pandas         | Data processing and transformation |
| Scikit-learn   | Machine learning model training    |
| Apache Kafka   | Streaming platform                 |
| Kafka Python   | Kafka producer and consumer        |
| PostgreSQL     | Analytical database                |
| Docker Compose | Container orchestration            |
| Streamlit      | Dashboard visualization            |
| Plotly         | Interactive charts                 |
| Psycopg2       | PostgreSQL connection              |
| Joblib         | Model serialization                |

---

# Project Structure

```text
workshop3_streaming_etl/
│
├── data/
│   ├── raw/
│   │   ├── 2015.csv
│   │   ├── 2016.csv
│   │   ├── 2017.csv
│   │   ├── 2018.csv
│   │   └── 2019.csv
│   │
│   └── processed/
│       └── final_happiness_dataset.csv
│
├── notebooks/
│   ├── eda.ipynb
│   └── model_training.ipynb
│
├── src/
│   ├── extract.py
│   ├── clean.py
│   ├── transform.py
│   ├── load.py
│   └── main.py
│
├── kafka/
│   ├── producer.py
│   └── consumer.py
│
├── dashboards/
│   ├── dashboard.py
│   └── screenshots/
│
├── sql/
│   └── create_tables.sql
│
├── models/
│   └── model.pkl
│
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

# Dataset Description

The project uses World Happiness Report datasets from 2015 to 2019.

The datasets include variables related to:

* GDP
* Family support
* Health
* Freedom
* Generosity
* Corruption
* Happiness score

Each yearly dataset was merged into a unified analytical dataset.

---

# ETL Pipeline

The ETL process was implemented inside the `src/` folder.

The ETL flow follows:

Extract → Clean → Transform → Load

---

# Extract Phase

File:

```text
src/extract.py
```

During the extraction phase:

* CSV files from 2015–2019 were loaded
* Raw datasets were read using Pandas
* Multiple yearly files were combined into a single dataframe

Main tasks:

* Read CSV files
* Verify columns
* Merge yearly datasets

---

# Data Cleaning Phase

File:

```text
src/clean.py
```

Several cleaning decisions were applied to improve data quality.

## Cleaning Decisions

### 1. Country Standardization

Country names contained inconsistencies.

Examples:

* bogota → Bogotá
* medellin → Medellín
* barranquila → Barranquilla

Mapping dictionaries were used to standardize names.

---

### 2. Missing Values

Rows containing null values in important columns were removed.

Important numerical features required by the model could not contain missing values.

---

### 3. Duplicate Records

Duplicated rows were removed to avoid incorrect predictions and duplicated streaming events.

---

### 4. Data Type Corrections

Numerical columns were converted into proper numeric types.

Examples:

* GDP
* Family
* Health
* Freedom
* Generosity
* Corruption
* Happiness score

---

### 5. Text Normalization

Country names were normalized:

* lowercase conversion
* trimming spaces
* formatting corrections

---

# Transformation Phase

File:

```text
src/transform.py
```

The transformation phase prepared the dataset for machine learning and streaming.

## Feature Selection

Selected features:

* gdp
* family
* health
* freedom
* generosity
* corruption

Target variable:

* happiness_score

---

## Feature Engineering Decisions

Feature selection was based on:

* correlation analysis
* domain relevance
* prediction usefulness

The project intentionally used a simple feature engineering approach because the workshop focuses more on pipeline integration than advanced ML optimization.

---

# Load Phase

File:

```text
src/load.py
```

The final cleaned dataset was saved into:

```text
data/processed/final_happiness_dataset.csv
```

This dataset became the source for:

* Machine learning training
* Kafka producer streaming

---

# Exploratory Data Analysis (EDA)

Notebook:

```text
notebooks/eda.ipynb
```

Several visualizations and analyses were created.

## Analyses Performed

* Distribution of happiness scores
* Correlation analysis
* GDP vs happiness
* Health vs happiness
* Feature relationships

---

## Important Findings

### Health vs Happiness Score

A strong positive relationship was observed between health and happiness score.

Countries with higher health indexes generally showed higher happiness scores.

### GDP vs Happiness Score

GDP also showed strong correlation with happiness.

### Freedom and Family Support

Freedom and family support positively influenced happiness predictions.

---

# Machine Learning Model

Notebook:

```text
notebooks/model_training.ipynb
```

---

# Model Selection

The selected model was:

## Linear Regression

The workshop suggested:

* Linear Regression
* Random Forest Regressor
* Decision Tree Regressor

A Linear Regression model was selected because:

* it is simple
* easy to interpret
* lightweight for streaming inference
* appropriate for workshop objectives

---

# Train/Test Split

The dataset was divided into:

* 70% training data
* 30% testing data

Code implementation:

```python
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.30,
    random_state=42
)
```

---

# Model Training

The model was trained using Scikit-learn.

Main features used:

* GDP
* Family
* Health
* Freedom
* Generosity
* Corruption

Target:

* happiness_score

---

# Model Evaluation

The following evaluation metrics were used:

| Metric | Result |
| ------ | ------ |
| MAE    | ~0.43  |
| RMSE   | ~0.56  |
| R²     | ~0.75  |

---

# Evaluation Interpretation

## MAE

The Mean Absolute Error indicates that predictions are, on average, approximately 0.43 points away from the actual happiness score.

---

## RMSE

The RMSE value shows that the model prediction error remains relatively low.

---

## R² Score

The R² value close to 0.75 means that the model explains approximately 75% of the variability in happiness scores.

This indicates a strong relationship between the selected features and the target variable.

---

# Model Serialization

The trained model was saved using Joblib.

Saved file:

```text
models/model.pkl
```

This serialized model is later loaded by the Kafka consumer for real-time inference.

---

# Apache Kafka Streaming Pipeline

The project implements a streaming architecture using Apache Kafka.

---

# Kafka Architecture

```text
Producer
↓
Kafka Topic
↓
Consumer
↓
Validation
↓
Prediction
↓
PostgreSQL
↓
Dashboard
```

---

# Docker Compose

Kafka, Zookeeper, and PostgreSQL were containerized using Docker Compose.

Main containers:

* Zookeeper
* Kafka
* PostgreSQL

---

# Kafka Topic

Topic name:

```text
happiness-predictions
```

The topic receives streaming happiness events serialized as JSON.

---

# Kafka Producer

File:

```text
kafka/producer.py
```

The producer performs the following tasks:

* Reads the processed dataset
* Streams records one by one
* Converts records into JSON format
* Sends events into Kafka topic

---

# Producer Event Format

Example streamed event:

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

# Kafka Consumer

File:

```text
kafka/consumer.py
```

The consumer performs several critical tasks.

## Consumer Responsibilities

* Receive Kafka events
* Validate incoming data
* Store raw events
* Generate ML predictions
* Store prediction results
* Handle invalid records
* Prevent pipeline crashes

---

# Event Validation

The consumer validates:

* Missing fields
* Invalid data types
* Invalid numerical values
* Missing prediction features

---

# Validation Statuses

The following statuses were implemented:

| Status           | Description             |
| ---------------- | ----------------------- |
| VALID            | Correct event           |
| INVALID_SCHEMA   | Missing fields          |
| INVALID_VALUES   | Invalid values or types |
| PREDICTION_ERROR | Prediction failure      |

---

# Raw Event Storage

All incoming Kafka events are stored before prediction.

Table:

```text
raw_happiness_events
```

This allows:

* auditing
* traceability
* debugging
* future reprocessing

Even invalid records are stored.

---

# PostgreSQL Analytical Database

The project uses PostgreSQL for storing streaming predictions.

---

# Database Schema

The analytical model contains:

## Raw Table

### raw_happiness_events

Stores original Kafka messages.

Columns:

* raw_event_id
* original_message
* processing_status
* error_message
* received_timestamp

---

## Fact Table

### fact_predictions

Stores prediction results.

Columns:

* prediction_id
* raw_event_id
* country_id
* date_id
* actual_score
* predicted_score
* prediction_error
* prediction_timestamp

---

## Dimension Tables

### dim_country

Stores country information.

### dim_date

Stores year and date information.

### dim_raw_event

Stores metadata related to raw events.

---

# Prediction Logic

The Kafka consumer loads the serialized model:

```python
model = joblib.load(MODEL_PATH)
```

The consumer extracts features from incoming events and generates predictions.

Prediction error is calculated as:

```python
prediction_error = abs(actual_score - predicted_score)
```

Results are inserted into:

```text
fact_predictions
```

---

# Dashboard

The dashboard was built using Streamlit.

File:

```text
dashboards/dashboard.py
```

The dashboard connects directly to PostgreSQL.

Important:

The dashboard does NOT use CSV files.

All visualizations query PostgreSQL live.

---

# Dashboard KPIs

The dashboard includes the required KPIs.

---

## 1. Average Prediction Error

Shows the average prediction error generated by the model.

This KPI helps evaluate prediction quality.

---

## 2. Total Predictions

Displays the total number of predictions processed by the streaming pipeline.

---

## 3. Countries

Shows the total number of unique countries processed.

---

## 4. Predictions by Country

Bar chart showing the number of predictions generated per country.

---

## 5. Predicted vs Actual Happiness Score

Scatterplot comparing:

* actual happiness score
* predicted happiness score

The chart demonstrates the correlation between predictions and actual values.

---

## 6. Prediction Trends Over Time

Line chart showing how predicted happiness scores evolve over time.

Years included:

* 2015
* 2016
* 2017
* 2018
* 2019

---

## 7. Prediction Data Table

Interactive table displaying:

* prediction_id
* country
* year
* actual score
* predicted score
* prediction error
* timestamp

---

# Dashboard Screenshots

Screenshots are stored in:

```text
dashboards/screenshots/
```

Included screenshots:

* main dashboard

---

# Challenges Faced During Development

Several technical challenges were encountered.

## Kafka Container Issues

Kafka containers initially failed due to architecture compatibility on Mac M2.

The issue was solved using:

```yaml
platform: linux/arm64
```

inside Docker Compose.

---

## NumPy Float PostgreSQL Error

PostgreSQL initially failed when inserting NumPy float values.

Solution:

Predictions were converted into standard Python float values.

---

# Final Results

The final solution successfully achieved:

* Complete ETL pipeline
* Kafka real-time streaming
* Real-time machine learning predictions
* Raw event persistence
* PostgreSQL analytical storage
* Data warehouse design
* Live dashboard analytics
* End-to-end pipeline integration

The final architecture simulates a real-world streaming machine learning system.

---

# Execution Instructions

## 1. Create Virtual Environment

```bash
python -m venv etl_env
```

---

## 2. Activate Environment

Mac/Linux:

```bash
source etl_env/bin/activate
```

Windows:

```bash
etl_env\Scripts\activate
```

---

## 3. Install Dependencies

```bash
pip install -r requirements.txt
```

---

## 4. Start Docker Services

```bash
docker compose up -d
```

---

## 5. Create Kafka Topic

```bash
docker exec -it kafka kafka-topics --create \
  --topic happiness-predictions \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

---

## 6. Run ETL Pipeline

```bash
python src/main.py
```

---

## 7. Train the Model

Open:

```text
notebooks/model_training.ipynb
```

Execute all notebook cells.

---

## 8. Run Kafka Consumer

```bash
python kafka/consumer.py
```

---

## 9. Run Kafka Producer

```bash
python kafka/producer.py
```

---

## 10. Run Dashboard

```bash
streamlit run dashboards/dashboard.py
```

---

# Repository Deliverables

The repository includes:

* ETL notebooks
* Producer and consumer scripts
* Serialized model
* SQL scripts
* Dashboard files
* Dashboard screenshots
* requirements.txt
* README.md
* Docker Compose configuration
* PostgreSQL schema
* Streamlit dashboard

---

# Conclusion

This project demonstrates how modern data engineering, machine learning, and streaming technologies can be integrated into a complete analytical pipeline.

The final solution successfully combines:

* ETL processing
* Kafka streaming
* Machine learning inference
* PostgreSQL warehousing
* Real-time analytics

into a scalable and traceable architecture.

The project also demonstrates practical implementation of validation, prediction tracking, streaming analytics, and dashboard monitoring in a real-time environment.
