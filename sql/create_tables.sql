CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id SERIAL PRIMARY KEY,
    original_message JSONB NOT NULL,
    processing_status VARCHAR(50) NOT NULL,
    error_message TEXT,
    received_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_raw_event (
    raw_event_id INT PRIMARY KEY,
    processing_status VARCHAR(50) NOT NULL,
    received_timestamp TIMESTAMP NOT NULL,
    FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events(raw_event_id)
);

CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    raw_event_id INT NOT NULL,
    country_id INT NOT NULL,
    date_id INT NOT NULL,
    actual_score FLOAT NOT NULL,
    predicted_score FLOAT NOT NULL,
    prediction_error FLOAT NOT NULL,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (raw_event_id) REFERENCES raw_happiness_events(raw_event_id),
    FOREIGN KEY (country_id) REFERENCES dim_country(country_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
);