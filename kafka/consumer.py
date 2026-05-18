import json
import joblib
import psycopg2
import pandas as pd

from kafka import KafkaConsumer


TOPIC_NAME = "happiness-predictions"
BOOTSTRAP_SERVERS = "localhost:9092"
MODEL_PATH = "models/model.pkl"


DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "happiness_db",
    "user": "admin",
    "password": "admin"
}


REQUIRED_FIELDS = [
    "country",
    "year",
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
    "actual_happiness_score"
]


def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )


def connect_postgres():
    return psycopg2.connect(**DB_CONFIG)


def store_raw_event(connection, event, status, error_message=None):
    cursor = connection.cursor()

    query = """
    INSERT INTO raw_happiness_events (
        original_message,
        processing_status,
        error_message
    )
    VALUES (%s, %s, %s)
    RETURNING raw_event_id
    """

    cursor.execute(
        query,
        (
            json.dumps(event),
            status,
            error_message
        )
    )

    raw_event_id = cursor.fetchone()[0]

    connection.commit()
    cursor.close()

    return raw_event_id


def insert_dim_country(connection, country_name):
    cursor = connection.cursor()

    query = """
    INSERT INTO dim_country (country_name)
    VALUES (%s)
    ON CONFLICT (country_name) DO NOTHING
    """

    cursor.execute(query, (country_name,))
    connection.commit()

    cursor.execute(
        "SELECT country_id FROM dim_country WHERE country_name = %s",
        (country_name,)
    )

    country_id = cursor.fetchone()[0]
    cursor.close()

    return country_id


def insert_dim_date(connection, year):
    cursor = connection.cursor()

    query = """
    INSERT INTO dim_date (year)
    VALUES (%s)
    ON CONFLICT (year) DO NOTHING
    """

    cursor.execute(query, (year,))
    connection.commit()

    cursor.execute(
        "SELECT date_id FROM dim_date WHERE year = %s",
        (year,)
    )

    date_id = cursor.fetchone()[0]
    cursor.close()

    return date_id


def insert_dim_raw_event(connection, raw_event_id):
    cursor = connection.cursor()

    query = """
    INSERT INTO dim_raw_event (
        raw_event_id,
        processing_status,
        received_timestamp
    )
    SELECT
        raw_event_id,
        processing_status,
        received_timestamp
    FROM raw_happiness_events
    WHERE raw_event_id = %s
    ON CONFLICT (raw_event_id) DO NOTHING
    """

    cursor.execute(query, (raw_event_id,))
    connection.commit()
    cursor.close()


def insert_fact_prediction(
    connection,
    raw_event_id,
    country_id,
    date_id,
    actual_score,
    predicted_score
):
    actual_score = float(actual_score)
    predicted_score = float(predicted_score)
    prediction_error = float(abs(actual_score - predicted_score))

    cursor = connection.cursor()

    query = """
    INSERT INTO fact_predictions (
        raw_event_id,
        country_id,
        date_id,
        actual_score,
        predicted_score,
        prediction_error
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    cursor.execute(
        query,
        (
            raw_event_id,
            country_id,
            date_id,
            actual_score,
            predicted_score,
            prediction_error
        )
    )

    connection.commit()
    cursor.close()


def validate_event(event):
    for field in REQUIRED_FIELDS:
        if field not in event:
            return False, "INVALID_SCHEMA", f"Missing field: {field}"

    if not isinstance(event["country"], str):
        return False, "INVALID_VALUES", "Invalid type for country"

    if not isinstance(event["year"], int):
        return False, "INVALID_VALUES", "Invalid type for year"

    numeric_fields = [
        "gdp",
        "family",
        "health",
        "freedom",
        "generosity",
        "corruption",
        "actual_happiness_score"
    ]

    for field in numeric_fields:
        value = event[field]

        if not isinstance(value, (int, float)):
            return False, "INVALID_VALUES", f"Invalid type for {field}"

        if value < 0:
            return False, "INVALID_VALUES", f"Negative value in {field}"

    return True, "VALID", None


def make_prediction(model, event):
    input_data = pd.DataFrame([{
        "gdp": event["gdp"],
        "family": event["family"],
        "health": event["health"],
        "freedom": event["freedom"],
        "generosity": event["generosity"],
        "corruption": event["corruption"]
    }])

    prediction = model.predict(input_data)[0]

    return float(round(prediction, 3))


def consume_events():
    print("Starting Kafka consumer...\n")

    consumer = create_consumer()
    connection = connect_postgres()
    model = joblib.load(MODEL_PATH)

    for message in consumer:
        event = message.value

        print(f"Received event: {event}")

        try:
            is_valid, status, error_message = validate_event(event)

            raw_event_id = store_raw_event(
                connection,
                event,
                status,
                error_message
            )

            if not is_valid:
                print(f"Invalid event skipped: {error_message}\n")
                continue

            prediction = make_prediction(model, event)

            country_id = insert_dim_country(connection, event["country"])
            date_id = insert_dim_date(connection, event["year"])

            insert_dim_raw_event(connection, raw_event_id)

            insert_fact_prediction(
                connection,
                raw_event_id,
                country_id,
                date_id,
                event["actual_happiness_score"],
                prediction
            )

            print(f"Predicted happiness score: {prediction}")
            print("Prediction stored successfully.\n")

        except Exception as e:
            connection.rollback()
            
            store_raw_event(
                connection,
                event,
                "PREDICTION_ERROR",
                str(e)
            )

            print(f"Prediction error: {e}\n")


if __name__ == "__main__":
    consume_events()