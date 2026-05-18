import json
import time
import pandas as pd
from kafka import KafkaProducer


TOPIC_NAME = "happiness-predictions"
BOOTSTRAP_SERVERS = "localhost:9092"
DATA_PATH = "data/processed/final_happiness_dataset.csv"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8")
    )


def build_event(row):
    return {
        "country": row["country"],
        "year": int(row["year"]),
        "gdp": float(row["gdp"]),
        "family": float(row["family"]),
        "health": float(row["health"]),
        "freedom": float(row["freedom"]),
        "generosity": float(row["generosity"]),
        "corruption": float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"])
    }


def stream_events():
    df = pd.read_csv(DATA_PATH)
    producer = create_producer()

    print("Starting Kafka producer...\n")

    for _, row in df.iterrows():
        event = build_event(row)

        producer.send(TOPIC_NAME, value=event)
        producer.flush()

        print(f"Event sent: {event}")

        time.sleep(1)

    print("\nAll events were sent successfully.")


if __name__ == "__main__":
    stream_events()