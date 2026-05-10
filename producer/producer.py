import csv
import json
import os
import time
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "sales_raw")
DATA_DIR = Path(os.getenv("DATA_DIR", "../исходные данные"))


def create_producer() -> KafkaProducer:
    last_error = None
    for _ in range(60):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda value: value.encode("utf-8"),
                acks="all",
                retries=5,
            )
        except NoBrokersAvailable as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Kafka broker is unavailable: {last_error}")


def csv_files() -> list[Path]:
    files = sorted(DATA_DIR.glob("MOCK_DATA*.csv"))
    if not files:
        raise FileNotFoundError(f"No MOCK_DATA*.csv files found in {DATA_DIR}")
    return files


def main() -> None:
    producer = create_producer()
    total = 0

    for file_path in csv_files():
        with file_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            for row_number, row in enumerate(reader, start=1):
                row["source_file"] = file_path.name
                row["source_row_number"] = row_number
                event_id = f"{file_path.stem}:{row.get('id', row_number)}"
                row["sale_event_id"] = event_id
                producer.send(TOPIC, key=event_id, value=row)
                total += 1

    producer.flush()
    producer.close()
    print(f"Sent {total} messages to Kafka topic '{TOPIC}'")


if __name__ == "__main__":
    main()
