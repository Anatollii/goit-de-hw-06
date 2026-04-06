import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import kafka_config


MY_NAME = "anatoliy"
SENSORS_TOPIC = f"{MY_NAME}_building_sensors"

TEMPERATURE_MIN = 25
TEMPERATURE_MAX = 45

HUMIDITY_MIN = 15
HUMIDITY_MAX = 85

SLEEP_SECONDS = 2


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8")
    )

# def generate_sensor_data(sensor_id: int) -> dict:
#     return {
#         "id": sensor_id,
#         "temperature": 35,
#         "humidity": 80,
#         "timestamp": datetime.now(timezone.utc).isoformat()
#     }


def generate_sensor_data(sensor_id: int) -> dict:
    return {
        "id": sensor_id,
        "temperature": random.randint(TEMPERATURE_MIN, TEMPERATURE_MAX),
        "humidity": random.randint(HUMIDITY_MIN, HUMIDITY_MAX),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def main() -> None:
    producer = create_producer()
    sensor_id = random.randint(1000, 9999)

    print(f"Producer started for sensor_id={sensor_id}")

    try:
        while True:
            data = generate_sensor_data(sensor_id)

            producer.send(
                SENSORS_TOPIC,
                key=str(sensor_id),
                value=data
            )
            producer.flush()

            print(f"Sent: {data}")
            time.sleep(SLEEP_SECONDS)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()