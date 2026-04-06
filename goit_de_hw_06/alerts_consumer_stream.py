import json

from kafka import KafkaConsumer

from configs import kafka_config


MY_NAME = "anatoliy"
ALERTS_TOPIC = f"{MY_NAME}_alerts"


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="anatoliy_alerts_stream_consumer_group"
    )


def main() -> None:
    consumer = create_consumer()

    print("Alerts consumer started...")

    try:
        for message in consumer:
            print(f"Topic: {message.topic}")
            print(f"Key: {message.key}")
            print("Value:")
            print(json.dumps(message.value, indent=2, ensure_ascii=False))
            print("-" * 50)

    except KeyboardInterrupt:
        print("\nAlerts consumer stopped by user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()