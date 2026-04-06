from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config


MY_NAME = "anatoliy"

BUILDING_SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
TEMPERATURE_ALERTS_TOPIC = f"{MY_NAME}_temperature_alerts"
HUMIDITY_ALERTS_TOPIC = f"{MY_NAME}_humidity_alerts"
ALERTS_TOPIC = f"{MY_NAME}_alerts"

NUM_PARTITIONS = 2
REPLICATION_FACTOR = 1


def create_admin_client() -> KafkaAdminClient:
    return KafkaAdminClient(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"]
    )


def build_topics() -> list[NewTopic]:
    return [
        NewTopic(
            name=BUILDING_SENSORS_TOPIC,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        ),
        NewTopic(
            name=TEMPERATURE_ALERTS_TOPIC,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        ),
        NewTopic(
            name=HUMIDITY_ALERTS_TOPIC,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        ),
        NewTopic(
            name=ALERTS_TOPIC,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        ),
    ]


def main() -> None:
    admin_client = create_admin_client()

    try:
        topics_to_create = build_topics()
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print("Topics created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        required_topics = [
            BUILDING_SENSORS_TOPIC,
            TEMPERATURE_ALERTS_TOPIC,
            HUMIDITY_ALERTS_TOPIC,
            ALERTS_TOPIC
        ]

        existing_topics = admin_client.list_topics()
        my_topics = [topic for topic in required_topics if topic in existing_topics]
        print(my_topics)
        admin_client.close()


if __name__ == "__main__":
    main()