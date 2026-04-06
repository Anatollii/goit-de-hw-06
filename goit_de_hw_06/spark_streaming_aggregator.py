from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import to_json, struct, current_timestamp

from configs import kafka_config


MY_NAME = "anatoliy"
SENSORS_TOPIC = f"{MY_NAME}_building_sensors"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SensorStreamingAggregation")
        .master("local[*]")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    alerts_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("goit_de_hw_06/alerts_conditions.csv")
    )

    alerts_df.printSchema()
    alerts_df.show(truncate=False)


    sensor_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("temperature", IntegerType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
        .option("subscribe", SENSORS_TOPIC)
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        )
        .option("startingOffsets", "latest")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), sensor_schema).alias("data"))
        .select("data.*")
    )

    events_df = (
        parsed_df
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
    )

    aggregated_df = (
        events_df
        .withWatermark("timestamp", "10 seconds")
        .groupBy(
            window(col("timestamp"), "1 minute", "30 seconds")
        )
        .agg(
            avg("temperature").alias("t_avg"),
            avg("humidity").alias("h_avg")
        )
        .select(
            col("window"),
            col("t_avg"),
            col("h_avg")
        )
    )

    alerts_result_df = (
        aggregated_df.crossJoin(alerts_df)
        .filter(
            (
                    (col("humidity_min") == -999) |
                    ((col("h_avg") >= col("humidity_min")) & (col("h_avg") <= col("humidity_max")))
            ) &
            (
                    (col("temperature_min") == -999) |
                    ((col("t_avg") >= col("temperature_min")) & (col("t_avg") <= col("temperature_max")))
            )
        )
        .select(
            col("window"),
            col("t_avg"),
            col("h_avg"),
            col("id").alias("alert_id"),
            col("code").cast("string").alias("code"),
            col("message")
        )
    )

    kafka_alerts_df = (
        alerts_result_df
        .withColumn("timestamp", current_timestamp().cast("string"))
        .selectExpr(
            "CAST(code AS STRING) AS key",
            """
            to_json(
                named_struct(
                    'window', named_struct(
                        'start', CAST(window.start AS STRING),
                        'end', CAST(window.end AS STRING)
                    ),
                    't_avg', t_avg,
                    'h_avg', h_avg,
                    'code', code,
                    'message', message,
                    'timestamp', timestamp
                )
            ) AS value
            """
        )
    )

    query = (
        kafka_alerts_df
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", ",".join(kafka_config["bootstrap_servers"]))
        .option("topic", f"{MY_NAME}_alerts")
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";'
        )
        .option("checkpointLocation", "goit_de_hw_06/checkpoints/alerts_kafka")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()



if __name__ == "__main__":
    main()