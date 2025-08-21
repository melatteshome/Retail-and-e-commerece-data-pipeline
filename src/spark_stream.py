import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, to_timestamp


DATABEND_HOST      = os.getenv("DATABEND_HOST")   
DATABEND_PORT      = os.getenv("DATABEND_PORT")        
DATABEND_DB        = os.getenv("DATABEND_DB")
DATABEND_USER      = os.getenv("DATABEND_USER" )
DATABEND_PASSWORD  = os.getenv("DATABEND_PASSWORD")
DATABEND_TABLE     = os.getenv("DATABEND_TABLE")

DATABEND_JDBC_URL = (
    f"jdbc:mysql://{DATABEND_HOST}:{DATABEND_PORT}/{DATABEND_DB}"
    "?useSSL=false&session_timezone=UTC&rewriteBatchedStatements=true"
)

# Kafka (adjust if needed)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC")


CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/checkpoints/kafka_order_events")

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("KafkaClickstreamIngest -> Databend")
        .config(
            "spark.jars.packages",
            ",".join([
                # Kafka source for Spark 4.x (Scala 2.13)
                "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
                # MySQL JDBC driver (Databend via MySQL wire protocol)
                "mysql:mysql-connector-j:8.4.0"
            ])
        )
        .getOrCreate()
    )
    logging.info("Spark session created successfully")
    return spark


def read_streaming_data_from_kafka(spark_session):
    try:
        raw_data = (
            spark_session.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "latest")
            .option("failOnDataLoss", "false")
            .load()
        )
        logging.info("Raw data read successfully")
        return raw_data
    except Exception as e:
        logging.warning(f"reading raw data was unsuccessful due to: {e}")
        raise


def create_schema_parsed_dataframe(raw_data):
    json_str = raw_data.selectExpr("CAST(value AS STRING) AS json_str")

    schema = StructType([
        StructField("user_id", StringType()),
        StructField("product-id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", StringType()),
    ])

    parsed = json_str.select(from_json(col("json_str"), schema).alias("data"))

    events = (
        parsed.select(
            col("data.user_id").alias("user_id"),
            col("data.`product-id`").alias("product_id"),
            col("data.event_type").alias("event_type"),
            # If not ISO8601, pass the exact pattern in the 2nd arg
            to_timestamp(col("data.timestamp")).alias("event_time")
        )
    )
    return events


def write_batch_to_databend(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return

    (batch_df
        .write
        .mode("append")
        .format("jdbc")
        .option("url", DATABEND_JDBC_URL)
        .option("dbtable", DATABEND_TABLE)
        .option("user", DATABEND_USER)
        .option("password", DATABEND_PASSWORD)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", "5000")
        .option("truncate", "false")
        .save()
    )
    logging.info(f"Wrote batch_id={batch_id} to Databend table={DATABEND_TABLE}")


def start_streaming_data():
    spark = create_spark_session()
    raw_data = read_streaming_data_from_kafka(spark_session=spark)
    final_df = create_schema_parsed_dataframe(raw_data=raw_data)

    # Align with destination schema
    final_df = final_df.select("user_id", "product_id", "event_type", "event_time")

    query = (
        final_df.writeStream
        .outputMode("append")
        .foreachBatch(write_batch_to_databend)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    logging.info("Streaming query started. Writing to Databend (local Docker) ...")
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming_data()
