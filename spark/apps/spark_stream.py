from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json, to_timestamp
import logging


def create_spark_session() -> SparkSession:
    
    spark = (
    SparkSession.builder
    .appName("KafkaClickstreamIngest")
    .getOrCreate()
    )
    logging.info("Spark session created successfully")

    return spark



def read_streaming_data_from_kafka(spark_session):
    try:
        raw_data = (
        spark_session.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")  
        .option("subscribe", "clickstream-events")
        .option("startingOffsets", "latest")             
        .option("failOnDataLoss", "false")
        .load()
        )
        logging.info("Raw data read successfully")
    except Exception as e:
        logging.warning(f"reading raw data was unsuccesfull due to: {e}" )
        raise
    return raw_data

def create_schema_parsed_dataframe(raw_data):
    # 2) Cast Kafka value to string
    json_str = raw_data.selectExpr("CAST(value AS STRING) AS json_str")

    # 3) Declare schema that matches your JSON (hyphenated key is OK here)
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("product-id", StringType()),
        StructField("event_type", StringType()),
        StructField("timestamp", StringType()),  
    ])

    # 4) Parse JSON
    parsed = json_str.select(from_json(col("json_str"), schema).alias("data"))

    # 5) Select & clean columns (use backticks for names with special chars, then alias)
    events = (
        parsed
        .select(
            col("data.user_id").alias("user_id"),
            col("data.`product-id`").alias("product_id"),
            col("data.event_type").alias("event_type"),
            to_timestamp(col("data.timestamp")).alias("event_time")
        )
    )
    return events

def start_streaming_Data():
    spark = create_spark_session()
    raw_data = read_streaming_data_from_kafka(spark_session=spark)
    final_df = create_schema_parsed_dataframe(raw_data=raw_data)
    logging.info(f"final dataframe from the excution: {final_df}")


if __name__ == "__main__":
    start_streaming_Data()