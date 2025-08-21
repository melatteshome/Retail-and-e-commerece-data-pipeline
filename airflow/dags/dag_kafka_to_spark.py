from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable

from src.data_generation.click_stream_data.producer_registry import generate_and_stream



KAFKA_TOPIC = Variable.get("KAFKA_TOPIC", default_var="events_topic")
BATCH_SIZE = int(Variable.get("KAFKA_BATCH_SIZE", default_var=100))

tz = pendulum.timezone("Africa/Addis_Ababa")

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="generate_to_kafka_then_run_spark",
    description="Generate data to Kafka, then launch a Spark job.",
    start_date=pendulum.now(tz).subtract(days=1),
    schedule_interval="0 * * * *",  # hourly at minute 0
    catchup=False,
    default_args=default_args,
    tags=["kafka", "spark", "batch"],
) as dag:

    generate_and_send = PythonOperator(
        task_id="generate_and_send_to_kafka",
        python_callable=generate_and_stream,
        op_kwargs={
            "topic": KAFKA_TOPIC,
            "batch_size": BATCH_SIZE,
        },
    )

    spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application=SPARK_APP,              
        conn_id="spark_default",            
        application_args=[
            f"--topic={KAFKA_TOPIC}",
            "--run-mode=batch",
        ],
        packages=None,                     
        executor_cores=2,
        executor_memory="2g",
        driver_memory="1g",
        verbose=False,
    )

    generate_and_send >> spark_job
