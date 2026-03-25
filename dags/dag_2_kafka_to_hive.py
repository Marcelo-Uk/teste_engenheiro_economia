from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="dag_2_kafka_to_hive",
    description="Consome JSONs do Kafka e persiste dados no Hive (bronze, silver e gold)",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["nfe", "kafka", "hive", "spark"],
) as dag:

    consumir_kafka_e_gravar_hive = BashOperator(
        task_id="consumir_kafka_e_gravar_hive",
        bash_command="""
        cd /opt/project && \
        spark-submit \
          --master local[*] \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2 \
          --conf spark.sql.shuffle.partitions=1 \
          --conf spark.default.parallelism=1 \
          --conf spark.sql.warehouse.dir=/opt/hive/data/warehouse \
          --conf hive.metastore.uris=thrift://hive-metastore:9083 \
          --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
          --conf spark.sql.hive.metastore.version=4.0.1 \
          --conf spark.sql.hive.metastore.jars=maven \
          /opt/project/jobs/kafka_to_hive.py \
          --topic nfe-topic \
          --bootstrap-servers kafka:9092 \
          --hive-db nfe \
          --hive-metastore-uri thrift://hive-metastore:9083 \
          --hive-metastore-version 4.0.1
        """,
    )

    consumir_kafka_e_gravar_hive