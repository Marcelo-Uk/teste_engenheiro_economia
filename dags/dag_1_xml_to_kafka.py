# 240326 2236h

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="dag_1_xml_to_kafka",
    description="Lê XMLs NF-e, transforma em JSON e publica no Kafka",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["nfe", "kafka", "spark", "ingestao"],
) as dag:

    publicar_xmls_no_kafka = BashOperator(
        task_id="publicar_xmls_no_kafka",
        bash_command="""
        cd /opt/project && \
        spark-submit \
          --master spark://spark-master:7077 \
          --py-files /opt/project/jobs/parser_nfe.py \
          /opt/project/jobs/xml_to_kafka.py \
          --xml-dir /opt/project/xmls \
          --topic nfe-topic \
          --bootstrap-servers kafka:9092
        """,
    )

    disparar_dag_2 = TriggerDagRunOperator(
        task_id="disparar_dag_2",
        trigger_dag_id="dag_2_kafka_to_hive",
        wait_for_completion=False,
    )

    publicar_xmls_no_kafka >> disparar_dag_2