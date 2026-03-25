import os
import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    explode,
    from_json,
    current_timestamp,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

def ensure_warehouse_permissions(hive_db: str):
    warehouse_root = "/opt/hive/data/warehouse"
    db_path = f"{warehouse_root}/{hive_db}.db"

    os.makedirs(warehouse_root, exist_ok=True)
    os.makedirs(db_path, exist_ok=True)

    try:
        os.chmod(warehouse_root, 0o777)
    except Exception as exc:
        print(f"[WARN] Não foi possível ajustar permissão de {warehouse_root}: {exc}")

    try:
        os.chmod(db_path, 0o777)
    except Exception as exc:
        print(f"[WARN] Não foi possível ajustar permissão de {db_path}: {exc}")

def build_payload_schema():
    item_schema = StructType([
        StructField("n_item", IntegerType(), True),
        StructField("codigo_produto", StringType(), True),
        StructField("descricao_produto", StringType(), True),
        StructField("ncm", StringType(), True),
        StructField("cfop", StringType(), True),
        StructField("unidade_comercial", StringType(), True),
        StructField("quantidade_comercial", DoubleType(), True),
        StructField("valor_unitario_comercial", DoubleType(), True),
        StructField("valor_produto", DoubleType(), True),
        StructField("valor_desconto", DoubleType(), True),
        StructField("valor_total_tributos_item", DoubleType(), True),
    ])

    return StructType([
        StructField("source_file", StringType(), True),
        StructField("ingestion_ts", StringType(), True),
        StructField("chave_nfe", StringType(), True),
        StructField("header", StructType([
            StructField("numero_nota", StringType(), True),
            StructField("serie", StringType(), True),
            StructField("data_emissao", StringType(), True),
            StructField("data_saida_entrada", StringType(), True),
            StructField("natureza_operacao", StringType(), True),
            StructField("tipo_nota", StringType(), True),
            StructField("finalidade", StringType(), True),
        ]), True),
        StructField("emitente", StructType([
            StructField("documento", StringType(), True),
            StructField("tipo_documento", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("fantasia", StringType(), True),
            StructField("municipio", StringType(), True),
            StructField("uf", StringType(), True),
        ]), True),
        StructField("destinatario", StructType([
            StructField("documento", StringType(), True),
            StructField("tipo_documento", StringType(), True),
            StructField("nome", StringType(), True),
            StructField("municipio", StringType(), True),
            StructField("uf", StringType(), True),
            StructField("email", StringType(), True),
        ]), True),
        StructField("totais", StructType([
            StructField("valor_produtos", DoubleType(), True),
            StructField("valor_nota", DoubleType(), True),
            StructField("valor_total_tributos", DoubleType(), True),
            StructField("valor_desconto", DoubleType(), True),
            StructField("valor_frete", DoubleType(), True),
        ]), True),
        StructField("protocolo", StructType([
            StructField("numero_protocolo", StringType(), True),
            StructField("status_codigo", StringType(), True),
            StructField("status_motivo", StringType(), True),
            StructField("data_recebimento", StringType(), True),
        ]), True),
        StructField("itens", ArrayType(item_schema), True),
    ])


def build_spark(
    app_name: str,
    master: str | None = None,
    hive_metastore_uri: str = "thrift://hive-metastore:9083",
    hive_metastore_version: str = "4.0.1",
):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", "/opt/hive/data/warehouse")
        .config("hive.metastore.uris", hive_metastore_uri)
        .config("spark.hadoop.hive.metastore.uris", hive_metastore_uri)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.hive.metastore.version", hive_metastore_version)
        .config("spark.sql.hive.metastore.jars", "maven")
        .enableHiveSupport()
    )
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser(description="Consome Kafka e grava dados NF-e no Hive")
    parser.add_argument("--topic", required=True, help="Tópico Kafka de origem")
    parser.add_argument("--bootstrap-servers", default="kafka:9092", help="Bootstrap servers do Kafka")
    parser.add_argument("--master", default=None, help="Spark master, ex.: spark://spark-master:7077")
    parser.add_argument("--hive-db", default="nfe", help="Database do Hive")
    parser.add_argument("--hive-metastore-uri", default="thrift://hive-metastore:9083", help="URI do Hive Metastore")
    parser.add_argument("--hive-metastore-version", default="4.0.1", help="Versão do Hive Metastore")
    args = parser.parse_args()

    spark = build_spark(
        app_name="KAFKA_TO_HIVE_NFE",
        master=args.master,
        hive_metastore_uri=args.hive_metastore_uri,
        hive_metastore_version=args.hive_metastore_version,
    )
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {args.hive_db}")
    # ensure_warehouse_permissions(args.hive_db)

    kafka_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    if kafka_df.rdd.isEmpty():
        print("[INFO] Nenhuma mensagem encontrada no tópico Kafka.")
        spark.stop()
        return

    bronze_df = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("message_key"),
        col("value").cast("string").alias("payload_json"),
        current_timestamp().alias("processed_at"),
    )

    bronze_df.write.mode("overwrite").format("parquet").saveAsTable(f"{args.hive_db}.bronze_nfe_json")

    schema = build_payload_schema()

    parsed_df = bronze_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("message_key"),
        col("processed_at"),
        from_json(col("payload_json"), schema).alias("data"),
    ).filter(col("data").isNotNull())

    header_df = parsed_df.select(
        col("data.chave_nfe").alias("chave_nfe"),
        col("data.source_file").alias("source_file"),
        col("data.ingestion_ts").alias("ingestion_ts"),
        col("data.header.numero_nota").alias("numero_nota"),
        col("data.header.serie").alias("serie"),
        col("data.header.data_emissao").alias("data_emissao"),
        col("data.header.data_saida_entrada").alias("data_saida_entrada"),
        col("data.header.natureza_operacao").alias("natureza_operacao"),
        col("data.header.tipo_nota").alias("tipo_nota"),
        col("data.header.finalidade").alias("finalidade"),
        col("data.emitente.documento").alias("emit_documento"),
        col("data.emitente.tipo_documento").alias("emit_tipo_documento"),
        col("data.emitente.nome").alias("emit_nome"),
        col("data.emitente.fantasia").alias("emit_fantasia"),
        col("data.emitente.municipio").alias("emit_municipio"),
        col("data.emitente.uf").alias("emit_uf"),
        col("data.destinatario.documento").alias("dest_documento"),
        col("data.destinatario.tipo_documento").alias("dest_tipo_documento"),
        col("data.destinatario.nome").alias("dest_nome"),
        col("data.destinatario.municipio").alias("dest_municipio"),
        col("data.destinatario.uf").alias("dest_uf"),
        col("data.destinatario.email").alias("dest_email"),
        col("data.totais.valor_produtos").alias("valor_produtos"),
        col("data.totais.valor_nota").alias("valor_nota"),
        col("data.totais.valor_total_tributos").alias("valor_total_tributos"),
        col("data.totais.valor_desconto").alias("valor_desconto"),
        col("data.totais.valor_frete").alias("valor_frete"),
        col("data.protocolo.numero_protocolo").alias("numero_protocolo"),
        col("data.protocolo.status_codigo").alias("status_codigo"),
        col("data.protocolo.status_motivo").alias("status_motivo"),
        col("data.protocolo.data_recebimento").alias("data_recebimento"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("processed_at"),
    )

    header_df.write.mode("overwrite").format("parquet").saveAsTable(f"{args.hive_db}.silver_nfe_header")

    item_df = parsed_df.select(
        col("data.chave_nfe").alias("chave_nfe"),
        col("data.source_file").alias("source_file"),
        col("data.ingestion_ts").alias("ingestion_ts"),
        explode(col("data.itens")).alias("item"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("processed_at"),
    ).select(
        col("chave_nfe"),
        col("source_file"),
        col("ingestion_ts"),
        col("item.n_item").alias("n_item"),
        col("item.codigo_produto").alias("codigo_produto"),
        col("item.descricao_produto").alias("descricao_produto"),
        col("item.ncm").alias("ncm"),
        col("item.cfop").alias("cfop"),
        col("item.unidade_comercial").alias("unidade_comercial"),
        col("item.quantidade_comercial").alias("quantidade_comercial"),
        col("item.valor_unitario_comercial").alias("valor_unitario_comercial"),
        col("item.valor_produto").alias("valor_produto"),
        col("item.valor_desconto").alias("valor_desconto"),
        col("item.valor_total_tributos_item").alias("valor_total_tributos_item"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
        col("processed_at"),
    )

    item_df.write.mode("overwrite").format("parquet").saveAsTable(f"{args.hive_db}.silver_nfe_item")

    gold_emitente_df = spark.sql(f"""
        SELECT
            emit_documento,
            emit_nome,
            COUNT(*) AS qtd_notas,
            SUM(valor_nota) AS valor_total_notas,
            SUM(valor_produtos) AS valor_total_produtos,
            SUM(valor_total_tributos) AS valor_total_tributos
        FROM {args.hive_db}.silver_nfe_header
        GROUP BY emit_documento, emit_nome
    """)
    
    gold_emitente_df.write.mode("overwrite").format("parquet").saveAsTable(
        f"{args.hive_db}.gold_nfe_por_emitente"
    )
    
    gold_uf_destino_df = spark.sql(f"""
        SELECT
            dest_uf,
            COUNT(*) AS qtd_notas,
            SUM(valor_nota) AS valor_total_notas
        FROM {args.hive_db}.silver_nfe_header
        GROUP BY dest_uf
    """)
    
    gold_uf_destino_df.write.mode("overwrite").format("parquet").saveAsTable(
        f"{args.hive_db}.gold_nfe_por_uf_destino"
    )
    
    gold_top_produtos_valor_df = spark.sql(f"""
        SELECT
            codigo_produto,
            descricao_produto,
            SUM(valor_produto) AS valor_total
        FROM {args.hive_db}.silver_nfe_item
        GROUP BY codigo_produto, descricao_produto
        ORDER BY valor_total DESC
        LIMIT 10
    """)
    
    gold_top_produtos_valor_df.write.mode("overwrite").format("parquet").saveAsTable(
        f"{args.hive_db}.gold_top_produtos_valor"
    )
    
    gold_top_produtos_quantidade_df = spark.sql(f"""
        SELECT
            codigo_produto,
            descricao_produto,
            SUM(quantidade_comercial) AS quantidade_total
        FROM {args.hive_db}.silver_nfe_item
        GROUP BY codigo_produto, descricao_produto
        ORDER BY quantidade_total DESC
        LIMIT 10
    """)
    
    gold_top_produtos_quantidade_df.write.mode("overwrite").format("parquet").saveAsTable(
        f"{args.hive_db}.gold_top_produtos_quantidade"
    )

    print("=" * 80)
    print("[RESUMO] Kafka -> Hive finalizado")
    print(f"[RESUMO] Bronze: {args.hive_db}.bronze_nfe_json")
    print(f"[RESUMO] Silver header: {args.hive_db}.silver_nfe_header")
    print(f"[RESUMO] Silver item: {args.hive_db}.silver_nfe_item")
    print(f"[RESUMO] Gold: {args.hive_db}.gold_nfe_por_emitente")
    print(f"[RESUMO] Gold: {args.hive_db}.gold_nfe_por_uf_destino")
    print(f"[RESUMO] Gold: {args.hive_db}.gold_top_produtos_valor")
    print(f"[RESUMO] Gold: {args.hive_db}.gold_top_produtos_quantidade")

    spark.stop()


if __name__ == "__main__":
    main()