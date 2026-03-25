import argparse
import json
import os
from glob import glob

from kafka import KafkaProducer
from pyspark.sql import SparkSession

from parser_nfe import parse_nfe


def discover_xml_files(xml_dir: str):
    patterns = [
        os.path.join(xml_dir, "*.xml"),
        os.path.join(xml_dir, "**", "*.xml"),
    ]
    files = set()
    for pattern in patterns:
        files.update(glob(pattern, recursive=True))
    return sorted(f for f in files if os.path.isfile(f))


def build_spark(app_name: str, master: str | None = None):
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def parse_file_safe(xml_path: str):
    try:
        return {
            "success": True,
            "xml_path": xml_path,
            "payload": parse_nfe(xml_path),
            "error": None,
        }
    except Exception as exc:
        return {
            "success": False,
            "xml_path": xml_path,
            "payload": None,
            "error": str(exc),
        }


def main():
    parser = argparse.ArgumentParser(description="Lê XMLs NF-e e publica JSONs no Kafka")
    parser.add_argument("--xml-dir", required=True, help="Diretório onde estão os XMLs")
    parser.add_argument("--topic", required=True, help="Tópico Kafka de destino")
    parser.add_argument("--bootstrap-servers", default="kafka:9092", help="Bootstrap servers do Kafka")
    parser.add_argument("--master", default=None, help="Spark master, ex.: spark://spark-master:7077")
    parser.add_argument("--limit", type=int, default=None, help="Limitar quantidade de arquivos para teste")
    args = parser.parse_args()

    spark = build_spark(app_name="XML_TO_KAFKA_NFE", master=args.master)

    xml_files = discover_xml_files(args.xml_dir)
    if args.limit:
        xml_files = xml_files[: args.limit]

    if not xml_files:
        print(f"[ERRO] Nenhum XML encontrado em: {args.xml_dir}")
        spark.stop()
        raise SystemExit(1)

    print(f"[INFO] Total de XMLs encontrados: {len(xml_files)}")

    rdd = spark.sparkContext.parallelize(xml_files, min(len(xml_files), 4))
    parsed_results = rdd.map(parse_file_safe).collect()

    success_count = 0
    error_count = 0
    failed_files = []

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    try:
        for item in parsed_results:
            if not item["success"]:
                error_count += 1
                failed_files.append((item["xml_path"], item["error"]))
                continue

            payload = item["payload"]
            chave_nfe = payload.get("chave_nfe") or os.path.basename(item["xml_path"])

            producer.send(
                topic=args.topic,
                key=chave_nfe,
                value=payload,
            )
            success_count += 1

        producer.flush()
    finally:
        producer.close()
        spark.stop()

    print("=" * 80)
    print("[RESUMO] Publicação XML -> Kafka finalizada")
    print(f"[RESUMO] XMLs processados com sucesso: {success_count}")
    print(f"[RESUMO] XMLs com erro: {error_count}")

    if failed_files:
        print("[RESUMO] Arquivos com falha:")
        for xml_path, error in failed_files:
            print(f" - {xml_path}: {error}")

    if success_count == 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()