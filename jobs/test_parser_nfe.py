import json
import os
import sys

from parser_nfe import parse_nfe


def main():
    if len(sys.argv) < 2:
        print("Uso: python3 jobs/test_parser_nfe.py <caminho_do_xml>")
        sys.exit(1)

    xml_path = sys.argv[1]

    if not os.path.exists(xml_path):
        print(f"Arquivo não encontrado: {xml_path}")
        sys.exit(1)

    result = parse_nfe(xml_path)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()