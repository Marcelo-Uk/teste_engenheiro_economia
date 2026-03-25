import os
from datetime import datetime, timezone
import xml.etree.ElementTree as ET


NS = {"nfe": "http://www.portalfiscal.inf.br/nfe"}


def get_text(parent, path, default=None):
    if parent is None:
        return default
    node = parent.find(path, NS)
    if node is None or node.text is None:
        return default
    value = node.text.strip()
    return value if value != "" else default


def to_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def get_document_info(node):
    if node is None:
        return None, None
    cnpj = get_text(node, "nfe:CNPJ")
    if cnpj:
        return cnpj, "CNPJ"
    cpf = get_text(node, "nfe:CPF")
    if cpf:
        return cpf, "CPF"
    return None, None


def parse_item(det):
    prod = det.find("nfe:prod", NS)
    imposto = det.find("nfe:imposto", NS)

    return {
        "n_item": int(det.attrib.get("nItem")) if det.attrib.get("nItem") else None,
        "codigo_produto": get_text(prod, "nfe:cProd"),
        "descricao_produto": get_text(prod, "nfe:xProd"),
        "ncm": get_text(prod, "nfe:NCM"),
        "cfop": get_text(prod, "nfe:CFOP"),
        "unidade_comercial": get_text(prod, "nfe:uCom"),
        "quantidade_comercial": to_float(get_text(prod, "nfe:qCom")),
        "valor_unitario_comercial": to_float(get_text(prod, "nfe:vUnCom")),
        "valor_produto": to_float(get_text(prod, "nfe:vProd")),
        "valor_desconto": to_float(get_text(prod, "nfe:vDesc")),
        "valor_total_tributos_item": to_float(get_text(imposto, "nfe:vTotTrib")),
    }


def parse_nfe(xml_path):
    tree = ET.parse(xml_path)
    root = tree.getroot()

    inf_nfe = root.find(".//nfe:infNFe", NS)
    ide = root.find(".//nfe:infNFe/nfe:ide", NS)
    emit = root.find(".//nfe:infNFe/nfe:emit", NS)
    dest = root.find(".//nfe:infNFe/nfe:dest", NS)
    icms_tot = root.find(".//nfe:infNFe/nfe:total/nfe:ICMSTot", NS)
    inf_prot = root.find(".//nfe:protNFe/nfe:infProt", NS)

    emit_doc, emit_doc_type = get_document_info(emit)
    dest_doc, dest_doc_type = get_document_info(dest)

    chave_nfe = get_text(inf_prot, "nfe:chNFe")
    if not chave_nfe and inf_nfe is not None:
        raw_id = inf_nfe.attrib.get("Id")
        if raw_id:
            chave_nfe = raw_id.replace("NFe", "", 1)

    itens = []
    for det in root.findall(".//nfe:infNFe/nfe:det", NS):
        itens.append(parse_item(det))

    result = {
        "source_file": os.path.basename(xml_path),
        "ingestion_ts": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "chave_nfe": chave_nfe,
        "header": {
            "numero_nota": get_text(ide, "nfe:nNF"),
            "serie": get_text(ide, "nfe:serie"),
            "data_emissao": get_text(ide, "nfe:dhEmi"),
            "data_saida_entrada": get_text(ide, "nfe:dhSaiEnt"),
            "natureza_operacao": get_text(ide, "nfe:natOp"),
            "tipo_nota": get_text(ide, "nfe:tpNF"),
            "finalidade": get_text(ide, "nfe:finNFe"),
        },
        "emitente": {
            "documento": emit_doc,
            "tipo_documento": emit_doc_type,
            "nome": get_text(emit, "nfe:xNome"),
            "fantasia": get_text(emit, "nfe:xFant"),
            "municipio": get_text(emit, "nfe:enderEmit/nfe:xMun"),
            "uf": get_text(emit, "nfe:enderEmit/nfe:UF"),
        },
        "destinatario": {
            "documento": dest_doc,
            "tipo_documento": dest_doc_type,
            "nome": get_text(dest, "nfe:xNome"),
            "municipio": get_text(dest, "nfe:enderDest/nfe:xMun"),
            "uf": get_text(dest, "nfe:enderDest/nfe:UF"),
            "email": get_text(dest, "nfe:email"),
        },
        "totais": {
            "valor_produtos": to_float(get_text(icms_tot, "nfe:vProd")),
            "valor_nota": to_float(get_text(icms_tot, "nfe:vNF")),
            "valor_total_tributos": to_float(get_text(icms_tot, "nfe:vTotTrib")),
            "valor_desconto": to_float(get_text(icms_tot, "nfe:vDesc")),
            "valor_frete": to_float(get_text(icms_tot, "nfe:vFrete")),
        },
        "protocolo": {
            "numero_protocolo": get_text(inf_prot, "nfe:nProt"),
            "status_codigo": get_text(inf_prot, "nfe:cStat"),
            "status_motivo": get_text(inf_prot, "nfe:xMotivo"),
            "data_recebimento": get_text(inf_prot, "nfe:dhRecbto"),
        },
        "itens": itens,
    }

    return result