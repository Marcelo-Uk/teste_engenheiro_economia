USE nfe;

DROP TABLE IF EXISTS analytics_01_emitente;

CREATE TABLE analytics_01_emitente
STORED AS PARQUET
AS
SELECT
    emit_documento,
    emit_nome,
    COUNT(*) AS qtd_notas,
    SUM(valor_nota) AS valor_total_notas,
    SUM(valor_produtos) AS valor_total_produtos,
    SUM(valor_total_tributos) AS valor_total_tributos
FROM silver_nfe_header
GROUP BY emit_documento, emit_nome;