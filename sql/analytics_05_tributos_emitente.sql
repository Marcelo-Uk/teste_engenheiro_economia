USE nfe;

DROP TABLE IF EXISTS analytics_05_tributos_emitente;

CREATE TABLE analytics_05_tributos_emitente
STORED AS PARQUET
AS
SELECT
    emit_documento,
    emit_nome,
    SUM(valor_total_tributos) AS total_tributos
FROM silver_nfe_header
GROUP BY emit_documento, emit_nome;