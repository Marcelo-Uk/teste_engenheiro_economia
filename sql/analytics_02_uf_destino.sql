USE nfe;

DROP TABLE IF EXISTS analytics_02_uf_destino;

CREATE TABLE analytics_02_uf_destino
STORED AS PARQUET
AS
SELECT
    dest_uf,
    COUNT(*) AS qtd_notas,
    SUM(valor_nota) AS valor_total_notas
FROM silver_nfe_header
GROUP BY dest_uf;