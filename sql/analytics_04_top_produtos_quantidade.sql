USE nfe;

DROP TABLE IF EXISTS analytics_04_top_produtos_quantidade;

CREATE TABLE analytics_04_top_produtos_quantidade
STORED AS PARQUET
AS
SELECT
    codigo_produto,
    descricao_produto,
    SUM(quantidade_comercial) AS quantidade_total
FROM silver_nfe_item
GROUP BY codigo_produto, descricao_produto
ORDER BY quantidade_total DESC
LIMIT 10;