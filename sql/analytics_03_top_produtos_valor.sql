USE nfe;

DROP TABLE IF EXISTS analytics_03_top_produtos_valor;

CREATE TABLE analytics_03_top_produtos_valor
STORED AS PARQUET
AS
SELECT
    codigo_produto,
    descricao_produto,
    SUM(valor_produto) AS valor_total
FROM silver_nfe_item
GROUP BY codigo_produto, descricao_produto
ORDER BY valor_total DESC
LIMIT 10;