# Teste Técnico — Engenheiro de Dados Sênior  
## Pipeline NF-e com Airflow, Spark, Kafka e Hive

## 1. Objetivo

Este projeto foi desenvolvido como solução para o teste técnico de **Engenheiro de Dados Sênior**, com foco em arquitetura de dados, processamento distribuído, mensageria, orquestração e persistência analítica.

A solução implementa um pipeline ponta a ponta para processamento de **Notas Fiscais Eletrônicas (NF-e)** em XML, contemplando:

- leitura dos XMLs
- transformação dos dados para JSON
- publicação em tópico Kafka
- consumo dos eventos
- persistência no Hive
- geração de consultas analíticas salvas no próprio Hive
- orquestração completa via Airflow

---

## 2. Arquitetura implementada

A arquitetura construída segue a proposta do teste:

**XML NF-e → Spark → Kafka → Spark → Hive → Consultas Analíticas**

### Componentes utilizados

- **Apache Airflow**: orquestração das DAGs
- **Apache Spark**: processamento dos dados
- **Apache Kafka**: barramento de mensagens
- **Apache Hive**: armazenamento e consultas analíticas
- **PostgreSQL**:
  - metadata do Airflow
  - metastore do Hive
- **Docker Compose**: subida da infraestrutura completa

### Fluxo executado

#### DAG 1 — `dag_1_xml_to_kafka`
Responsável por:

- ler os XMLs da pasta `xmls/`
- transformar cada nota em JSON estruturado
- publicar cada JSON no tópico Kafka `nfe-topic`

#### DAG 2 — `dag_2_kafka_to_hive`
Responsável por:

- consumir os JSONs publicados no Kafka
- gravar dados no Hive em camadas:
  - `bronze`
  - `silver`
  - `gold`
- persistir as consultas analíticas no próprio Hive

#### Encadeamento
A **DAG 1** foi configurada para disparar automaticamente a **DAG 2** ao final, fechando o fluxo completo ponta a ponta.

---

## 3. Estrutura do projeto

```text
desafio-nfe/
├── dags/
│   ├── dag_1_xml_to_kafka.py
│   └── dag_2_kafka_to_hive.py
├── jobs/
│   ├── parser_nfe.py
│   ├── test_parser_nfe.py
│   ├── xml_to_kafka.py
│   └── kafka_to_hive.py
├── sql/
│   ├── analytics_01_emitente.sql
│   ├── analytics_02_uf_destino.sql
│   ├── analytics_03_top_produtos_valor.sql
│   ├── analytics_04_top_produtos_quantidade.sql
│   └── analytics_05_tributos_emitente.sql
├── xmls/
├── conf/
│   └── hive/
│       └── hive-site.xml
├── docker/
│   └── airflow/
│       ├── Dockerfile
│       └── requirements.txt
├── drivers/
│   └── postgresql.jar
├── evidencias/
│   └── teste-engenheiro/
├── docker-compose.yml
├── .env
├── .gitignore
└── README.md
```

---

## 4. Infraestrutura com Docker Compose

Toda a infraestrutura foi subida em containers, integrados na mesma rede Docker.

### Serviços utilizados

- `airflow-webserver`
- `airflow-scheduler`
- `airflow-init`
- `postgres-airflow`
- `kafka`
- `spark-master`
- `spark-worker`
- `postgres-hive`
- `hive-metastore`
- `hive-server2`

### Observação importante de implementação

Durante a estabilização da solução, a **DAG 1** foi mantida executando Spark no cluster standalone (`spark://spark-master:7077`), conforme a proposta arquitetural.

Já a **DAG 2** foi estabilizada em `local[*]` dentro do container do Airflow para evitar problemas de commit/rename em filesystem compartilhado do Hive Warehouse em ambiente Docker local de notebook. Em ambiente produtivo, o ideal seria utilizar um storage compartilhado distribuído, como HDFS, S3, MinIO ou NFS.

---

## 5. Modelo de dados utilizado

### 5.1 Camada Bronze

Tabela:
- `nfe.bronze_nfe_json`

Objetivo:
- armazenar o payload bruto vindo do Kafka

Campos principais:
- tópico
- partição
- offset
- timestamp Kafka
- chave da mensagem
- JSON bruto da NF-e

---

### 5.2 Camada Silver

Tabelas:
- `nfe.silver_nfe_header`
- `nfe.silver_nfe_item`

Objetivo:
- estruturar os dados para análise

#### `silver_nfe_header`
Uma linha por nota fiscal, contendo:

- chave da NF-e
- arquivo de origem
- informações do cabeçalho
- emitente
- destinatário
- totais
- protocolo

#### `silver_nfe_item`
Uma linha por item da nota, contendo:

- chave da NF-e
- número do item
- código do produto
- descrição
- NCM
- CFOP
- quantidade
- valor unitário
- valor do item
- tributos do item

---

### 5.3 Camada Gold

Tabelas analíticas persistidas no Hive:

- `nfe.gold_nfe_por_emitente`
- `nfe.gold_nfe_por_uf_destino`
- `nfe.gold_top_produtos_valor`
- `nfe.gold_top_produtos_quantidade`

Além disso, também foram criadas consultas analíticas em arquivos SQL, com resultados persistidos em tabelas específicas:

- `analytics_01_emitente`
- `analytics_02_uf_destino`
- `analytics_03_top_produtos_valor`
- `analytics_04_top_produtos_quantidade`
- `analytics_05_tributos_emitente`

---

## 6. Parser dos XMLs

Foi desenvolvido um parser próprio para NF-e em XML, considerando o namespace oficial SEFAZ.

O parser extrai:

- chave da NF-e
- cabeçalho da nota
- emitente
- destinatário
- totais
- protocolo
- itens

Cada XML é transformado em um JSON estruturado, utilizado como contrato de dados entre a DAG 1 e a DAG 2.

Exemplo de campos transformados:

- `source_file`
- `ingestion_ts`
- `chave_nfe`
- `header`
- `emitente`
- `destinatario`
- `totais`
- `protocolo`
- `itens`

---

## 7. Consultas analíticas no Hive

Atendendo ao item 4 do teste, foram criadas consultas SQL executadas no Hive com uso de:

- contagens
- somas
- agrupamentos

Os resultados foram salvos no próprio Hive.

### Consultas implementadas

#### 1. Total de notas e valores por emitente
Arquivo:
- `sql/analytics_01_emitente.sql`

Resultado salvo em:
- `nfe.analytics_01_emitente`

#### 2. Total de notas e faturamento por UF do destinatário
Arquivo:
- `sql/analytics_02_uf_destino.sql`

Resultado salvo em:
- `nfe.analytics_02_uf_destino`

#### 3. Top 10 produtos por valor vendido
Arquivo:
- `sql/analytics_03_top_produtos_valor.sql`

Resultado salvo em:
- `nfe.analytics_03_top_produtos_valor`

#### 4. Top 10 produtos por quantidade vendida
Arquivo:
- `sql/analytics_04_top_produtos_quantidade.sql`

Resultado salvo em:
- `nfe.analytics_04_top_produtos_quantidade`

#### 5. Total de tributos por emitente
Arquivo:
- `sql/analytics_05_tributos_emitente.sql`

Resultado salvo em:
- `nfe.analytics_05_tributos_emitente`

---

## 8. Como executar o projeto

### 8.1 Subir a infraestrutura

```bash
docker compose up -d --build
```

### 8.2 Acessos

#### Airflow
- URL: `http://localhost:18080`
- Usuário: `admin`
- Senha: `admin`

#### Spark Master
- URL: `http://localhost:18081`

#### Spark Worker
- URL: `http://localhost:8082`

#### Hive Server2
- Porta: `10000`

---

## 9. Execução das DAGs

### DAG 1
DAG:
- `dag_1_xml_to_kafka`

Responsável por:
- ler os XMLs
- publicar os JSONs no Kafka

### DAG 2
DAG:
- `dag_2_kafka_to_hive`

Responsável por:
- consumir os JSONs do Kafka
- persistir no Hive

### Encadeamento
A DAG 1 foi revisada para disparar automaticamente a DAG 2 ao final usando `TriggerDagRunOperator`.

---

## 10. Validação no Hive

Exemplo de acesso:

```bash
docker exec -it hive-server2 /opt/hive/bin/beeline -u 'jdbc:hive2://localhost:10000/'
```

Exemplos de comandos utilizados:

```sql
USE nfe;
SHOW TABLES;
SELECT COUNT(*) FROM bronze_nfe_json;
SELECT COUNT(*) FROM silver_nfe_header;
SELECT COUNT(*) FROM silver_nfe_item;
SELECT * FROM analytics_01_emitente LIMIT 20;
SELECT * FROM analytics_03_top_produtos_valor LIMIT 20;
```

---

## 11. Evidências de funcionamento

Foram geradas evidências visuais e funcionais do pipeline completo.

### Evidências registradas

- DAG 1 executada com sucesso
- DAG 2 executada com sucesso
- `SHOW TABLES` no Hive
- resultado das 5 consultas analíticas
- validações da persistência no Hive

As evidências foram armazenadas em:

```text
evidencias/teste-engenheiro/
```

---

## 12. Principais decisões técnicas

### 1. Parser próprio para NF-e
Foi implementado parser em Python para garantir extração controlada dos campos do XML SEFAZ.

### 2. Kafka como desacoplamento
A arquitetura foi separada em duas etapas, com Kafka funcionando como camada intermediária entre ingestão e persistência.

### 3. Modelagem Bronze / Silver / Gold
A persistência foi organizada em camadas para facilitar rastreabilidade, estruturação e consumo analítico.

### 4. Persistência analítica no próprio Hive
As consultas solicitadas pelo teste foram persistidas como tabelas analíticas no Hive.

### 5. Execução da DAG 2 em `local[*]`
Essa decisão foi adotada para estabilizar a escrita na warehouse do Hive em ambiente Docker local com volume compartilhado. Em ambiente produtivo, o ideal seria storage distribuído.

---

## 13. Resultado final

A solução entregue demonstra com sucesso:

- infraestrutura completa em Docker Compose
- leitura de 100 XMLs NF-e
- transformação para JSON
- publicação no Kafka
- consumo do Kafka
- persistência no Hive
- consultas analíticas persistidas no Hive
- orquestração completa com Airflow
- evidências de funcionamento

---

## 14. Desafios encontrados e soluções adotadas

Durante a implementação, alguns desafios técnicos surgiram, principalmente por se tratar de uma stack distribuída rodando localmente em Docker sobre notebook.

### 1. Compatibilidade de versão do Python entre driver e worker do Spark
**Problema:** o Airflow estava executando o driver com Python 3.12, enquanto o worker do Spark usava Python 3.10, causando erro de `PYTHON_VERSION_MISMATCH`.

**Solução adotada:** a imagem do Airflow foi ajustada para usar explicitamente Python 3.10, alinhando driver e worker.

---

### 2. Disponibilização do módulo `parser_nfe.py` para os executores Spark
**Problema:** o job de publicação no Kafka falhava com `No module named 'parser_nfe'` nos executores.

**Solução adotada:** o arquivo auxiliar foi distribuído explicitamente no `spark-submit` usando `--py-files`, garantindo que os executores recebessem o módulo necessário.

---

### 3. Integração do Spark com o Hive Metastore remoto
**Problema:** inicialmente o Spark tentava usar um metastore local em Derby (`metastore_db`) em vez do Hive Metastore remoto, causando falhas ao criar database e tabelas.

**Solução adotada:** foram adicionadas configurações explícitas no `spark-submit` e no `SparkSession` para apontar para:
- `hive.metastore.uris`
- `spark.hadoop.hive.metastore.uris`
- `spark.sql.hive.metastore.version=4.0.1`
- `spark.sql.hive.metastore.jars=maven`

---

### 4. Permissões na Hive Warehouse em ambiente Docker local
**Problema:** o Spark em modo cluster apresentou erros de `mkdir` e `rename` ao tentar gravar arquivos Parquet no diretório da warehouse do Hive, devido ao compartilhamento de volume entre containers.

**Solução adotada:** a DAG 2 foi estabilizada executando Spark em `local[*]` dentro do próprio container do Airflow. Isso eliminou conflitos de commit/rename entre driver e executores em containers distintos.

---

### 5. Operação SQL não suportada no catálogo Hive/Spark
**Problema:** o uso de `CREATE OR REPLACE TABLE ... AS SELECT` falhou com erro de operação não suportada no catálogo atual.

**Solução adotada:** as tabelas analíticas passaram a ser geradas via DataFrame e persistidas com:
- `write.mode("overwrite")`
- `format("parquet")`
- `saveAsTable(...)`

---

### 6. Montagem da pasta `sql/` dentro dos containers Hive
**Problema:** para executar os arquivos analíticos com `!run`, a pasta `sql/` precisava estar montada no `hive-server2`.

**Solução adotada:** o `docker-compose.yml` foi ajustado para montar:
- `./sql:/opt/project/sql`

nos serviços Hive, permitindo a execução dos scripts diretamente no Beeline.

---

## 15. Autor

Projeto desenvolvido por **Marcelo Ribeiro** como solução para o teste técnico de Engenheiro de Dados Sênior.
