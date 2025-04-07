-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Demonstração de Leitura de Dados do Kafka com DLT + SQL
-- MAGIC
-- MAGIC Este notebook demonstra como implementar a leitura de dados do Apache Kafka no Databricks, utilizando a arquitetura Medallion.
-- MAGIC
-- MAGIC * Esse notebook já está ponto, para rodar ou após realizar alguma atualização, é necessario criar uma DLT Pipeline e associar ao notebook.
-- MAGIC *  Você pode conferir como fazer isso:  <a href="$../../Guias_UI/criar_dlt_kafka.md">aqui</a>
-- MAGIC
-- MAGIC ## Documentação Oficial
-- MAGIC - [Databricks Structured Streaming with Kafka](https://docs.databricks.com/structured-streaming/kafka.html)
-- MAGIC - [AWS MSK IAM Authentication](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
-- MAGIC
-- MAGIC ## Cenário de Demonstração
-- MAGIC Vamos criar um cenário prático onde:
-- MAGIC 1. Lemos dados de clickstream em tempo real do Kafka
-- MAGIC 2. Implementamos a arquitetura Medallion (Bronze, Silver, Gold)
-- MAGIC 3. Aplicamos validações e transformações nos dados
-- MAGIC
-- MAGIC ### Arquitetura de Dados
-- MAGIC - **Bronze**: Dados brutos do Kafka
-- MAGIC - **Silver**: Dados validados e estruturados
-- MAGIC - **Gold**: Agregações e métricas de negócio

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Leitura dos Dados do Kafka (Bronze Layer)
-- MAGIC
-- MAGIC Na camada Bronze, realizamos a ingestão dos dados brutos do Kafka.
-- MAGIC Os dados são lidos em formato JSON e armazenados sem transformações.

-- COMMAND ----------

-- DBTITLE 1,Criando tabela Bronze com dados do Kafka
CREATE OR REFRESH STREAMING TABLE clickstream_bronze
AS
SELECT
  from_json(
    CAST(value AS STRING),
    "SESSION_ID STRING, TIMESTAMP TIMESTAMP, PAGE_NAME STRING, BROWSER_FAMILY STRING, BROWSER_VERSION STRING, OS_FAMILY STRING, DEVICE_FAMILY STRING, DEVICE_BRAND STRING, DEVICE_MODEL STRING, CITY STRING, _rescued_data STRING"
  ) AS kafka_value
FROM
  STREAM (
    read_kafka(
      bootstrapServers => '${bootstrapservers}',
      subscribe => '${topic}',
      `kafka.sasl.mechanism` => 'AWS_MSK_IAM',
      `kafka.sasl.jaas.config` => "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;",
      `kafka.security.protocol` => 'SASL_SSL',
      `startingOffsets`=> 'earliest',
      `kafka.sasl.client.callback.handler.class` => "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    )
  )

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Processamento dos Dados (Silver Layer)
-- MAGIC
-- MAGIC Na camada Silver, aplicamos validações e estruturação nos dados:
-- MAGIC - Validamos campos obrigatórios
-- MAGIC - Removemos registros inválidos
-- MAGIC - Estruturamos os dados em colunas tipadas

-- COMMAND ----------

-- DBTITLE 1,Criando tabela Silver com validações
CREATE OR REFRESH STREAMING TABLE clickstream_silver
(
  CONSTRAINT session_id_not_null EXPECT (SESSION_ID IS NOT NULL)  ,
  CONSTRAINT timestamp_not_null EXPECT (TIMESTAMP IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT page_name_not_null EXPECT (PAGE_NAME IS NOT NULL) ,
  CONSTRAINT browser_family_not_null EXPECT (BROWSER_FAMILY IS NOT NULL) ,
  CONSTRAINT browser_version_not_null EXPECT (BROWSER_VERSION IS NOT NULL) ,
  CONSTRAINT os_family_not_null EXPECT (OS_FAMILY IS NOT NULL) ,
  CONSTRAINT device_family_not_null EXPECT (DEVICE_FAMILY IS NOT NULL) ,
  CONSTRAINT device_brand_not_null EXPECT (DEVICE_BRAND IS NOT NULL) ,
  CONSTRAINT device_model_not_null EXPECT (DEVICE_MODEL IS NOT NULL) ,
  CONSTRAINT city_not_null EXPECT (CITY IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT rescued_data EXPECT (_rescued_data IS NULL) ON VIOLATION DROP ROW
) 
AS
SELECT
  kafka_value.*
FROM
  STREAM(clickstream_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Análises e Agregações (Gold Layer)
-- MAGIC
-- MAGIC Na camada Gold, criamos views materializadas com métricas de negócio:
-- MAGIC - Total de clicks por sistema operacional
-- MAGIC - Total de clicks por tipo de dispositivo
-- MAGIC - Total de clicks por cidade

-- COMMAND ----------

-- DBTITLE 1,Criando views materializadas com métricas de negócio
CREATE OR REPLACE MATERIALIZED VIEW clickstream_gold_clicks_total_by_os_family AS
SELECT
  OS_FAMILY,
  COUNT(*) AS Count_OS_Family
FROM
  clickstream_silver
GROUP BY
  OS_FAMILY;

CREATE OR REPLACE MATERIALIZED VIEW clickstream_gold_clicks_total_by_device_family AS
SELECT
  DEVICE_FAMILY,
  COUNT(*) AS Count_Device_Family
FROM
  clickstream_silver
GROUP BY
  DEVICE_FAMILY;

CREATE OR REPLACE MATERIALIZED VIEW clickstream_gold_clicks_total_by_city AS
SELECT
  CITY,
  COUNT(*) AS Count_City
FROM
  clickstream_silver
GROUP BY
  CITY;
