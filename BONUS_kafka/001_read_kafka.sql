-- Databricks notebook source
-- DBTITLE 1,Select no MSK
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
-- MAGIC inserir desenho medallion Again

-- COMMAND ----------

-- DBTITLE 1,Criando tabela "streaming" lendo da tabela raw para a tabela CLICKSTREAM
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

-- DBTITLE 1,Criando tabelas adicionais (view materializadas)
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
