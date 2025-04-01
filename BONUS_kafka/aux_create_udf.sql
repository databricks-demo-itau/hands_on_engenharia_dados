-- Databricks notebook source

-- DBTITLE 1,Obtendo o nome do usuário formatado
-- MAGIC %python
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName()
user_name = user_name.toString().split('@')[0].split('(')[1].replace('.', '_')
spark.sql(f"SET schema_name = '{user_name}'")

-- COMMAND ----------

-- DBTITLE 1,Configurando o catalog e schema
USE CATALOG dev_hands_on;
USE SCHEMA IDENTIFIER(${schema_name});

-- COMMAND ----------

-- DBTITLE 1,Criando a UDF ler_kafka
CREATE OR REPLACE FUNCTION ler_kafka(bootstrapServers STRING, topic STRING)
RETURNS TABLE (value STRING)
RETURN 
  SELECT value
  FROM read_kafka(
    bootstrapServers => bootstrapServers,
    subscribe => topic,
    `kafka.sasl.mechanism` => 'AWS_MSK_IAM',
    `kafka.sasl.jaas.config` => "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;",
    `kafka.security.protocol` => 'SASL_SSL',
    `startingOffsets`=> 'earliest',
    `kafka.sasl.client.callback.handler.class` => "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
  );

-- COMMAND ----------

-- DBTITLE 1,Exemplo de uso da UDF
-- SELECT * FROM TABLE(ler_kafka('seu_bootstrap_servers', 'seu_topico')); 