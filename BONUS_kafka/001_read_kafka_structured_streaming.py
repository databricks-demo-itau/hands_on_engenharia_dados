# Databricks notebook source
# MAGIC %md
# MAGIC # Leitura de Dados do Kafka com Spark Structured Streaming
# MAGIC
# MAGIC Este notebook demonstra como implementar a leitura de dados do Apache 
# MAGIC Kafka usando Spark Structured Streaming.
# MAGIC
# MAGIC ## Documentação Oficial
# MAGIC - [Databricks Structured Streaming with Kafka](
# MAGIC   https://docs.databricks.com/structured-streaming/kafka.html)
# MAGIC - [AWS MSK IAM Authentication](
# MAGIC   https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)
from typing import Dict, Any


# COMMAND ----------

# DBTITLE 1,Configuração do Schema e Usuário
# Schema dos dados do Kafka
CLICKSTREAM_SCHEMA = StructType([
    StructField("SESSION_ID", StringType(), True),
    StructField("TIMESTAMP", TimestampType(), True),
    StructField("PAGE_NAME", StringType(), True),
    StructField("BROWSER_FAMILY", StringType(), True),
    StructField("BROWSER_VERSION", StringType(), True),
    StructField("OS_FAMILY", StringType(), True),
    StructField("DEVICE_FAMILY", StringType(), True),
    StructField("DEVICE_BRAND", StringType(), True),
    StructField("DEVICE_MODEL", StringType(), True),
    StructField("CITY", StringType(), True),
    StructField("_rescued_data", StringType(), True)
])

# Obtém o nome do usuário formatado
user_name = (dbutils.notebook.entry_point.getDbutils().notebook()
             .getContext().userName().toString()
             .split('@')[0].split('(')[1].replace('.', '_'))


# COMMAND ----------

# DBTITLE 1,Configuração do Catalog e Schema
# MAGIC %sql
# MAGIC USE CATALOG dev_hands_on;
# MAGIC USE SCHEMA IDENTIFIER($user_name);


# COMMAND ----------

# DBTITLE 1,Criação do Volume para Checkpoint
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS sst_kafka_checkpoint;


# COMMAND ----------

# DBTITLE 1,Configuração do Kafka
def get_kafka_options(bootstrapservers: str, topic: str) -> Dict[str, Any]:
    """
    Retorna as configurações básicas de conexão com o Kafka usando AWS MSK IAM.
    
    Args:
        bootstrapservers: Endereço dos servidores Kafka
        topic: Nome do tópico Kafka
        
    Returns:
        Dict[str, Any]: Configurações do Kafka
    """
    handler = "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    jaas = "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;"
    
    return {
        "kafka.bootstrap.servers": bootstrapservers,
        "subscribe": topic,
        "kafka.sasl.mechanism": "AWS_MSK_IAM",
        "kafka.sasl.jaas.config": jaas,
        "kafka.security.protocol": "SASL_SSL",
        "startingOffsets": "earliest",
        "kafka.sasl.client.callback.handler.class": handler
    }


# COMMAND ----------

# DBTITLE 1,Leitura do Kafka e Escrita na Camada Bronze
# Configurações do Kafka
bootstrapservers = spark.conf.get("bootstrapservers")
topic = spark.conf.get("topic")

# Leitura do stream do Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .options(**get_kafka_options(
        bootstrapservers=bootstrapservers, 
        topic=topic
    ))
    .load()
)

# Processamento do stream
bronze_stream = (
    kafka_stream
    .select(
        from_json(col("value").cast("string"), CLICKSTREAM_SCHEMA)
        .alias("kafka_value")
    )
    .select("kafka_value.*")
)

# Escrita do stream na tabela bronze
query = (
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation", 
        f"/Volumes/dev_hands_on/{user_name}/sst_kafka_checkpoint/bronze"
    )
    .trigger(processingTime="10 seconds")
    .table("sst_clickstream_bronze")
)


# COMMAND ----------

# DBTITLE 1,Limpeza dos Assets
# MAGIC %sql
# MAGIC -- Aguarda 1 minuto para demonstração e então limpa os assets
# MAGIC SELECT sleep(60000);
# MAGIC 
# MAGIC -- Drop da tabela bronze
# MAGIC DROP TABLE IF EXISTS sst_clickstream_bronze;
# MAGIC 
# MAGIC -- Drop do volume de checkpoint
# MAGIC DROP VOLUME sst_kafka_checkpoint; 