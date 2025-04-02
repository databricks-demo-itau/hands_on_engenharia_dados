# Databricks notebook source
# MAGIC %md
# MAGIC # Demonstração de Leitura de Dados do Kafka com PySpark
# MAGIC
# MAGIC Este notebook demonstra como implementar a leitura de dados do Apache Kafka 
# MAGIC no Databricks usando PySpark, utilizando a arquitetura Medallion.
# MAGIC
# MAGIC ## Documentação Oficial
# MAGIC - [Databricks Structured Streaming with Kafka](
# MAGIC   https://docs.databricks.com/structured-streaming/kafka.html)
# MAGIC - [AWS MSK IAM Authentication](
# MAGIC   https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html)
# MAGIC
# MAGIC ## Cenário de Demonstração
# MAGIC Vamos criar um cenário prático onde:
# MAGIC 1. Lemos dados de clickstream em tempo real do Kafka
# MAGIC 2. Implementamos a arquitetura Medallion (Bronze, Silver, Gold)
# MAGIC 3. Aplicamos validações e transformações nos dados
# MAGIC
# MAGIC ### Arquitetura de Dados
# MAGIC - **Bronze**: Dados brutos do Kafka
# MAGIC - **Silver**: Dados validados e estruturados
# MAGIC - **Gold**: Agregações e métricas de negócio

# COMMAND ----------

# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)
from typing import Dict, Any

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Definição do Schema
# MAGIC Definimos o schema dos dados que serão lidos do Kafka para garantir a tipagem correta.

# COMMAND ----------

# DBTITLE 1,Bronze Schema
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração do Kafka
# MAGIC Configuramos a conexão com o Kafka usando autenticação AWS MSK IAM.

# COMMAND ----------

# DBTITLE 1,Kafka Options & Autenticação
def get_kafka_options(bootstrapservers: str, topic: str) -> Dict[str, Any]:
    """
    Retorna as configurações básicas de conexão com o Kafka usando AWS MSK IAM.
    
    Args:
        bootstrapservers: Endereço dos servidores Kafka
        topic: Nome do tópico Kafka
        
    Returns:
        Dict[str, Any]: Configurações do Kafka
    """
    return {
        "kafka.bootstrap.servers": bootstrapservers,
        "subscribe": topic,
        "kafka.sasl.mechanism": "AWS_MSK_IAM",
        "kafka.sasl.jaas.config": (
            "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;"
        ),
        "kafka.security.protocol": "SASL_SSL",
        "startingOffsets": "earliest",
        "kafka.sasl.client.callback.handler.class": (
            "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        )
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Camada Bronze
# MAGIC Na camada Bronze, realizamos a ingestão dos dados brutos do Kafka.
# MAGIC Os dados são lidos em formato JSON e armazenados sem transformações.

# COMMAND ----------

# DBTITLE 1,Bronze
@dlt.table(
    name="pyspark_clickstream_bronze",
    comment="Camada Bronze: Dados brutos do Kafka"
)
def clickstream_bronze():
    """
    Lê os dados brutos do Kafka e armazena na camada Bronze.
    Os dados são mantidos em seu formato original, apenas com parse do JSON.
    """
    bootstrapservers = spark.conf.get("bootstrapservers")
    topic = spark.conf.get("topic")

    return (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options(
            bootstrapservers=bootstrapservers, 
            topic=topic
        ))
        .load()
        .select(
            from_json(col("value").cast("string"), CLICKSTREAM_SCHEMA)
            .alias("kafka_value")
        )
        .select("kafka_value.*")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Camada Silver
# MAGIC Na camada Silver, aplicamos validações e estruturação nos dados:
# MAGIC - Validamos campos obrigatórios
# MAGIC - Removemos registros inválidos
# MAGIC - Estruturamos os dados em colunas tipadas

# COMMAND ----------

# DBTITLE 1,Silver com Expectations
@dlt.table(
    name="pyspark_clickstream_silver",
    comment="Camada Silver: Dados validados e estruturados",
    table_properties={
        "quality": "silver",
        "pipelines.reset.allowed": "false"
    }
)
@dlt.expect_or_drop("session_id_not_null", "SESSION_ID IS NOT NULL")
@dlt.expect_or_drop("timestamp_not_null", "TIMESTAMP IS NOT NULL")
@dlt.expect_or_drop("page_name_not_null", "PAGE_NAME IS NOT NULL")
@dlt.expect_or_drop("browser_family_not_null", "BROWSER_FAMILY IS NOT NULL")
@dlt.expect_or_drop("browser_version_not_null", "BROWSER_VERSION IS NOT NULL")
@dlt.expect_or_drop("os_family_not_null", "OS_FAMILY IS NOT NULL")
@dlt.expect_or_drop("device_family_not_null", "DEVICE_FAMILY IS NOT NULL")
@dlt.expect_or_drop("device_brand_not_null", "DEVICE_BRAND IS NOT NULL")
@dlt.expect_or_drop("device_model_not_null", "DEVICE_MODEL IS NOT NULL")
@dlt.expect_or_drop("city_not_null", "CITY IS NOT NULL")
@dlt.expect_or_drop("rescued_data", "_rescued_data IS NULL")
def clickstream_silver():
    """
    Processa os dados da camada Bronze, aplicando validações e estruturação.
    Remove registros que não atendem às expectativas de qualidade.
    """
    return dlt.read_stream("pyspark_clickstream_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Camada Gold
# MAGIC Na camada Gold, criamos views materializadas com métricas de negócio:
# MAGIC - Total de clicks por sistema operacional
# MAGIC - Total de clicks por tipo de dispositivo
# MAGIC - Total de clicks por cidade

# COMMAND ----------

# DBTITLE 1,Gold
@dlt.table(
    name="pyspark_clickstream_gold_clicks_total_by_os_family",
    comment="Camada Gold: Total de clicks por sistema operacional"
)
def clickstream_gold_clicks_total_by_os_family():
    """
    Cria uma view materializada com o total de clicks por sistema operacional.
    """
    return (
        dlt.read("pyspark_clickstream_silver")
        .groupBy("OS_FAMILY")
        .count()
        .withColumnRenamed("count", "Count_OS_Family")
    )


@dlt.table(
    name="pyspark_clickstream_gold_clicks_total_by_device_family",
    comment="Camada Gold: Total de clicks por tipo de dispositivo"
)
def clickstream_gold_clicks_total_by_device_family():
    """
    Cria uma view materializada com o total de clicks por tipo de dispositivo.
    """
    return (
        dlt.read("pyspark_clickstream_silver")
        .groupBy("DEVICE_FAMILY")
        .count()
        .withColumnRenamed("count", "Count_Device_Family")
    )


@dlt.table(
    name="pyspark_clickstream_gold_clicks_total_by_city",
    comment="Camada Gold: Total de clicks por cidade"
)
def clickstream_gold_clicks_total_by_city():
    """
    Cria uma view materializada com o total de clicks por cidade.
    """
    return (
        dlt.read("pyspark_clickstream_silver")
        .groupBy("CITY")
        .count()
        .withColumnRenamed("count", "Count_City")
    )
