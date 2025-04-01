# Databricks notebook source

import dlt
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)
from typing import Dict, Any
from pyspark.sql import SparkSession


# Definição do schema dos dados do Kafka
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


def get_kafka_options() -> Dict[str, Any]:
    """
    Retorna as configurações de conexão com o Kafka.
    
    Returns:
        Dict[str, Any]: Dicionário com as configurações do Kafka
    """
    callback_handler = (
        "shadedmskiam.software.amazon.msk.auth.iam"
        ".IAMClientCallbackHandler"
    )
    jaas_config = (
        "shadedmskiam.software.amazon.msk.auth.iam"
        ".IAMLoginModule required;"
    )
    
    return {
        "kafka.bootstrap.servers": "${bootstrapservers}",
        "subscribe": "${topic}",
        "kafka.sasl.mechanism": "AWS_MSK_IAM",
        "kafka.sasl.jaas.config": jaas_config,
        "kafka.security.protocol": "SASL_SSL",
        "startingOffsets": "earliest",
        "kafka.sasl.client.callback.handler.class": callback_handler
    }


# COMMAND ----------

@dlt.table(
    name="pyspark_clickstream_bronze",
    comment="Camada Bronze: Dados brutos do Kafka"
)
def clickstream_bronze():
    """
    Lê os dados brutos do Kafka e armazena na camada Bronze.
    Os dados são mantidos em seu formato original, apenas com parse do JSON.
    """
    spark = SparkSession.builder.getOrCreate()
    return (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options())
        .load()
        .select(
            from_json(col("value").cast("string"), CLICKSTREAM_SCHEMA)
            .alias("kafka_value")
        )
        .select("kafka_value.*")
    )


# COMMAND ----------

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

@dlt.view(
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


@dlt.view(
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


@dlt.view(
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