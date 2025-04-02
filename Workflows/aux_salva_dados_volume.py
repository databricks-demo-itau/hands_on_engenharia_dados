# Databricks notebook source

# MAGIC %md
# MAGIC # Geração de Dados de Exemplo para Volume
# MAGIC 
# MAGIC Este notebook é responsável por gerar dados sintéticos de clientes e transações 
# MAGIC para serem salvos em um Volume do Unity Catalog.
# MAGIC 
# MAGIC ## Objetivo
# MAGIC - Gerar dados realistas de clientes com informações pessoais
# MAGIC - Gerar dados de transações associadas a estes clientes
# MAGIC - Salvar os dados no formato JSON em um Volume específico do usuário


# COMMAND ----------

# Obtém o nome do usuário formatado para uso no caminho do volume
user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName()
user_name = (
    user_name.toString().split('@')[0].split('(')[1].replace('.', '_')
)

# COMMAND ----------

# Define o caminho do volume onde os dados serão salvos
volume_path = f"/Volumes/dev_hands_on/{user_name}/raw_data"
print(volume_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Instalação de Dependências
# MAGIC Instalando a biblioteca Faker para geração de dados sintéticos

# COMMAND ----------

# MAGIC %pip install Faker

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geração e Salvamento dos Dados
# MAGIC O código abaixo gera dados sintéticos para clientes e transações, 
# MAGIC salvando-os no volume especificado.

# COMMAND ----------

try:
    # Verifica se os diretórios de destino existem
    dbutils.fs.ls(volume_path+"/transactions")
    dbutils.fs.ls(volume_path+"/customers")

    # Importação das bibliotecas necessárias
    from pyspark.sql import functions as F
    from faker import Faker
    from collections import OrderedDict 
    import uuid
    import random
    fake = Faker()

    # Definição das funções UDF para geração de dados
    fake_firstname = F.udf(fake.first_name)
    fake_lastname = F.udf(fake.last_name)
    fake_email = F.udf(fake.ascii_company_email)
    fake_date = F.udf(
        lambda: fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S")
    )
    fake_address = F.udf(fake.address)
    
    # Define as probabilidades de cada tipo de operação
    operations = OrderedDict([
        ("APPEND", 0.5),
        ("DELETE", 0.1),
        ("UPDATE", 0.3),
        (None, 0.01)
    ])
    fake_operation = F.udf(
        lambda: fake.random_elements(elements=operations, length=1)[0]
    )
    fake_id = F.udf(
        lambda: str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None
    )

    # Geração dos dados de clientes
    df = spark.range(0, 100000).repartition(100)
    df = df.withColumn("id", fake_id())
    df = df.withColumn("firstname", fake_firstname())
    df = df.withColumn("lastname", fake_lastname())
    df = df.withColumn("email", fake_email())
    df = df.withColumn("address", fake_address())
    df = df.withColumn("operation", fake_operation())
    df_customers = df.withColumn("operation_date", fake_date())
    
    # Salva os dados de clientes
    (df_customers.repartition(100)
     .write
     .format("json")
     .mode("append")
     .save(volume_path+"/customers"))

    # Geração dos dados de transações
    df = spark.range(0, 10000).repartition(20)
    df = df.withColumn("id", fake_id())
    df = df.withColumn("transaction_date", fake_date())
    df = df.withColumn("amount", F.round(F.rand()*1000))
    df = df.withColumn("item_count", F.round(F.rand()*10))
    df = df.withColumn("operation", fake_operation())
    df = df.withColumn("operation_date", fake_date())
    
    # Join com os dados de clientes para garantir IDs válidos
    customers_df = (
        spark.read.json(volume_path+"/customers")
        .select("id")
        .withColumnRenamed("id", "customer_id")
        .withColumn("t_id", F.monotonically_increasing_id())
    )
    
    df = (
        df.withColumn("t_id", F.monotonically_increasing_id())
        .join(customers_df, "t_id")
        .drop("t_id")
    )
    
    # Salva os dados de transações
    (df.repartition(10)
     .write
     .format("json")
     .mode("append")
     .save(volume_path+"/transactions"))

except Exception as e:  
    msg = (
        "É necessário instalar o dbdemos dlt-cdc antes de executar este notebook."
    )
    print(msg)
    print(e)
