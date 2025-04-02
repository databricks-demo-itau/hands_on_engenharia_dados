-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Demonstração de SQL Scripting com API
-- MAGIC
-- MAGIC Este notebook demonstra como utilizar SQL Scripting para fazer requisições HTTP e tratar os dados retornados de uma API.
-- MAGIC
-- MAGIC ## Documentação Oficial
-- MAGIC - [SQL Scripting](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting)
-- MAGIC - [Função HTTP Request](https://docs.databricks.com/aws/en/sql/language-manual/functions/http_request)
-- MAGIC
-- MAGIC ## Cenário de Demonstração
-- MAGIC Vamos criar um cenário prático onde:
-- MAGIC 1. Fazemos uma requisição HTTP para a API de terremotos do USGS
-- MAGIC 2. Tratamos a resposta usando SQL Scripting
-- MAGIC 3. Salvamos os dados em uma tabela Delta
-- MAGIC 4. Implementamos tratamento de erros
-- MAGIC
-- MAGIC ### Requisitos
-- MAGIC - Conexão HTTP configurada no Unity Catalog
-- MAGIC - DBR 16.3 ou superior para SQL Scripting

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuração da Conexão HTTP
-- MAGIC
-- MAGIC Neste exemplo, vamos utilizar a função [http_request](https://docs.databricks.com/aws/en/sql/language-manual/functions/http_request#:~:text=Makes%20an%20HTTP%20request%20using,function%20requires%20named%20parameter%20invocation.). Para isso, precisamos primeiro criar uma conexão no Unity Catalog. Para esta demo, a conexão já foi criada para nós, mas o comando seria assim:
-- MAGIC
-- MAGIC ```sql
-- MAGIC CREATE CONNECTION IF NOT EXISTS usgs_conn
-- MAGIC   type HTTP
-- MAGIC   options (
-- MAGIC   host 'https://earthquake.usgs.gov',
-- MAGIC   port '443',
-- MAGIC   base_path '/earthquakes/feed/v1.0/',
-- MAGIC   bearer_token 'na'
-- MAGIC   );
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Configuração do Schema
-- MAGIC
-- MAGIC Primeiro, vamos configurar o schema no Unity Catalog usando o nome do usuário.
-- MAGIC Esta é uma prática recomendada para garantir isolamento de dados entre diferentes usuários.

-- COMMAND ----------

-- DBTITLE 1,Configurar variável com nome do usuário
DECLARE OR REPLACE VARIABLE user_name STRING;
SET VARIABLE user_name = (select replace(split(current_user, '@')[0], '.', '_') as user_name)

-- COMMAND ----------

-- DBTITLE 1,Criar Schema
CREATE SCHEMA IF NOT EXISTS IDENTIFIER("dev_hands_on." || user_name)

-- COMMAND ----------

-- DBTITLE 1,Configurar Catalog e Schema
use catalog dev_hands_on;
use schema identifier(user_name) 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Criar Tabela de Log
-- MAGIC
-- MAGIC Vamos criar uma tabela para registrar erros nas requisições HTTP.
-- MAGIC Isso é importante para monitoramento e troubleshooting.

-- COMMAND ----------

-- DBTITLE 1,Criar Tabela de Log
create table if not exists earthquakes_log (
  timestamp timestamp,
  job_id string,
  job_run_id string,
  status_code int,
  text string
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Fazer Requisição HTTP
-- MAGIC
-- MAGIC Utilizaremos a função `http_request` para acessar a API do USGS.
-- MAGIC A conexão HTTP já deve estar configurada no Unity Catalog.

-- COMMAND ----------

-- DBTITLE 1,Declarar Variável para Resposta HTTP
declare or replace resp struct<status_code int, text string>;

set var resp = (
  select
    http_request(
      conn => concat('usgs_conn'),
      method => 'GET',
      path => 'summary/all_day.geojson'
    )
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Processar Dados da API
-- MAGIC
-- MAGIC Vamos utilizar o tipo de dado Variant para processar o JSON retornado pela API.
-- MAGIC Para mais informações sobre o tipo Variant, consulte:
-- MAGIC [Variant Data Type](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)

-- COMMAND ----------

-- DBTITLE 1,"Selecting" from the API
select explode(variant_get(parse_json(resp.text), '$.features')::array<variant>) as col

-- COMMAND ----------

-- DBTITLE 1,Lendo os dados da API e transformando os dados Variant
with raw_data as (
      select explode(variant_get(parse_json(resp.text), '$.features')::array<variant>) as col
    )
    select
      col:id::string,
      col:properties.mag::double,
      col:properties.place::string,
      from_unixtime(col:properties.time::bigint/1000) as time,
      col:properties.updated::bigint,
      col:properties.tz::string,
      col:properties.url::string,
      col:properties.detail::string,
      col:properties.felt::string,
      col:properties.cdi::string,
      col:properties.mmi::double,
      col:properties.alert::string,
      col:properties.status::string,
      col:properties.tsunami::string,
      col:properties.sig::bigint,
      col:properties.net::string,
      col:properties.code::string,
      col:properties.ids::string,
      col:properties.sources::string,
      col:properties.types::string,
      col:properties.nst::bigint,
      col:properties.dmin::double,
      col:properties.rms::double,
      col:properties.gap::double,
      col:properties.magType::string,
      col:properties.type::string,
      col:properties.title::string,
      col:geometry.coordinates[0]::double as longitude,
      col:geometry.coordinates[1]::double as latitude,
      col:geometry.coordinates[2]::double as depth
    from raw_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 5. Salvar Dados e Tratar Erros
-- MAGIC
-- MAGIC Utilizando SQL Scripting, vamos:
-- MAGIC 1. Verificar o status code da resposta
-- MAGIC 2. Se for 200, salvar os dados em uma tabela
-- MAGIC 3. Se houver erro, registrar na tabela de log

-- COMMAND ----------

-- DBTITLE 1,Salvando dados em Tabela e logando os erros
-- Log non-200 responses to Delta table
-- Script must be in separate notebook cell
begin
  if resp.status_code == 200 then
    create or replace table earthquakes as
    with raw_data as (
      select explode(variant_get(parse_json(resp.text), '$.features')::array<variant>) as col
    )
    select
      col:id::string,
      col:properties.mag::double,
      col:properties.place::string,
      from_unixtime(col:properties.time::bigint/1000) as time,
      col:properties.updated::bigint,
      col:properties.tz::string,
      col:properties.url::string,
      col:properties.detail::string,
      col:properties.felt::string,
      col:properties.cdi::string,
      col:properties.mmi::double,
      col:properties.alert::string,
      col:properties.status::string,
      col:properties.tsunami::string,
      col:properties.sig::bigint,
      col:properties.net::string,
      col:properties.code::string,
      col:properties.ids::string,
      col:properties.sources::string,
      col:properties.types::string,
      col:properties.nst::bigint,
      col:properties.dmin::double,
      col:properties.rms::double,
      col:properties.gap::double,
      col:properties.magType::string,
      col:properties.type::string,
      col:properties.title::string,
      col:geometry.coordinates[0]::double as longitude,
      col:geometry.coordinates[1]::double as latitude,
      col:geometry.coordinates[2]::double as depth
    from raw_data;
  else
    insert into IDENTIFIER(:catalog || '.' || :schema || '.' || 'earthquakes_log')
      values (current_timestamp(), :job_id, :job_run_id, resp.status_code, resp.text);
  end if;
end;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 6. Verificar Dados
-- MAGIC
-- MAGIC Vamos consultar os dados salvos para confirmar que tudo funcionou corretamente.

-- COMMAND ----------

select * from earthquakes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 7. Limpeza
-- MAGIC
-- MAGIC Por fim, vamos remover os assets criados para manter o ambiente organizado.

-- COMMAND ----------

-- DBTITLE 1,Remover assets
drop table earthquakes_log;
drop table earthquakes;