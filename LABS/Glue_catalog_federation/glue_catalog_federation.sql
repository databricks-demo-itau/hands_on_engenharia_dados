-- Databricks notebook source

-- MAGIC %md
-- MAGIC # Federação de Catálogo AWS Glue no Unity Catalog
-- MAGIC 
-- MAGIC ## Introdução
-- MAGIC 
-- MAGIC Este notebook demonstra como utilizar a funcionalidade de federação de catálogo do AWS Glue no Unity Catalog do Databricks. 
-- MAGIC A federação de catálogo permite que você acesse e gerencie tabelas registradas no AWS Glue diretamente através do Unity Catalog, 
-- MAGIC sem necessidade de ETL ou cópia de dados.
-- MAGIC 
-- MAGIC ### Conceitos Importantes
-- MAGIC 
-- MAGIC 1. **Federação de Catálogo**: Permite que o Unity Catalog gerencie e acesse tabelas registradas em outro metastore (neste caso, o AWS Glue)
-- MAGIC 2. **Catálogo Externo**: Um catálogo no Unity Catalog que representa as tabelas do AWS Glue
-- MAGIC 3. **Conexão**: Define como o Unity Catalog se conecta ao AWS Glue
-- MAGIC 4. **Credenciais de Armazenamento**: Especifica as credenciais necessárias para acessar os dados no S3
-- MAGIC 
-- MAGIC ### Pré-requisitos
-- MAGIC 
-- MAGIC Para este lab, já temos:
-- MAGIC - Catálogo federado `dev_glue` configurado
-- MAGIC - Schema `product` com as tabelas:
-- MAGIC   - `portfolio`
-- MAGIC   - `price`

-- MAGIC %md
-- MAGIC ## Configuração Inicial
-- MAGIC 
-- MAGIC Primeiro, vamos obter o nome do usuário formatado conforme as regras do hands-on:

DECLARE OR REPLACE VARIABLE user_name STRING;
SET VARIABLE user_name = (select replace(split(current_user(), '@')[0], '.', '_') as user_name);

-- MAGIC %md
-- MAGIC ## Explorando as Tabelas Federadas
-- MAGIC 
-- MAGIC Vamos explorar as tabelas disponíveis no catálogo federado:

USE CATALOG dev_glue;
USE SCHEMA product;

-- Visualizando os dados da tabela portfolio
SELECT * FROM portfolio LIMIT 5;

-- Visualizando os dados da tabela price
SELECT * FROM price LIMIT 5;

-- MAGIC %md
-- MAGIC ## Clonando Tabelas para o Unity Catalog
-- MAGIC 
-- MAGIC Agora vamos demonstrar como clonar as tabelas do AWS Glue para o Unity Catalog usando a técnica de clone.
-- MAGIC Este processo é eficiente pois não copia os dados, apenas os metadados.

-- Configurando o schema do usuário
USE CATALOG dev_hands_on;
USE SCHEMA IDENTIFIER(user_name);

-- Clonando a tabela portfolio
CREATE TABLE IF NOT EXISTS portfolio_clone 
CLONE dev_glue.product.portfolio;

-- Clonando a tabela price
CREATE TABLE IF NOT EXISTS price_clone 
CLONE dev_glue.product.price;

-- MAGIC %md
-- MAGIC ## Verificando as Tabelas Clonadas
-- MAGIC 
-- MAGIC Vamos verificar se as tabelas foram clonadas corretamente:

-- Consultando a tabela portfolio clonada
SELECT * FROM portfolio_clone LIMIT 5;

-- Consultando a tabela price clonada
SELECT * FROM price_clone LIMIT 5;

-- MAGIC %md
-- MAGIC ## Limpeza dos Recursos
-- MAGIC 
-- MAGIC Por fim, vamos limpar os recursos criados:

DROP TABLE IF EXISTS portfolio_clone;
DROP TABLE IF EXISTS price_clone;