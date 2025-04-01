-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Demonstração de Row Level Security e Column Masking
-- MAGIC
-- MAGIC Este notebook demonstra como usar Row Level Security (RLS) e Column Masking no Databricks.
-- MAGIC Vamos criar um cenário simples onde:
-- MAGIC 1. Usuários com acesso especial podem ver todos os dados (usando mapping table)
-- MAGIC 2. Informações sensíveis são mascaradas para usuários sem permissão (Column Masking)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configuração do Schema
-- MAGIC Primeiro, vamos configurar o schema no Unity Catalog usando o nome do usuário.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Obtém o nome do usuário e formata para o schema
-- MAGIC user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName()
-- MAGIC user_name = user_name.toString().split('@')[0].split('(')[1].replace('.', '_')
-- MAGIC
-- MAGIC # Define o schema no Unity Catalog
-- MAGIC schema_name = user_name
-- MAGIC
-- MAGIC # Cria o schema se não existir
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS dev_hands_on.{schema_name}")
-- MAGIC
-- MAGIC # Define a variável para uso no SQL
-- MAGIC spark.sql("DECLARE VARIABLE schema_name STRING")
-- MAGIC spark.sql(f"SET VARIABLE schema_name = '{schema_name}'")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Configurar Catalog e Schema
-- MAGIC Vamos configurar o catalog e schema para uso no notebook.

-- COMMAND ----------

-- DBTITLE 1,Configurar catalog e schema
USE CATALOG dev_hands_on;
USE SCHEMA IDENTIFIER(schema_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar Tabela de Exemplo
-- MAGIC Vamos criar uma tabela Delta com dados de funcionários para demonstrar as funcionalidades.

-- COMMAND ----------

-- DBTITLE 1,Criar tabela e inserir dados
-- Criando uma tabela Delta com dados de funcionários
CREATE OR REPLACE TABLE funcionarios (
  id INT,
  nome STRING,
  departamento STRING,
  salario DECIMAL(10,2),
  cpf STRING,
  data_admissao DATE
) USING DELTA;

-- Inserindo dados de exemplo
INSERT INTO funcionarios VALUES
  (1, 'João Silva', 'TI', 5000.00, '123.456.789-00', '2023-01-15'),
  (2, 'Maria Santos', 'RH', 4500.00, '987.654.321-00', '2023-02-20'),
  (3, 'Pedro Oliveira', 'TI', 4800.00, '456.789.123-00', '2023-03-10'),
  (4, 'Ana Costa', 'Financeiro', 5500.00, '789.123.456-00', '2023-04-05'),
  (5, 'Carlos Santos', 'Financeiro', 6000.00, '321.654.987-00', '2023-05-01'),
  (6, schema_name, 'Financeiro', 6000.00, '123.456.789-10', '2023-05-01');

-- COMMAND ----------

-- DBTITLE 1,Verificar dados originais
-- Verificando os dados originais antes de aplicar RLS
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Criar Mapping Table
-- MAGIC Vamos criar uma tabela que contém os usuários com acesso especial.

-- COMMAND ----------

-- DBTITLE 1,Criar mapping table
-- Criando uma tabela para mapear usuários com acesso especial
CREATE OR REPLACE TABLE usuarios_acesso_especial (
  email STRING,
  departamento STRING,
  CPF STRING
) USING DELTA;

-- Inserindo alguns usuários com acesso especial
INSERT INTO usuarios_acesso_especial VALUES
  ('admin@databricks.com', 'TI', '0000'),
  ('rh@databricks.com', 'RH', '0000');

-- COMMAND ----------

-- DBTITLE 1,Select na tabela que controla os acessos
SELECT * FROM usuarios_acesso_especial

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Implementar Row Level Security
-- MAGIC Agora vamos criar uma função UDF que verifica se o usuário está na mapping table.

-- COMMAND ----------

-- DBTITLE 1,Criar função UDF para RLS
-- Criando função UDF que verifica se o usuário está na mapping table
CREATE OR REPLACE FUNCTION acesso_especial_filter (departamento STRING)
RETURN EXISTS(
  SELECT 1 FROM usuarios_acesso_especial u
  WHERE u.email = CURRENT_USER()
  AND u.departamento = departamento
) and departamento in (select departamento from usuarios_acesso_especial f where f.email = CURRENT_USER())

-- COMMAND ----------

-- DBTITLE 1,Select antes de aplicar RLS
-- Verificando os dados originais antes de aplicar RLS
SELECT * FROM funcionarios;

-- COMMAND ----------

-- DBTITLE 1,Aplicar política de RLS
-- Aplicando a política de RLS à tabela
ALTER TABLE funcionarios
SET ROW FILTER acesso_especial_filter ON (departamento);

-- COMMAND ----------

-- DBTITLE 1,Realizando na tabela com RLS
-- Verificando como o usuário atual vê os dados
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Adicionar Usuário Atual à Mapping Table
-- MAGIC Vamos adicionar o usuário atual à mapping table e ver o efeito.

-- COMMAND ----------

-- DBTITLE 1,Insert na Mapping Table
-- Adicionando o usuário atual à mapping table
INSERT INTO usuarios_acesso_especial 
SELECT CURRENT_USER(), 'Financeiro','123.456.789-10';

-- COMMAND ----------

-- DBTITLE 1,Select com linhas Filtradas
-- Verificando como o usuário atual vê os dados
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Implementar Column Masking
-- MAGIC Vamos criar funções UDF para as máscaras de colunas e aplicá-las à tabela.

-- COMMAND ----------

-- DBTITLE 1,Criar funções UDF para máscaras
-- Criando funções UDF para cada máscara de coluna
CREATE OR REPLACE FUNCTION cpf_mask (cpf STRING)
RETURN CASE
  WHEN EXISTS(
    SELECT 1 FROM usuarios_acesso_especial u
    WHERE u.departamento = 'Financeiro'
    and u.email = CURRENT_USER()
  )
   AND cpf in (select cpf from usuarios_acesso_especial f where f.email = CURRENT_USER()) 
   THEN cpf
  ELSE '***.***.***-**'
END;

CREATE OR REPLACE FUNCTION salario_mask (salario DECIMAL , cpf STRING)
RETURN CASE
  WHEN EXISTS(
    SELECT 1 FROM usuarios_acesso_especial u
    WHERE u.departamento = 'Financeiro'
    and u.email = CURRENT_USER()
  )
   AND cpf in (select cpf from usuarios_acesso_especial f where f.email = CURRENT_USER()) 
   THEN salario
  ELSE 0
END;

-- COMMAND ----------

-- DBTITLE 1,Aplicar máscaras de colunas
-- Aplicando as máscaras às colunas
ALTER TABLE funcionarios ALTER COLUMN cpf SET MASK cpf_mask;
ALTER TABLE funcionarios ALTER COLUMN salario SET MASK salario_mask USING COLUMNS(cpf);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Testar as Políticas
-- MAGIC Vamos verificar como o usuário atual vê os dados.

-- COMMAND ----------

-- DBTITLE 1,Verificar dados após adicionar column MASKING
-- Verificando como o usuário vê os dados após ser adicionado à mapping table
SELECT * FROM funcionarios;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Limpeza
-- MAGIC Por fim, vamos remover todos os assets criados.

-- COMMAND ----------

-- DBTITLE 1,Remover assets
-- Removendo as máscaras e políticas
ALTER TABLE funcionarios ALTER COLUMN cpf DROP MASK;
ALTER TABLE funcionarios ALTER COLUMN salario DROP MASK;
ALTER TABLE funcionarios DROP ROW FILTER;

-- Removendo as funções UDF
DROP FUNCTION IF EXISTS acesso_especial_filter;
DROP FUNCTION IF EXISTS cpf_mask;
DROP FUNCTION IF EXISTS salario_mask;

-- Removendo as tabelas
DROP TABLE IF EXISTS funcionarios;
DROP TABLE IF EXISTS usuarios_acesso_especial; 