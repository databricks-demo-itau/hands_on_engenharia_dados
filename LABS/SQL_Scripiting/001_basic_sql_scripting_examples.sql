-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Exemplos Básicos de SQL Scripting
-- MAGIC
-- MAGIC Este notebook demonstra os conceitos básicos de SQL Scripting no Databricks, incluindo estruturas de controle de fluxo, manipulação de variáveis e operações com tabelas.
-- MAGIC
-- MAGIC ## Requisitos
-- MAGIC - Databricks Runtime (DBR) 16.3 ou superior
-- MAGIC - Unity Catalog configurado
-- MAGIC
-- MAGIC ## Documentação Oficial
-- MAGIC - [SQL Scripting](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-scripting)
-- MAGIC - [LOOP Statement](https://docs.databricks.com/aws/en/sql/language-manual/control-flow/loop-stmt)
-- MAGIC - [IF Statement](https://docs.databricks.com/aws/en/sql/language-manual/control-flow/if-stmt)
-- MAGIC
-- MAGIC ## Cenário de Demonstração
-- MAGIC Vamos explorar:
-- MAGIC 1. Declaração e uso de variáveis
-- MAGIC 2. Estruturas de controle IF/THEN/ELSE com tabelas
-- MAGIC 3. Loops e iterações para inserção de dados
-- MAGIC 4. Tratamento de erros básico
-- MAGIC 5. Manipulação de dados em tabelas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Configuração do Schema
-- MAGIC
-- MAGIC Primeiro, vamos configurar o schema no Unity Catalog usando o nome do usuário.

-- COMMAND ----------

-- DBTITLE 1,Configurar variável com nome do usuário
DECLARE OR REPLACE VARIABLE user_name STRING;
SET VARIABLE user_name = (select replace(split(current_user, '@')[0], '.', '_') as user_name);

-- COMMAND ----------

-- DBTITLE 1,Criar Schema
CREATE SCHEMA IF NOT EXISTS IDENTIFIER("dev_hands_on." || user_name);

-- COMMAND ----------

-- DBTITLE 1,Configurar Catalog e Schema
use catalog dev_hands_on;
use schema identifier(user_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Exemplo com Tabela de Produtos
-- MAGIC
-- MAGIC Vamos criar uma tabela de produtos e manipular seus dados usando SQL Scripting.

-- COMMAND ----------

-- DBTITLE 1,Criar Tabela de Produtos
CREATE OR REPLACE TABLE produtos (
  id INT,
  nome STRING,
  preco DECIMAL(10,2),
  categoria STRING,
  estoque INT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 Inserção de Dados com Loop
-- MAGIC
-- MAGIC Vamos inserir produtos usando um loop, com preços calculados dinamicamente.

-- COMMAND ----------

-- DBTITLE 1,Inserir Produtos com Loop
BEGIN
  DECLARE contador INT DEFAULT 1;
  DECLARE preco_base DECIMAL(10,2) DEFAULT 100.00;
  
  -- Loop para inserir 10 produtos
  WHILE contador <= 10 DO
    -- Calcula preço com variação baseada no contador
    INSERT INTO produtos VALUES (
      contador,
      CONCAT('Produto ', contador),
      preco_base + (contador * 10.50),
      CASE 
        WHEN contador <= 3 THEN 'Eletrônicos'
        WHEN contador <= 6 THEN 'Móveis'
        ELSE 'Acessórios'
      END,
      50 + (contador * 5)
    );
    
    SET contador = contador + 1;
  END WHILE;
  
  -- Mostrar quantidade de produtos inseridos
  SELECT COUNT(*) as total_produtos FROM produtos;
END;

-- COMMAND ----------

select * from produtos

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2 Atualização Condicional de Preços
-- MAGIC
-- MAGIC Exemplo de como atualizar preços baseado em condições.

-- COMMAND ----------

-- DBTITLE 1,Atualizar Preços com Condições
BEGIN
  -- Declarar variáveis para ajuste de preços
  DECLARE aumento_eletronicos DECIMAL(5,2) DEFAULT 1.15; -- 15% de aumento
  DECLARE aumento_moveis DECIMAL(5,2) DEFAULT 1.10;      -- 10% de aumento
  DECLARE aumento_acessorios DECIMAL(5,2) DEFAULT 1.05;  -- 5% de aumento
  
  -- Atualizar preços por categoria
  UPDATE produtos 
  SET preco = 
    CASE categoria
      WHEN 'Eletrônicos' THEN preco * aumento_eletronicos
      WHEN 'Móveis' THEN preco * aumento_moveis
      WHEN 'Acessórios' THEN preco * aumento_acessorios
    END;
    
  -- Mostrar novos preços
  SELECT categoria, 
         COUNT(*) as quantidade, 
         AVG(preco) as preco_medio 
  FROM produtos 
  GROUP BY categoria;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.3 Controle de Estoque com LOOP e IF
-- MAGIC
-- MAGIC Exemplo de como atualizar estoque usando estruturas de controle.

-- COMMAND ----------

BEGIN
  DECLARE produto_atual INT DEFAULT 1;
  DECLARE estoque_minimo INT DEFAULT 70;
  DECLARE quantidade_repor INT DEFAULT 30;
  DECLARE estoque_atual INT;
  -- Mostrar produtos que foram atualizados
  WITH movimentacao_estoque AS (
    SELECT 
      id as produto_id,
      nome,
      estoque as quantidade_atual,
      CASE 
        WHEN estoque >= estoque_minimo THEN 'OK'
        ELSE 'Baixo'
      END as status_estoque
    FROM produtos
  )
  SELECT * FROM movimentacao_estoque
  ORDER BY produto_id;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Agora vamos realizar um loop e atualizar os registros com estoque abaixo do valor definido na variavel estoque_minimo;

-- COMMAND ----------

-- DBTITLE 1,Atualizar Estoque
BEGIN
  DECLARE produto_atual INT DEFAULT 1;
  DECLARE estoque_minimo INT DEFAULT 70;
  DECLARE quantidade_repor INT DEFAULT 30;
  DECLARE estoque_atual INT;
  
  -- Loop pelos produtos
  WHILE produto_atual <= 10 DO
    -- Obter estoque atual
    EXECUTE IMMEDIATE 'select estoque from produtos WHERE id = ?' INTO estoque_atual USING produto_atual;
    
    
    -- Verificar e atualizar estoque se necessário
    IF estoque_atual < estoque_minimo THEN
      -- Atualizar estoque
      UPDATE produtos 
      SET estoque = estoque + quantidade_repor 
      WHERE id = produto_atual;
    END IF;
    
    SET produto_atual = produto_atual + 1;
  END WHILE;
  
  -- Mostrar produtos que foram atualizados
  WITH movimentacao_estoque AS (
    SELECT 
      id as produto_id,
      nome,
      estoque as quantidade_atual,
      CASE 
        WHEN estoque >= estoque_minimo THEN 'OK'
        ELSE 'Baixo'
      END as status_estoque
    FROM produtos
  )
  SELECT * FROM movimentacao_estoque
  ORDER BY produto_id;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.4 Relatório de Produtos
-- MAGIC
-- MAGIC Exemplo de como gerar um relatório usando os dados manipulados.

-- COMMAND ----------

-- DBTITLE 1,Gerar Relatório
BEGIN
  -- Criar tabela de relatório
  CREATE OR REPLACE TABLE relatorio_produtos AS
  SELECT 
    categoria,
    COUNT(*) as total_produtos,
    ROUND(AVG(preco), 2) as preco_medio,
    SUM(estoque) as estoque_total,
    MIN(preco) as menor_preco,
    MAX(preco) as maior_preco
  FROM produtos
  GROUP BY categoria
  ORDER BY categoria;
  
  -- Mostrar relatório
  SELECT * FROM relatorio_produtos;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. Tratamento de Erros
-- MAGIC
-- MAGIC Nesta seção, vamos demonstrar como implementar tratamento de erros usando SQL Scripting.
-- MAGIC Veremos:
-- MAGIC 1. Como criar handlers para capturar erros
-- MAGIC 2. Como usar RESIGNAL para repassar erros
-- MAGIC 3. Como registrar erros em uma tabela de log
-- MAGIC
-- MAGIC ### Conceitos Importantes
-- MAGIC - `EXIT HANDLER`: Captura o erro e encerra a execução do bloco
-- MAGIC - `RESIGNAL`: Repassa o erro capturado para o próximo nível
-- MAGIC - `GET DIAGNOSTICS`: Obtém informações detalhadas sobre o erro

-- COMMAND ----------

SET spark.sql.ansi.enabled = True; -- Setando esse parametro pois por padrão dividir por zero retorna null (hive) ao inves de 0

-- COMMAND ----------

-- DBTITLE 1,Criar Tabela de Log
CREATE OR REPLACE TABLE log_erros (
  data_hora TIMESTAMP,
  operacao STRING,
  erro_codigo STRING,
  erro_mensagem STRING,
  erro_estado STRING,
  linha_numero BIGINT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Exemplo de Divisão por Zero
-- MAGIC
-- MAGIC Vamos criar um exemplo que tenta realizar uma divisão por zero e captura o erro:
-- MAGIC 1. O handler captura o erro DIVIDE_BY_ZERO
-- MAGIC 2. Registra os detalhes do erro na tabela de log
-- MAGIC 3. Repassa o erro usando RESIGNAL

-- COMMAND ----------

-- DBTITLE 1,Exemplo com Handler e RESIGNAL
BEGIN
  -- Declarar handler para divisão por zero
  DECLARE EXIT HANDLER FOR DIVIDE_BY_ZERO
  BEGIN
    DECLARE erro_codigo STRING;
    DECLARE erro_mensagem STRING;
    DECLARE erro_estado STRING;
    DECLARE linha_numero BIGINT;
    
    -- Obter informações detalhadas do erro
    GET DIAGNOSTICS CONDITION 1
      erro_codigo = CONDITION_IDENTIFIER,
      erro_mensagem = MESSAGE_TEXT,
      erro_estado = RETURNED_SQLSTATE,
      linha_numero = LINE_NUMBER;
    
    -- Registrar erro na tabela de log
    INSERT INTO log_erros VALUES (
      current_timestamp(),
      'divisao_teste',
      erro_codigo,
      erro_mensagem,
      erro_estado,
      linha_numero
    );
    
    -- Repassar o erro
    RESIGNAL;
  END;
  
  -- Tentar fazer uma divisão por zero
  SELECT 10/0 as resultado;
END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Verificar Log de Erros
-- MAGIC
-- MAGIC Vamos consultar a tabela de log para ver os detalhes do erro capturado:

-- COMMAND ----------

-- DBTITLE 1,Consultar Log de Erros
SELECT * FROM log_erros ORDER BY data_hora DESC LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 4. Limpeza
-- MAGIC
-- MAGIC Removendo todas as tabelas criadas durante a demonstração.

-- COMMAND ----------

-- DBTITLE 1,Remover Assets
DROP TABLE IF EXISTS produtos;
DROP TABLE IF EXISTS relatorio_produtos;
DROP TABLE IF EXISTS log_erros;