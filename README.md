# Hands-on Engenharia de Dados - Databricks

Este repositório contém o material para o hands-on de Engenharia de Dados no Databricks. Durante esta sessão, você irá explorar conceitos fundamentais e práticos de engenharia de dados usando a plataforma Databricks.

## Pré-requisitos

- Acesso a um workspace Databricks
- Permissões para:
  - Criar clusters
  - Criar pipelines DLT
  - Criar dashboards e queries no DBSQL
  - Acesso ao Unity Catalog

## Estrutura do Hands-on

O hands-on está dividido em duas partes principais:

1. **Demonstrações Interativas usando DBDemos**
   - Delta Lake: Fundamentos e recursos avançados
   - Auto Loader: Ingestão de dados em tempo real
   - DLT CDC: Change Data Capture com Delta Live Tables

2. **Laboratórios Práticos**
   - SQL Scripting: Práticas avançadas de SQL no Databricks
   - Kafka: Integração com Apache Kafka
   - Row-level Security e Column Masking: Segurança em nível de linha e mascaramento de coluna
   - Glue Catalog Federation: Federação com AWS Glue Catalog

## Roteiro de Execução

### Parte 1: Configuração Inicial

1. Abra o notebook `001_Instala_dbdemos_engenharia.ipynb`
2. Execute as células em sequência para:
   - Configurar seu nome de usuário
   - Instalar a biblioteca dbdemos
   - Instalar as demos necessárias

### Parte 2: Demonstrações DBDemos

#### Demo 1: Delta Lake
- Navegue até a pasta da demo Delta Lake instalada
- Execute os notebooks em ordem numérica
- Principais conceitos abordados:
  - ACID Transactions
  - Time Travel
  - Schema Evolution
  - Optimization e Z-Ordering

#### Demo 2: Auto Loader
- Navegue até a pasta da demo Auto Loader
- Execute os notebooks em ordem numérica
- Principais conceitos abordados:
  - Ingestão incremental de dados
  - Schema inference e evolution
  - Monitoramento e tratamento de erros

#### Demo 3: DLT CDC
- Navegue até a pasta da demo DLT CDC
- Execute os notebooks em ordem numérica
- Principais conceitos abordados:
  - Change Data Capture
  - Delta Live Tables
  - Qualidade de dados
  - Pipelines de dados em tempo real

### Parte 3: Laboratórios Práticos

#### Lab 1: SQL Scripting
- Navegue até `LABS/SQL_Scripiting/`
- Siga as instruções do notebook principal
- Execute os exercícios propostos

#### Lab 2: Kafka Integration
- Navegue até `LABS/Kafka/`
- Configure a conexão com o Kafka
- Execute os exercícios de integração

#### Lab 3: Row-level Security e Column Masking
- Navegue até `LABS/Row_level_security_Column_Masking/`
- Implemente controles de acesso em nível de linha
- Configure mascaramento de colunas sensíveis

#### Lab 4: Glue Catalog Federation
- Navegue até `LABS/Glue_catalog_federation/`
- Configure a federação com AWS Glue
- Execute as queries de exemplo

## Boas Práticas

- Siga as diretrizes estabelecidas no arquivo de regras do Cursor
- Mantenha o catálogo e schema organizados conforme as instruções
- Limpe os recursos criados após cada laboratório
- Documente suas observações e aprendizados

## Suporte

Em caso de dúvidas durante os laboratórios:
1. Consulte a documentação oficial do Databricks
2. Verifique os exemplos fornecidos em cada notebook
3. Peça ajuda ao instrutor

## Limpeza

Ao finalizar os laboratórios:
1. Delete todos os assets criados (exceto catálogo e schema)
2. Pare todos os clusters em execução
3. Termine todas as pipelines DLT

## Próximos Passos

- Explore a documentação oficial do Databricks
- Experimente outros demos disponíveis via dbdemos
- Aplique os conceitos aprendidos em seus próprios projetos 