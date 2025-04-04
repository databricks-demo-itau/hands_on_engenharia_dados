# Hands-on Engenharia de Dados - Databricks

Este repositório contém o material para o hands-on de Engenharia de Dados no Databricks. Durante esta sessão, você irá explorar conceitos fundamentais e práticos de engenharia de dados usando a plataforma Databricks.

## Pré-requisitos

- Acesso a um workspace Databricks
- Permissões para:
  - Criar clusters
  - Criar pipelines DLT
  - Criar dashboards e queries no DBSQL
  - Acesso ao Unity Catalog
  - Acesso a catálogos externos (AWS Glue)

## Configuração Inicial


### Eu estou acessando esse documento pelo GITHUB:
1. [Passo a passo de como criar Git Folder no Databricks](./Guias_UI/git_folder.md).

### Após clonar, siga pelo DATABRICKS:
2. <a href="$./Guias_UI/cluster.md">Criar um Cluster</a> para executar os notebooks
3. Abra o notebook <a href="$./DBDEMOS/001_Instala_dbdemos_engenharia">001_Instala_dbdemos_engenharia</a>
4. Execute as células em sequência para:
   - Configurar seu nome de usuário
   - Instalar a biblioteca dbdemos
   - Instalar as demos necessárias

## Estrutura do Hands-on

O hands-on está dividido em duas partes principais:

### 1. <a href="$./DBDEMOS/README.md">Demonstrações Interativas (DBDEMOS)</a>
Explore os conceitos fundamentais através de demos interativas:
- <a href="$./DBDEMOS/README_Delta_Lake.md">Delta Lake</a>
- <a href="$./DBDEMOS/README_Auto_Loader.md">Auto Loader</a>
- <a href="$./DBDEMOS/README_DLT_CDC.md">DLT CDC</a>

Para mais detalhes sobre as demos, consulte o <a href="$./DBDEMOS/README.md">README das Demonstrações</a>.

### 2. <a href="$./Extras/README.md">Laboratórios Práticos (Extras)</a>
Pratique com exercícios hands-on:
- <a href="$./Extras/README_SQL_Scripting.md">SQL Scripting</a>
- <a href="$./Extras/README_Kafka.md">Kafka Integration</a>
- <a href="$./Extras/README_RLS_CM.md">Row-level Security e Column Masking</a>
- <a href="$./Extras/README_Glue.md">Glue Catalog Federation</a>
- <a href="$./Extras/README_CICD.md">DABS + CICD + Unit Test + Databricks Connect + Github Actions</a>

Para mais detalhes sobre os laboratórios, consulte o <a href="$./Extras/README.md">README dos Laboratórios</a>.

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

Para mais detalhes sobre cada módulo, consulte os READMEs específicos nas pastas DBDEMOS e Extras.


