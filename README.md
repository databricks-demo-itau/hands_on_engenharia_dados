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

### 1. <a href="$./DBDEMOS/README.md">Demonstrações Interativas (DBDEMOS)</a>
Explore os conceitos fundamentais através de demos interativas:
- <a href="$./DBDEMOS/README_Delta_Lake.md">Delta Lake</a>
- <a href="$./DBDEMOS/README_Auto_Loader.md">Auto Loader</a>
- <a href="$./DBDEMOS/README_DLT_CDC.md">DLT CDC</a>

Para mais detalhes sobre as demos, consulte o <a href="$./DBDEMOS/README.md">README das Demonstrações</a>.

### 2. <a href="$./LABS/README.md">Laboratórios Práticos (LABS)</a>
Pratique com exercícios hands-on:
- <a href="$./LABS/README_SQL_Scripting.md">SQL Scripting</a>
- <a href="$./LABS/README_Kafka.md">Kafka Integration</a>
- <a href="$./LABS/README_RLS_CM.md">Row-level Security e Column Masking</a>
- <a href="$./LABS/README_Glue.md">Glue Catalog Federation</a>

Para mais detalhes sobre os laboratórios, consulte o <a href="$./LABS/README.md">README dos Laboratórios</a>.

## Configuração Inicial

1. Abra o notebook <a href="$./001_Instala_dbdemos_engenharia.ipynb">001_Instala_dbdemos_engenharia.ipynb</a>
2. Execute as células em sequência para:
   - Configurar seu nome de usuário
   - Instalar a biblioteca dbdemos
   - Instalar as demos necessárias

## Estrutura do Repositório

```
.
├── DBDEMOS/
│   ├── delta-lake/           # Notebooks do Delta Lake
│   ├── dlt-cdc/             # Notebooks do DLT com CDC
│   ├── auto-loader/         # Notebooks do Auto Loader
│   └── _resources/          # Recursos compartilhados
│
└── LABS/
    ├── SQL_Scripiting/      # Labs de SQL avançado
    ├── Kafka/               # Labs de integração Kafka
    ├── Row_level_security_Column_Masking/  # Labs de segurança
    └── Glue_catalog_federation/  # Labs de federação Glue
```

## Boas Práticas

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

Para mais detalhes sobre cada módulo, consulte os READMEs específicos nas pastas DBDEMOS e LABS.


