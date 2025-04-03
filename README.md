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

### 1. [Demonstrações Interativas (DBDEMOS)](DBDEMOS/README.md)
- [Delta Lake](DBDEMOS/README_Delta_Lake.md)
- [Auto Loader](DBDEMOS/README_Auto_Loader.md)
- [DLT CDC](DBDEMOS/README_DLT_CDC.md)

### 2. [Laboratórios Práticos (LABS)](LABS/README.md)
- [SQL Scripting](LABS/README_SQL_Scripting.md)
- [Kafka Integration](LABS/README_Kafka.md)
- [Row-level Security e Column Masking](LABS/README_RLS_CM.md)
- [Glue Catalog Federation](LABS/README_Glue.md)

## Configuração Inicial

1. Abra o notebook `001_Instala_dbdemos_engenharia.ipynb`
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

Para mais detalhes sobre cada módulo, consulte os READMEs específicos nas pastas DBDEMOS e LABS.


