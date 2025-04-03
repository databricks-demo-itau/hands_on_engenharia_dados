# Hands-on: Federação de Catálogo AWS Glue no Unity Catalog

Este guia demonstra como utilizar a funcionalidade de federação de catálogo do AWS Glue no Unity Catalog do Databricks, permitindo acesso direto a tabelas do AWS Glue sem necessidade de ETL ou cópia de dados.

## Pré-requisitos
- Acesso ao ambiente Databricks
- Conhecimento básico de SQL
- Unity Catalog configurado
- Catálogo federado `dev_glue` configurado
- Permissões para acessar catálogos externos

## Roteiro de Execução

### 1. Federação de Catálogo AWS Glue
**Notebook**: <a href="$./Glue_catalog_federation/glue_catalog_federation">glue_catalog_federation</a>
- Conexão com catálogo AWS Glue
- Exploração de tabelas federadas
- Clonagem de tabelas para o formato Delta
- Uso de recursos do Databricks com tabelas federadas

## Tópicos Abordados
- Conceitos fundamentais de federação de catálogo
- Acesso a dados remotos sem ETL
- Clonagem de tabelas para o Unity Catalog
- Integração com recursos avançados do Databricks
- Configuração e gerenciamento de conexões
- Vantagens da federação de catálogos

## Recursos Adicionais
- [Documentação sobre HMS Federation](https://docs.databricks.com/aws/en/data-governance/unity-catalog/hms-federation/)
- [Federação de Catálogo do AWS Glue](https://docs.databricks.com/aws/en/data-governance/unity-catalog/hms-federation/hms-federation-glue)
- [Query Federation](https://docs.databricks.com/aws/en/query-federation/)
- [Clonagem de Tabelas Parquet](https://learn.microsoft.com/en-us/azure/databricks/ingestion/data-migration/clone-parquet)

## Observações Importantes
- Execute o notebook na ordem das células apresentadas
- Certifique-se de ter as permissões corretas no Unity Catalog
- As tabelas federadas são apenas para leitura (read-only)
- Limpe os recursos criados após concluir os exercícios
- A federação permite acesso a dados sem movê-los
- Verifique as limitações de recursos com tabelas federadas
- A integração com Genie e outros recursos avançados pode ser explorada

Para retornar ao índice dos laboratórios, clique <a href="$../README.md">aqui</a>. 