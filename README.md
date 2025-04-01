# Hands-on Engenharia de Dados com Databricks

Este repositório contém uma série de demonstrações práticas das principais funcionalidades do Databricks, com foco em Engenharia de Dados. O objetivo é fornecer uma experiência hands-on com diferentes aspectos da plataforma Databricks e suas integrações.

## Demonstrações Principais

### 1. Delta Lake
- ACID transactions em Data Lakes
- Operações DELETE/UPDATE/MERGE
- Unificação de processamento batch e streaming
- Time travel e versionamento de dados
- Clonagem Zero-Copy
- Particionamento otimizado
- Change Data Flow (CDF)
- Delta Lake 3.0: Liquid Clustering e Universal Format

### 2. Auto Loader
- Ingestão automática de dados
- Schema inference e evolution
- Processamento incremental
- Suporte a múltiplos formatos de arquivo
- Integração com cloud storage

### 3. CDC com Delta Live Tables
- Implementação de Change Data Capture
- Pipelines declarativos
- Qualidade de dados integrada
- Monitoramento em tempo real
- Orquestração automatizada

## Cenários Bônus

### 1. Federação com AWS Glue Catalog
- Integração entre Unity Catalog e AWS Glue
- Gerenciamento centralizado de metadados
- Descoberta de dados federada
- Governança unificada

### 2. Segurança e Governança
- Row Level Security (RLS)
- Mascaramento de Colunas
- Políticas de acesso granular
- Auditoria e compliance

### 3. CI/CD e Testes
- Integração contínua com notebooks
- Testes unitários automatizados
- Deployment automatizado
- Boas práticas de desenvolvimento

## Pré-requisitos

- Acesso a um workspace Databricks
- Permissões adequadas no Unity Catalog
- Conhecimento básico de SQL e Python
- Familiaridade com conceitos de Data Engineering

## Estrutura do Repositório

```
.
├── delta_lake/              # Demos de Delta Lake
├── auto_loader/            # Demos de Auto Loader
├── cdc_pipeline/           # Demos de CDC com Delta Live Tables
├── BONUS_glue/            # Demo de integração com AWS Glue
├── BONUS_security/        # Demo de RLS e Column Masking
└── BONUS_cicd/           # Demo de CI/CD e testes
```

## Como Usar

1. Clone este repositório
2. Configure seu ambiente Databricks seguindo as instruções em cada diretório
3. Execute os notebooks em sequência dentro de cada diretório
4. Consulte a documentação específica em cada demo para mais detalhes

## Documentação Oficial

- [Delta Lake](https://docs.databricks.com/delta/index.html)
- [Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Delta Live Tables](https://docs.databricks.com/workflows/delta-live-tables/index.html)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [AWS Glue Federation](https://docs.databricks.com/aws/data-governance/unity-catalog/hms-federation/hms-federation-glue.html)
- [Row Level Security e Column Masking](https://docs.databricks.com/aws/tables/row-and-column-filters.html)
- [Testing Notebooks](https://docs.databricks.com/aws/notebooks/testing.html)

## Contribuições

Sinta-se à vontade para contribuir com melhorias, correções ou novas demonstrações através de pull requests.

## Licença

Este projeto está licenciado sob a licença MIT - veja o arquivo LICENSE para mais detalhes. 