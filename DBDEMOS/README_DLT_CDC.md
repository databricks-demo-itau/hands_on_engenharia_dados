# Hands-on: Delta Live Tables (DLT) com Change Data Capture (CDC)

Este guia apresenta uma demonstração prática de como implementar pipelines de dados em tempo real usando Delta Live Tables com Change Data Capture.

## Pré-requisitos
- Acesso ao ambiente Databricks
- dbdemos instalado (execute o notebook `001_Instala_dbdemos_engenharia.ipynb` caso ainda não tenha instalado)
- Conhecimento básico de Delta Lake (recomendado completar o hands-on de Delta Lake primeiro)

## Roteiro de Execução

### 1. DLT com CDC usando SQL
**Notebook**: `01-Retail_DLT_CDC_SQL.sql`
- Introdução ao Delta Live Tables
- Implementação de CDC usando SQL
- Criação de pipeline de dados incremental
- Transformações em tempo real

### 2. DLT com CDC usando Python
**Notebook**: `02-Retail_DLT_CDC_Python.py`
- Implementação de CDC usando Python
- Desenvolvimento de pipelines DLT
- Boas práticas de desenvolvimento
- Tratamento de dados em tempo real

### 3. Monitoramento de Pipelines DLT
**Notebook**: `03-Retail_DLT_CDC_Monitoring.py`
- Monitoramento de pipelines em tempo real
- Métricas e indicadores importantes
- Troubleshooting e otimização
- Dashboards de monitoramento

### 4. Pipeline DLT Completo
**Notebook**: `04-Retail_DLT_CDC_Full.py`
- Implementação de um pipeline end-to-end
- Integração com outras ferramentas
- Casos de uso avançados
- Melhores práticas de produção

## Recursos Adicionais
- Consulte a pasta `_dashboards` para exemplos de visualizações
- A pasta `_resources` contém dados e arquivos auxiliares para os exercícios

## Observações Importantes
- Execute os notebooks na ordem apresentada
- Cada notebook contém instruções detalhadas em português
- Preste atenção aos comentários sobre configuração de pipeline
- Limpe os recursos e pipelines após a conclusão dos exercícios
- Em caso de dúvidas, consulte a documentação referenciada nos notebooks 