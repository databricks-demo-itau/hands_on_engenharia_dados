# Hands-on: Integração com Apache Kafka

Este guia apresenta diferentes abordagens para integrar o Databricks com Apache Kafka, incluindo implementações em SQL e PySpark.

## Pré-requisitos
- Acesso ao ambiente Databricks
- Conhecimento básico de streaming de dados
- Familiaridade com Apache Kafka
- Cluster configurado com suporte a Kafka

## Roteiro de Execução

### 1. Leitura de Dados do Kafka com SQL
**Notebook**: `001_read_kafka.sql`
- Configuração básica de conexão
- Leitura de tópicos Kafka usando SQL
- Transformações em tempo real
- Boas práticas de consumo de dados

### 2. Streaming Estruturado com PySpark
**Notebook**: `001_read_kafka_structured_streaming.py`
- Structured Streaming com Kafka
- Processamento em tempo real
- Gerenciamento de checkpoints
- Tratamento de erros e recuperação

### 3. Integração Avançada com PySpark
**Notebook**: `001_read_kafka_pyspark.py`
- Implementação com PySpark
- Transformações complexas
- Otimização de performance
- Monitoramento e debugging

## Recursos Adicionais
- Documentação do Kafka Connector
- Exemplos de configurações avançadas
- Dicas de troubleshooting

## Observações Importantes
- Certifique-se de ter acesso ao cluster Kafka
- Configure corretamente as credenciais e endpoints
- Monitore o consumo de recursos durante o streaming
- Implemente mecanismos de recuperação de falhas
- Limpe os recursos e streams após os exercícios 