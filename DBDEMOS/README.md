# Hands-on Engenharia de Dados - Databricks

Bem-vindo ao hands-on de Engenharia de Dados no Databricks! Este repositório contém uma série de demonstrações práticas para aprender conceitos fundamentais de engenharia de dados usando a plataforma Databricks.

## Instalação do dbdemos

Antes de começar, execute o notebook `001_Instala_dbdemos_engenharia.ipynb` para instalar o dbdemos e seus assets necessários.

## Roteiro de Aprendizado

Recomendamos seguir a ordem abaixo para melhor compreensão dos conceitos:

### 1. [Delta Lake](README_Delta_Lake.md)
- Fundamentos do Delta Lake
- Operações CRUD
- Performance e otimizações
- Change Data Feed
- Recursos avançados

### 3. [Auto Loader](README_Auto_Loader.md)
- Ingestão automática de dados
- Evolução de schema
- Monitoramento e otimização

### 2. [Delta Live Tables com CDC](README_DLT_CDC.md)
- Pipelines de dados em tempo real
- Change Data Capture
- Monitoramento e otimização
- Implementações em SQL e Python


## Estrutura do Repositório

```
DBDEMOS/
├── delta-lake/           # Notebooks do Delta Lake
├── dlt-cdc/             # Notebooks do DLT com CDC
├── auto-loader/         # Notebooks do Auto Loader
└── _resources/          # Recursos compartilhados
```

## Observações Importantes

- Todos os notebooks contêm instruções detalhadas em ingles
- Siga a ordem recomendada dos exercícios
- Leia atentamente os comentários e documentações
- Limpe os recursos após concluir cada exercício
- Em caso de dúvidas, consulte a documentação referenciada nos notebooks

## Pré-requisitos

- Acesso a um workspace Databricks
- Permissões necessárias para criar e executar notebooks
- Conhecimento básico de SQL e Python
- dbdemos instalado através do notebook de instalação

## Suporte

Em caso de dúvidas ou problemas:
- Consulte os comentários nos notebooks
- Verifique a documentação oficial do Databricks
- Entre em contato com o instrutor do hands-on 