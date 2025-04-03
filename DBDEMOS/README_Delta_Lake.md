# Hands-on: Delta Lake

Este guia apresenta uma introdução prática ao Delta Lake, uma tecnologia fundamental para o lakehouse da Databricks.

## Pré-requisitos
- Acesso ao ambiente Databricks
- dbdemos instalado (execute o notebook `001_Instala_dbdemos_engenharia.ipynb` caso ainda não tenha instalado)

## Roteiro de Execução

### 1. Introdução ao Delta Lake
**Notebook**: `00-Delta-Lake-Introduction.sql`
- Visão geral do Delta Lake
- Conceitos fundamentais
- Benefícios e casos de uso

### 2. Primeiros Passos com Delta Lake
**Notebook**: `01-Getting-Started-With-Delta-Lake.sql`
- Criação de tabelas Delta
- Operações básicas (INSERT, UPDATE, DELETE)
- Versionamento e Time Travel
- Gerenciamento de schema

### 3. Performance no Delta Lake
**Notebook**: `02-Delta-Lake-Performance.sql`
- Otimizações de performance
- Compaction e Z-Ordering
- Boas práticas de otimização

### 4. Delta Lake Uniform
**Notebook**: `03-Delta-Lake-Uniform.py`
- Entendendo o Delta Lake Uniform
- Otimizações de layout de arquivos
- Melhorias de performance em consultas

### 5. Change Data Feed (CDF)
**Notebook**: `04-Delta-Lake-CDF.sql`
- Captura de mudanças em tabelas Delta
- Implementação de CDC (Change Data Capture)
- Casos de uso e boas práticas

### 6. Recursos Avançados
**Notebook**: `05-Advanced-Delta-Lake-Internal.sql`
- Funcionalidades avançadas
- Internals do Delta Lake
- Dicas e truques avançados

## Observações Importantes
- Execute os notebooks na ordem apresentada
- Cada notebook contém instruções detalhadas em português
- Não se esqueça de limpar os recursos após a conclusão dos exercícios
- Em caso de dúvidas, consulte os comentários e documentações referenciadas nos notebooks 