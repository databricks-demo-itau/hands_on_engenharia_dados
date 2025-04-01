# Demonstração de Segurança em Nível de Linha e Máscara de Colunas no Databricks

Este diretório contém notebooks que demonstram funcionalidades avançadas de segurança no Databricks, especificamente:

1. **Segurança em Nível de Linha (Row Level Security - RLS)**
   - Permite controlar quais linhas de dados cada usuário pode visualizar
   - Útil para cenários onde diferentes departamentos só devem ver seus próprios dados
   - Implementado através de políticas de segurança

2. **Máscara de Colunas (Column Masking)**
   - Permite mascarar dados sensíveis em colunas específicas
   - Diferentes usuários podem ver diferentes níveis de detalhamento dos dados
   - Ideal para proteger informações sensíveis como CPF, salários, etc.

## Notebooks Disponíveis

1. `README.md` (este arquivo): Explicação geral do conteúdo
2. `rls_column_masking_demo.sql`: Demonstração prática das funcionalidades

## Pré-requisitos

- Acesso a um workspace do Databricks
- Permissões para criar tabelas e políticas de segurança
- Unity Catalog habilitado no workspace

## Como Usar

1. Abra o notebook `rls_column_masking_demo.sql`
2. Execute as células em sequência
3. Observe como diferentes usuários têm acesso a diferentes partes dos dados
4. Ao final, todos os assets criados serão automaticamente removidos

## Documentação Oficial

- [Row Level Security](https://learn.microsoft.com/en-us/azure/databricks/security/access-control/table-acls/row-level-security)
- [Column Masking](https://learn.microsoft.com/en-us/azure/databricks/security/access-control/table-acls/column-masking)
- [Unity Catalog](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/overview) 