# Como Criar DLT Pipeline no Databricks

Este guia demonstra o passo a passo para criar o DLT relacionado ao exemplo EXTRA de ingestão com Kafka.   
(Guia com imagens)

## Passos para Criação

1. Clique em Pipelines
2. Clique em "Create Pipeline"
3. Clique em "ETL Pipeline"
4. Coloque um nome, exemplo: demo_notebook_04
5. Selecione opção Advanced
6. Em Pipeline Mode, escolha a opção Triggered
6. Na categoria Source Code, click no icone de pasta para escolher o notebook
7. Navegue até o notebook, que está em:  
/Workspace/Users/{seu_usuario}/hands_on_engenharia_dados/Extras/Kafka/001_read_kafka
8. Em Destination, selecione Unity Catalog
9. Escolha o catalogo "dev_hands_on"
10. Selecione o schema com o nome do seu usuario
11. Em Cluster Policy, escolha a "dlt_policy"
12. Em Cluster Mode, escolha "Fixed Size"
13. Em Workers, altera de 5 para 1
14. Em Instance Profile, escolha "KafkaReadWriteInstanceProfile"
15. Em Advanced, Click em "Add Configuration"
16. Adicione as duas configuraçöes
  - bootstrapservers `b-1.itaudevkafka01.nohjsv.c21.kafka.us-east-1.amazonaws.com:9098,b-2.itaudevkafka01.nohjsv.c21.kafka.us-east-1.amazonaws.com:9098b-1.itaudevkafka01.nohjsv.c21.kafka.us-east-1.amazonaws.com:9098,b-2.itaudevkafka01.nohjsv.c21.kafka.us-east-1.amazonaws.com:9098`
  - topic `dev_clickstream`
17. Em Worker Type, escolha `r6id.xlarge`
18. Em Driver Type, escolha `r6id.xlarge` ou `Same as Worker`
19. Click em Create
20. Click em Start

## Demonstração Visual

![Processo de criação de Git Folder no Databricks](images/criar_dlt_kafka.png)