# Assignment 2 – Monitoramento de Sensores IoT no Databricks com Confluent Cloud Kafka

> **Contexto**: Este subprojeto implementa um pipeline de ingestão e processamento em tempo real para eventos simulados de sensores IoT usando **Confluent Cloud (Kafka gerenciado)** + **Databricks** (Community ou Enterprise) + **Delta/Unity Catalog** seguindo um layout **medalhão** (landing → bronze → prata ).

### Configuração Databricks
Para este projeto optei por usar um kafka gerenciado na confluent cloud
Após a criação, guarde suas credenciais no databricks em secret scopes

Como armazenamento optei por usar a solução Unity Catalog e para isso criei o database first_sandbox_challenge e o schema iot_sensors após fazer esse processo executei o notebook producer assignment_2/iot_producer/dbricks/iot_producer_job.ipynb para publicar as mensagens mockadas no topico Kafka

Após publicadas as mensagens o notebook ingestions faz o papel de consumer da solução, evoluindo os dados entre as camadas da arquitetura medalion

Para garan
