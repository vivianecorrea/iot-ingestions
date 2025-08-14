# Assignment 2 – Monitoramento de Sensores IoT no Databricks com Confluent Cloud Kafka

> **Contexto**: Este subprojeto está localizado em `assignment_2/` no repositório. Ele implementa um pipeline de ingestão e processamento em tempo real para eventos simulados de sensores IoT usando **Confluent Cloud (Kafka gerenciado)** + **Databricks** (Community ou Enterprise) + **Delta/Unity Catalog** seguindo um layout **medalhão** (bronze → prata → ouro).

---

## 1) Visão Geral

* **Objetivo**: Ingerir eventos de IoT em streaming (temperatura, umidade, localização, metadados do dispositivo), processá-los com Spark Structured Streaming no Databricks e armazenar os dados curados em tabelas Delta.
* **Por que este design**: Streaming escalável e tolerante a falhas com semântica exatamente uma vez no Delta; simples de operar no Databricks; compatível com Kafka gerenciado (Confluent Cloud).

## 3) Pré-requisitos

* Cluster no **Confluent Cloud** (Básico ou superior), **um tópico** (ex.: `iot.sensors.v1`) e **Chave/Segredo da API**.
* Workspace do **Databricks** + cluster com **Spark 3.5+** (ou runtime 13.3+ LTS), DBR suporta conector Kafka.
* **Unity Catalog** habilitado (recomendado) ou um caminho no DBFS para tabelas Delta.
* Um **Produtor** local/remoto publicando eventos JSON no tópico (exemplos abaixo).

## 4) Configuração segura

Crie um **escopo de segredo** no Databricks e armazene as credenciais do Confluent:

1. No Databricks (UI do Workspace) → *Compute* → confirme que o cluster está em execução.
2. Use a interface **Secrets** ou CLI para criar o escopo `confluent-secrets`.
3. Adicione as chaves:

    * `confluent.bootstrap.servers = <SEU_HOST_DE_BOOTSTRAP:PORTA>` (ex.: `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`)
    * `confluent.api.key = <SUA_CHAVE_API>`
    * `confluent.api.secret = <SEU_SEGREDO_API>`

Você fará referência aos segredos nos notebooks via `dbutils.secrets.get("confluent-secrets","<key>")`.
