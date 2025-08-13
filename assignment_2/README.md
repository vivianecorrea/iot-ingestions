# Assignment 2 – IoT Sensor Monitoring on Databricks with Confluent Cloud Kafka

> **Context**: This subproject lives at `assignment_2/` in the repository. It implements a real‑time ingestion and processing pipeline for simulated IoT sensor events using **Confluent Cloud (managed Kafka)** + **Databricks** (Community or Enterprise) + **Delta/Unity Catalog** following a **medallion** layout (bronze → silver → gold).

---

## 1) Overview

* **Goal**: Ingest streaming IoT events (temperature, humidity, geo, device metadata), process them with Spark Structured Streaming on Databricks, and store curated data in Delta tables.
* **Why this design**: Scalable, fault‑tolerant streaming with exactly‑once semantics into Delta; simple to operate on Databricks; compatible with managed Kafka (Confluent Cloud).


## 3) Prerequisites

* **Confluent Cloud** cluster (Basic or above), **one topic** (e.g. `iot.sensors.v1`) and **API Key/Secret**.
* **Databricks** workspace + cluster with **Spark 3.5+** (or runtime 13.3+ LTS), DBR supports Kafka connector.
* **Unity Catalog** enabled (recommended) or a DBFS path for Delta tables.
*  A local/remote **Producer** publishing JSON events to the topic (examples below).


## 4) Secure configuration

Create a Databricks **secret scope** and store Confluent credentials:

1. In Databricks (Workspace UI) → *Compute* → confirm cluster running.
2. Use the **Secrets** UI or CLI to create scope `confluent-secrets`.
3. Add keys:

   * `confluent.bootstrap.servers = <YOUR_BOOTSTRAP_HOST:PORT>` (e.g. `pkc-xxxxx.us-east-1.aws.confluent.cloud:9092`)
   * `confluent.api.key = <YOUR_API_KEY>`
   * `confluent.api.secret = <YOUR_API_SECRET>`

You will reference secrets in notebooks via `dbutils.secrets.get("confluent-secrets","<key>")`.
