# Assignment 2 – Monitoramento de Sensores IoT no Databricks com Confluent Cloud Kafka

> **Contexto**: Este subprojeto implementa um pipeline de ingestão e processamento em tempo real para eventos simulados de sensores IoT usando **Confluent Cloud (Kafka gerenciado)** + **Databricks** (Community ou Enterprise) + **Delta/Unity Catalog** seguindo um layout **medalhão** (landing → bronze → prata ).

Foi desenvolvido no Databricks, utilizando PySpark Structured Streaming e um serviço gerenciado do Kafka (Confluent Cloud) para mensageria.

###?Arquitetura da Solução

A solução segue uma arquitetura de pipelines em camadas (Medallion Architecture) para garantir processamento em larga escala, resiliência e qualidade dos dados:
	1.	Landing → Bronze
	•	Recebe dados brutos em tempo real de um tópico Kafka (iot_sensors_landing).
	•	Dados enviados por um Producer em Python que simula sensores usando a biblioteca Faker (temperatura, umidade, localização, etc.).
	2.	Bronze → Silver
	•	Transformações em PySpark para normalizar tipos, tratar campos nulos e converter timestamps.
	3.	Persistência
    	•	Dados processados armazenados em tabelas Unity Catalog no Databricks.

### Decisões de Implementação
	•	Databricks + Kafka (Confluent Cloud): Atende ao requisito de usar uma solução de streaming real, simulando um cenário de Big Data.
	•	PySpark Structured Streaming: Garante escalabilidade e processamento distribuído.
	•	Unity Catalog: Utilizado como “banco de dados” no contexto Databricks, garantindo persistência, versionamento e controle de acesso.
	•	Medallion Architecture: Organização em camadas (Bronze, Silver, Gold) para isolamento de estágios e facilidade de manutenção.
	•	Resiliência a falhas: Uso de checkpoints no streaming para permitir retomada segura em caso de interrupção.
	
### Execução
	1.	Producer
	•	Executar o script Python do producer para enviar dados simulados ao tópico Kafka.
	2.	Consumer no Databricks
	•	Rodar o notebook principal, seguindo a ordem das células que representam as camadas de processamento.
	3.	Consulta aos Dados
	•	As tabelas resultantes no Unity Catalog podem ser consultadas via Spark SQL ou integradas a ferramentas de BI.

### Melhorias Possíveis
	•	Separar cada camada em um notebook independente, semelhante ao que foi feito no Assignment 1, permitindo:
	•	Maior isolamento de estágios de processamento.
	•	Melhor organização para execução como jobs independentes.
	•	Facilidade de manutenção e evolução futura
