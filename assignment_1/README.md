
# Assignment 1 - Diário de Bordo

Este projeto implementa o **Assignment 1 – Diário de Bordo**,  utilizando a arquitetura Medalion Bronze → Silver → Gold em **PySpark**, com execução via **notebooks Databricks**. Cada camada é acionada como **task individual**, promovendo modularidade, isolamento e facilidade de manutenção.

* **Bronze**: ingestão contínua via Auto Loader → tabela Delta bronze (`rides_bronze`)
* **Silver**: parsing de datas, cast, validações → tabela Delta silver (`rides_silver`)
* **Gold**: agregações diárias com métricas específicas → tabela Delta gold (`rides_daily_info_gold`)

## Principais Componentes e Decisões de implementação

* **Modularização com classes Python** (`src/stages.py`): cada estágio (BronzeToSilver, SilverToGold) é um `DataFrame -> DataFrame` puramente funcional facilitando o reuso.

* **Constantes e utilitários centralizados** (`src/constants.py` e `src/schemas.py` ): esquema, padrões de data, paths, normalização de strings, mantendo consistência e fácil manutenção.


## Estrutura do Repositório

```
assignment_1/
├── notebooks/
│   ├── bronze.ipynb
│   ├── silver.ipynb
│   └── gold.ipynb
├── src/
│   ├── stages.py
│   ├── constants.py
|   └── schemas.py
├── README.md
└── requirements.txt
```

![ETL Job Success](img/1-ETL-job.png)
*Figura 1: Imagem ilustrativa de execução bem-sucedida do job ETL no Databricks.*

## 🚀 Setup no Databricks

1. No **Databricks**, suba o diretório `assignment_1/`, configure os notebooks como tasks de um job.

2. Para simular a chegada do dado na landing, suba o csv para o local `/Volumes/sandbox_first_catalog/transports/landing-zone/`no volume DBFS, dentro do catalogo `sandbox_first_catalog` e do schema `transports`

2. Execute sequencialmente: **01\_bronze → 02\_silver → 03\_gold** através de trigger manual

3. Visualiza as tabelas no catálogo conforme tabela abaixo.

## ⚙️ Camadas do Pipeline

| Camada     | Descrição                          | Catálogo                                   |
| ---------- | ---------------------------------- | ----------------------------------------- |
| **Bronze** | Ingestão raw CSV com Auto Loader   | `sandbox_first_catalog.transports.rides_bronze` |
| **Silver** | Parsing, validações, transformação | `sandbox_first_catalog.transports.rides_silver` |
| **Gold**   | Agregações diárias e métricas      | `sandbox_first_catalog.transports.rides_daily_info_gold` |


## 📊 Colunas de Saída (`rides_daily_info_gold`)

| Coluna                   | Descrição                                                 |
| ------------------------ | --------------------------------------------------------- |
| **DT\_REFE**             | Data de referência (`yyyy-MM-dd`)                         |
| **QT\_CORR**             | Quantidade total de corridas                              |
| **QT\_CORR\_NEG**        | Corridas com categoria "Negócio"                          |
| **QT\_CORR\_PESS**       | Corridas com categoria "Pessoal"                          |
| **VL\_MAX\_DIST**        | Maior distância percorrida                                |
| **VL\_MIN\_DIST**        | Menor distância percorrida                                |
| **VL\_AVG\_DIST**        | Média das distâncias                                      |
| **QT\_CORR\_REUNI**      | Corridas com propósito "Reunião"                          |
| **QT\_CORR\_NAO\_REUNI** | Corridas com propósito declarado e diferente de "Reunião" |

## Anexos

**DataFrame Final Disponível no Unity Catalog:**
![DataFrame Final](img/2-dataset-final.png)
