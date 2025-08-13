from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    avg,
    col,
    count,
    date_format,
    max,
    min,
    round,
    sum,
    to_timestamp,
)
from pyspark.sql.types import StringType, StructField, StructType

CATALOG = "sandbox_first_catalog"
SCHEMA = "transports"

LANDING_PATH = "/Volumes/sandbox_first_catalog/transports/landing-zone/"
CHECKPOINT_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/_checkpoints"

TBL_BRONZE = f"{CATALOG}.{SCHEMA}.rides_bronze"
TBL_SILVER = f"{CATALOG}.{SCHEMA}.rides_silver"
TBL_GOLD = f"{CATALOG}.{SCHEMA}.rides_daily_info_gold"


class SourceCollumns:
    DATA_INICIO = "DATA_INICIO"
    DATA_FIM = "DATA_FIM"
    CATEGORIA = "CATEGORIA"
    LOCAL_INICIO = "LOCAL_INICIO"
    LOCAL_FIM = "LOCAL_FIM"
    DISTANCIA = "DISTANCIA"
    PROPOSITO = "PROPOSITO"


class SinkCollumns:
    DT_REFE = "Data de referência."
    QT_CORR = "Quantidade de corridas."
    QT_CORR_NEG = "Quantidade de corridas com a categoria Negócio."
    QT_CORR_PESS = "Quantidade de corridas com a categoria Pessoal."
    VL_MAX_DIST = "Maior distância percorrida por uma corrida."
    VL_MIN_DIST = "Menor distância percorrida por uma corrida."
    VL_AVG_DIST = "Média das distâncias percorridas."
    QT_CORR_REUNI = "Quantidade de corridas com o propósito de Reunião."
    QT_CORR_NAO_REUNI = (
        "Quantidade de corridas com o propósito declarado e diferente de Reunião."
    )


class ProposalEnums:
    NEGOCIO = "Negocio"
    PESSOAL = "Pessoal"
    REUNIAO = "Reuniao"


BRONZE_SCHEMA: StructType = StructType(
    [
        StructField(SourceCollumns.DATA_INICIO, StringType(), True),
        StructField(SourceCollumns.DATA_FIM, StringType(), True),
        StructField(SourceCollumns.CATEGORIA, StringType(), True),
        StructField(SourceCollumns.LOCAL_INICIO, StringType(), True),
        StructField(SourceCollumns.LOCAL_FIM, StringType(), True),
        StructField(SourceCollumns.DISTANCIA, StringType(), True),
        StructField(SourceCollumns.PROPOSITO, StringType(), True),
    ]
)

CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/_checkpoints/rides_bronze"


class Stage(ABC):
    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:  # noqa E704
        ...


@dataclass(frozen=True)
class LandingToBronze(Stage):
    def transform(self, spark, input_path: str = LANDING_PATH) -> DataFrame:
        bronze_stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaEvolutionMode", "none")
            .option("trigger", "availableNow")
            .option("header", "true")
            .option("delimiter", ";")
            .schema(BRONZE_SCHEMA)
            .load(LANDING_PATH)
        )
        (
            bronze_stream.writeStream.format("delta")
            .option("checkpointLocation", CHECKPOINT)
            .trigger(availableNow=True)
            .outputMode("append")
            .toTable(TBL_BRONZE)
        )


@dataclass(frozen=True)
class BronzeToSilver(Stage):
    input_pattern: str = "MM-dd-yyyy H:mm"
    output_pattern: str = "yyyy-MM-dd"
    numeric_double_type: str = "double"

    def transform(self, df: DataFrame) -> DataFrame:
        parsed = (
            df.withColumn(
                SourceCollumns.DATA_INICIO,
                to_timestamp(col(SourceCollumns.DATA_INICIO), self.input_pattern),
            )
            .withColumn(
                SourceCollumns.DATA_FIM,
                to_timestamp(col(SourceCollumns.DATA_FIM), self.input_pattern),
            )
            .select(
                date_format(col(SourceCollumns.DATA_INICIO), self.output_pattern).alias(
                    SourceCollumns.DATA_INICIO
                ),
                date_format(col(SourceCollumns.DATA_FIM), self.output_pattern).alias(
                    SourceCollumns.DATA_FIM
                ),
                col(SourceCollumns.DISTANCIA)
                .cast(self.numeric_double_type)
                .alias(SourceCollumns.DISTANCIA),
                col(SourceCollumns.CATEGORIA).alias(SourceCollumns.CATEGORIA),
                col(SourceCollumns.LOCAL_INICIO).alias(SourceCollumns.LOCAL_INICIO),
                col(SourceCollumns.LOCAL_FIM).alias(SourceCollumns.LOCAL_FIM),
                col(SourceCollumns.PROPOSITO).alias(SourceCollumns.PROPOSITO),
            )
        )
        return parsed


@dataclass(frozen=True)
class SilverToGold(Stage):
    def transform(self, df: DataFrame) -> DataFrame:
        return df.groupBy(
            col(SourceCollumns.DATA_INICIO).alias(SinkCollumns.DT_REFE)
        ).agg(
            count("*").alias(SinkCollumns.QT_CORR.name),
            sum(
                (col(SourceCollumns.PROPOSITO) == ProposalEnums.NEGOCIO).cast("int")
            ).alias(SinkCollumns.QT_CORR_NEG.name),
            sum(
                (col(SourceCollumns.PROPOSITO) == ProposalEnums.PESSOAL).cast("int")
            ).alias(SinkCollumns.QT_CORR_PESS.name),
            max(col(SourceCollumns.DISTANCIA)).alias(SinkCollumns.VL_MAX_DIST.name),
            min(col(SourceCollumns.DISTANCIA)).alias(SinkCollumns.VL_MIN_DIST.name),
            round(avg(col(SourceCollumns.DISTANCIA)), 2).alias(
                SinkCollumns.VL_AVG_DIST.name
            ),
            sum(
                (col(SourceCollumns.PROPOSITO) == ProposalEnums.REUNIAO).cast("int")
            ).alias(SinkCollumns.QT_CORR_REUNI.name),
            sum(
                (
                    (col(SourceCollumns.PROPOSITO).isNotNull())
                    & (col(SourceCollumns.PROPOSITO) != ProposalEnums.REUNIAO)
                ).cast("int")
            ).alias(SinkCollumns.QT_CORR_NAO_REUNI.name),
        )
