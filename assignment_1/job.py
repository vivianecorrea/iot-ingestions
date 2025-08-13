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

from assignment_1.assets.constants import (
    CHECKPOINT,
    LANDING_PATH,
    TBL_BRONZE,
    TBL_GOLD,
    TBL_SILVER,
)
from assignment_1.assets.schemas import (
    BRONZE_SCHEMA,
    ProposalEnums,
    SinkCollumns,
    SourceCollumns,
)


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
            .load(input_path)
        )
        (
            bronze_stream.writeStream.format("delta")
            .option("checkpointLocation", f"{CHECKPOINT}bronze")
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
        (
            parsed.writeStream.format("delta")
            .option("checkpointLocation", f"{CHECKPOINT}silver")
            .trigger(availableNow=True)
            .outputMode("update")
            .toTable(TBL_SILVER)
        )


@dataclass(frozen=True)
class SilverToGold(Stage):
    def transform(self, df: DataFrame) -> DataFrame:

        df_gold = df.groupBy(
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
        (
            df_gold.writeStream.format("delta")
            .option("checkpointLocation", f"{CHECKPOINT}gold")
            .trigger(availableNow=True)
            .outputMode("complete")
            .toTable(TBL_GOLD)
        )
