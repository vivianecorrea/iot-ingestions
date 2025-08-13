from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, max, min, round, sum

from assignment_1.rides_ingestion.src.base import Stage
from assignment_1.rides_ingestion.src.constants import TBL_GOLD, TBL_SILVER
from assignment_1.rides_ingestion.src.schemas import (
    ProposalEnums,
    SinkCollumns,
    SourceCollumns,
)

spark = SparkSession.builder.getOrCreate()


class SilverToGold(Stage):
    def transform(self) -> DataFrame:

        df_gold = (
            spark.read.table(TBL_SILVER)
            .groupBy(col(SourceCollumns.DATA_INICIO).alias(SinkCollumns.DT_REFE))
            .agg(
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
        )
        return df_gold


job = SilverToGold(layer="gold", table_name=TBL_GOLD, output_mode="complete")
job.run()
