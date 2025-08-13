from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp

from assignment_1.rides_ingestion.src.constants import TBL_BRONZE, TBL_SILVER
from assignment_1.rides_ingestion.src.schemas import SourceCollumns
from assignment_1.rides_ingestion.src.stage import Stage

spark = SparkSession.builder.getOrCreate()


class BronzeToSilver(Stage):
    input_pattern: str = "MM-dd-yyyy H:mm"
    output_pattern: str = "yyyy-MM-dd"
    numeric_double_type: str = "double"

    def transform(self, df: DataFrame) -> DataFrame:
        parsed = (
            spark.read.table(TBL_BRONZE)
            .withColumn(
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


job = BronzeToSilver(layer="silver", table_name=TBL_SILVER, output_mode="append")
job.run()
