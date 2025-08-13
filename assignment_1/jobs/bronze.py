from jobs.base import Stage
from pyspark.sql import DataFrame, SparkSession

from assignment_1.assets.constants import LANDING_PATH, TBL_BRONZE
from assignment_1.assets.schemas import BRONZE_SCHEMA

spark = SparkSession.builder.getOrCreate()


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
        return bronze_stream


job = LandingToBronze(layer="bronze", table_name=TBL_BRONZE, output_mode="append")
job.run()
