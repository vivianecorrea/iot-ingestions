from abc import ABC, abstractmethod

from pyspark.sql import DataFrame

from assignment_1.assets.constants import CHECKPOINT


class Stage(ABC):
    def __init__(self, layer: str, table_name: str, output_mode: str):
        self.layer = layer
        self.table_name = table_name
        self.output_mode = output_mode

    def run(self):
        transformed_df = self.transform()
        self._save_df_to_table(transformed_df)

    def _save_df_to_table(self, df: DataFrame) -> None:
        (
            df.writeStream.format("delta")
            .option("checkpointLocation", f"{CHECKPOINT}{self.layer}")
            .trigger(availableNow=True)
            .outputMode(self.output_mode)
            .toTable(self.table_name)
            .trigger(availableNow=True)
        )

    @abstractmethod
    def transform(self): ...  # noqa E701
