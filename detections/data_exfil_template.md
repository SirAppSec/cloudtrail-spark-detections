from pyspark.sql import DataFrame
from pyspark.sql import functions as F

suspicious_event = "ModifyDBSnapshotAttribute"
def detect(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("eventName").isin(suspicious_event)) &
        (F.col("requestParameters.attributeName") == "restore")
    )
