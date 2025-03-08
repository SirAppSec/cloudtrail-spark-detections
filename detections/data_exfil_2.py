from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def detect(df: DataFrame) -> DataFrame:
    suspicious_events = ["ModifyDBSnapshotAttribute"]

    logs = df.filter(
        F.col("eventName").isin(suspicious_events) &
        (F.col("requestParameters.attributeName") == "restore") #&
                # (F.size(F.col("requestParameters.valuesToAdd")) > 0)
    )    
    return logs
