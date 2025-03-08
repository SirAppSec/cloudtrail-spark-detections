from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def detect(df: DataFrame) -> DataFrame:
    suspicious_events = ["ModifyDBInstance"]

    logs = df.filter(
        F.col("eventName").isin(suspicious_events) & 
        F.col("requestParameters").getItem("masterUserPassword").isNotNull() & 
        (F.col("userIdentity.type") == "AssumedRole")  # Especially new roles
    )
    return logs
