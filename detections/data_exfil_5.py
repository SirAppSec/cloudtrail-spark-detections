from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def detect(df: DataFrame) -> DataFrame:
    suspicious_events = ["ExecuteStatement"]

    logs = df.filter(
        F.col("eventName").isin(suspicious_events) & 
        (F.col("count") > 1000) # this is arbitrary, but we can use threshold based
    )
    return logs
