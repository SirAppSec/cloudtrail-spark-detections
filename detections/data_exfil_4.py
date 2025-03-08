from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def detect(df: DataFrame) -> DataFrame:
    suspicious_events = ["CreateReplicationTask"]
    
    logs = df.filter(
        F.col("eventName").isin(suspicious_events) & 
        (F.split(F.col("requestParameters.targetEndpointArn"), ":")[4] != F.col("userIdentity.accountId")) &
        (F.col("requestParameters.migrationType") == "full-load")
    )
    return logs
