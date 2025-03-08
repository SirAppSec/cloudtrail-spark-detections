from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def detect(df: DataFrame) -> DataFrame:
    suspicious_events = ["CreateDBInstanceReadReplica"]

    logs = df.filter(
        F.col("eventName").isin(suspicious_events) & 
        # ~F.col("awsRegion").isin(["us-east-1", "eu-west-1"]) &  # Common regions
        (F.col("requestParameters.publiclyAccessible") == "true")
    )
    return logs
