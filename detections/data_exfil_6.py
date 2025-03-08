from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def detect(df: DataFrame) -> DataFrame:
    suspicious_events = ["AuthorizeSecurityGroupIngress"]

    logs = df.filter(
        F.col("eventName").isin(suspicious_events) & 
        (F.col("requestParameters.ipPermissions.items").like("%0.0.0.0/0%")) # this is arbitrary, but we can use threshold based
    )
    return logs
