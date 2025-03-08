from pyspark.sql import DataFrame
from pyspark.sql import functions as F

suspicious_events = ["GetObject"]
def detect(df: DataFrame) -> DataFrame:
    logs = df.filter(
        F.col("eventName").isin(suspicious_events) 
    )    
    return logs
