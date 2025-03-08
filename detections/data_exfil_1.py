from pyspark.sql import DataFrame
from pyspark.sql import functions as F
    
# Export to S3 buckets
def detect(df: DataFrame) -> DataFrame:
    suspicious_events = ["StartExportTask"]
    logs = df.filter(
    F.col("eventName").isin(suspicious_events)# & 
    # ~F.col("requestParameters.s3BucketName").rlike(
    #     r"^(backup|audit|internal)-\w+"  # Common benign patterns
    # )
)
    return logs
