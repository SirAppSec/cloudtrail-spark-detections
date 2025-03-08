import importlib
import json
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

log_directory = "input_logs"
output_format = "stdout"
input = log_directory

def main():
    spark = SparkSession.builder \
        .appName("CloudTrail Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    try:
        detection_module = importlib.import_module(f"detections.data_exfil_0")
        
        df = spark.read.json(input)
        
        results = detection_module.detect(df)
        
        # output handling
        if output_format == "stdout":
            results.show(truncate=False)
        else:
            results.write.mode("overwrite").json("data_exil_results.json")
            
    except Exception as e:
        print(f"Error processing detection: {str(e)}")
    finally:
        spark.stop()
if __name__ == "__main__":
    main()
