import importlib
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from schema_validation.cloudtrail import cloudtrail_schema, CloudTrailValidation


log_directory = "input_logs/logs_part2"
output_format = "stdout"
input = log_directory

def load_and_preprocess(spark: SparkSession, input_path: str) -> DataFrame:
    raw_df = spark.read.schema(cloudtrail_schema).json(input_path) # read json
    
    # Explode and flatten exactly what needed
    return raw_df.select(
        F.explode("Records").alias("record")
    ).select(
        F.col("record.eventName").alias("eventName"),
        F.col("record.eventSource").alias("eventSource"),
        F.col("record.eventTime").alias("eventTime"),
        F.col("record.sourceIPAddress").alias("sourceIPAddress"),
        F.col("record.userIdentity").alias("userIdentity"),
        F.col("record.requestParameters").alias("requestParameters"),
        F.col("record.responseElements").alias("responseElements"),
        F.col("record.resources").alias("resources"),
        F.col("record.errorCode").alias("errorCode")
    )
def main():
    spark = SparkSession.builder \
        .appName("CloudTrail Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # load df
    print(input+'/test.json')
    raw_df = spark.read.json(input+'/test.json')
    df = load_and_preprocess(spark,input +'/test.json')
    # validate DataFrame
    CloudTrailValidation.validate_df(df)
    # print("Final DataFrame Schema:")
    # df.printSchema()

    # print("Sample Data:")
    # df.select("eventName", "eventSource", "sourceIPAddress").show(5, truncate=False)
    try:
        
        # output handling
        if output_format == "stdout":
            results.show(truncate=False)
        else:
            results.write.mode("overwrite").json("data_exil_results.json")
    except AnalysisException as e:
        print(f"COLUMN ERROR: {str(e)}")
        print("Available columns:", df.columns)
        raise
        # Dynamically load all detection modules
        detection_modules = []
        detection_dir = Path("detections")
        for file in detection_dir.glob("data_exfil_*.py"):
            module_name = file.stem
            try:
                module = importlib.import_module(f"detections.{module_name}")
                detection_modules.append((module_name, module))
            except Exception as e:
                print(f"Failed to load {module_name}: {str(e)}")
                continue

        if not detection_modules:
            raise ValueError("No valid detection modules found in detections/ folder")

        # Process each detection
        for module_name, module in detection_modules:
            print(f"\nRunning detection: {module_name}")
            try:
                results = module.detect(df)
                if output_format == "stdout":
                    print(f"Results for {module_name}:")
                    results.show(truncate=False)
                else:
                    print(f"Saved results for {module_name} to {output_path}")
            except AnalysisException as e:
                print(f"Column error in {module_name}: {str(e)}")
                print("Available columns:", df.columns)
            except Exception as e:
                print(f"Error in {module_name}: {str(e)}")

    except Exception as e:
        print(f"Fatal error: {str(e)}")
    finally:
        spark.stop()
if __name__ == "__main__":
    main()
