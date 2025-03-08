import importlib
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from schema_validation.cloudtrail import cloudtrail_schema, CloudTrailValidation


log_directory = "input_logs/logs_part2/"
output_format = "stdout"
output_folder = "/output/"
input = log_directory

def load_and_preprocess(spark, input_path):
    return spark.read.schema(cloudtrail_schema) \
        .json(input_path) \
        .select(F.explode("Records").alias("record")) \
        .select("record.*")
def main():
    spark = SparkSession.builder \
        .appName("CloudTrail Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false") \
        .getOrCreate()

    try:
        df = load_and_preprocess(spark, input) # expand and flattens all Record logs in a directory
        CloudTrailValidation.validate_df(df) #validate Schema
        
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
                    output_path = f"{output_folder}{module_name}_results.json"
                    results.write.mode("overwrite").json(output_path)
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
