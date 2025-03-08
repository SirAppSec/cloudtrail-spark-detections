import argparse
def parse_args():
    parser = argparse.ArgumentParser(
        description="PySpark CloudTrail detections",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "--input", "-i",
        type=str,
        default="input_logs/logs_part2/",
        help="path to directoyr"
             "supports partisioned data sets and nested dirs"
    )
    
    parser.add_argument(
        "--output-format", "-of",
        choices=["stdout", "json"],
        default="stdout",
        help="output format for the results"
             "'stdout' prints to console, 'json' writes to files"
    )
    
    parser.add_argument(
        "--output-dir", "-od",
        type=str,
        default="output/",
        help="output directory for json formatted resutls"
    )
    
    parser.add_argument(
        "--detections-dir", "-dd",
        type=str,
        default="detections",
        help="directory of the detections "
            "data exfiltration should be named: 'data_exfil_*.py'"
    )

    return parser.parse_args()
