# Cloudtrail Pattern Detections using Spark

## Quick start
Install Spark
```
cd pyspark-playground
make run
```
or
```
sudo apt update
sudo apt install default-jdk
java -version
wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
tar -xvzf spark-3.2.0-bin-hadoop3.2.tgz
sudo mv spark-3.2.0-bin-hadoop3.2 /opt/spark

```

add the following to bashrc:
```
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```
update bash/zshrc
```
# source ~/.bashrc
source ~/.zshrc
```
install pyspark :
```
pip install pyspark # Optimally use in venv
```
## quick start project
initialize virtual env:
```
# pip install virtualenv # if missing
python3 -m venv venv
source venv/bin/activate
```
## install requirements
```
pip install -r requirements.txt
```
## Test pyspark
```
python tests/validate_pyspark.py
```


# CLI
example:
```
python cli.py
```
abstraction:
```
❯ python cli.py -h
usage: cli.py [-h] [--input INPUT] [--output-format {stdout,json}] [--output-dir OUTPUT_DIR] [--detections-dir DETECTIONS_DIR]

PySpark CloudTrail detections

options:
  -h, --help            show this help message and exit
  --input INPUT, -i INPUT
                        path to directoyrsupports partisioned data sets and nested dirs (default: input_logs/logs_part2/)
  --output-format {stdout,json}, -of {stdout,json}
                        output format for the results'stdout' prints to console, 'json' writes to files (default: stdout)
  --output-dir OUTPUT_DIR, -od OUTPUT_DIR
                        output directory for json formatted resutls (default: /output/)
  --detections-dir DETECTIONS_DIR, -dd DETECTIONS_DIR
                        directory of the detections data exfiltration should be named: 'data_exfil_*.py' (default: detections)
```

