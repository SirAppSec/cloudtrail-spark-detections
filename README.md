# Cloudtrail Pattern Detections using Spark

## Quick start
Install Spark
```
cd pyspark-playground
make run

or
sudo apt update
sudo apt install default-jdk
java -version
wget https://archive.apache.org/dist/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz
tar -xvzf spark-3.2.0-bin-hadoop3.2.tgz
sudo mv spark-3.2.0-bin-hadoop3.2 /opt/spark

```
add the following to bashrc
export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
```
update bash/zshrc
```
# source ~/.bashrc
source ~/.zshrc
```
install pyspark 
```
pip install pyspark
```
## quick start project

