# Pyspark forecasting DE task

## setup

### Getting the Spark files

Steps:
Get the necessary Spark version file from the website at Apache Spark Downloads.

Extract to: C:\spark_setup\spark-3.3.0-bin-hadoop2

Check to see if the Winutils version matches the Hadoop version. The version listed below is necessary for the prior installation of Hadoop 2.

Download the winutils.exe and hadoop.dll files for the Spark and Hadoop versions from this [winutils](https://github.com/cdarlint/winutils/tree/master/hadoop-2.7.2/bin)
 . Please make use of hadoop-2.7.2/bin.

After downloading, copy the files into the bin directory at C:\spark_setup\spark-3.3.0-bin-hadoop2.

Copy and replace the following files (hadoop.dll and winutils.exe) in the hadoop\bin directories you created: C:\spark_setup\spark-3.3.0-bin-hadoop2\bin and C:\spark_setup\spark-3.3.0-bin-hadoop2\hadoop\bin.

pip install -r requirements.txt
set $SPARK_HOME environment variable = "C:\spark_setup\spark-3.3.0-bin-hadoop2"

## Run

### kaggle config

```text
create a kaggle.json file like {"username":"papazerveas","key":"xxxxx"}
or download it from kaggle
```

### run the etl pipeline

```cmd
help: python run_etl_pyspark.py --help
run: python run_etl_pyspark.py --download_data True --config config.yml
```

### testing

```cmd
python -m pytest
```
