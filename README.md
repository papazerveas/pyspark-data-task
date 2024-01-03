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

### calculate skew

```python
from data_task.etl_process_pyspark import calc_skew
calc_skew(forecast_ts, col= ['product_category_name_english']) # 7.172007821761112
calc_skew(forecast_ts, col= ['order_purchase_week_end_sunday'])  # 2.789086008976788
calc_skew(forecast_ts, col= ['order_purchase_week_end_sunday','product_category_name_english']) # 17.407835945652945
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

### output schema

>>> forecast_ts.show()
+------------------------------+--------------------+-----------------------------+--------------------+--------------+-----------+-----------+-------------+
|order_purchase_week_end_sunday|          product_id|product_category_name_english|       customer_city|customer_state|sales_value|sales_items|shipping_cost|
+------------------------------+--------------------+-----------------------------+--------------------+--------------+-----------+-----------+-------------+
|                    2018-04-15|018ca97302e429305...|         agro_industry_and...|           sao paulo|            SP|       88.0|          1|        11.95|
|                    2018-08-12|026f43af35e795106...|         agro_industry_and...|           juquitiba|            SP|      359.8|          1|        62.68|
|                    2018-03-18|07f01b6fcacc1b187...|         agro_industry_and...|           cabo frio|            RJ|       57.0|          1|        29.99|
|                    2018-04-08|0a0adf0de1769b297...|         agro_industry_and...|           sao paulo|            SP|       58.5|          1|        29.11|
|                    2018-08-05|0a27862bbf658a5b8...|         agro_industry_and...|      rio de janeiro|            RJ|      51.73|          1|        66.89|
|                    2018-07-22|0b2a1288e8ba64c79...|         agro_industry_and...|  cachoeira paulista|            SP|       67.7|          1|        34.23|
|                    2018-04-29|0b2a1288e8ba64c79...|         agro_industry_and...|        santa isabel|            SP|       96.9|          1|         7.54|
|                    2018-03-18|10e33205ed375c8e4...|         agro_industry_and...|          piracicaba|            SP|      330.0|          1|        61.15|
|                    2018-03-04|11250b0d4b709fee9...|         agro_industry_and...|      belo horizonte|            MG|      412.0|          1|        26.26|
|                    2018-01-21|11250b0d4b709fee9...|         agro_industry_and...|governador valadares|            MG|      412.0|          1|        30.26|
|                    2017-12-31|11250b0d4b709fee9...|         agro_industry_and...|        juiz de fora|            MG|      412.0|          1|        30.26|
|                    2018-02-04|11250b0d4b709fee9...|         agro_industry_and...|              bonito|            MS|      412.0|          1|        30.26|
|                    2018-02-11|11250b0d4b709fee9...|         agro_industry_and...|           garanhuns|            PE|      412.0|          1|        60.67|
|                    2018-02-04|11250b0d4b709fee9...|         agro_industry_and...|        alto piquiri|            PR|      412.0|          1|         20.5|
|                    2018-02-25|11250b0d4b709fee9...|         agro_industry_and...|            curitiba|            PR|      412.0|          1|        17.09|
|                    2018-04-08|11250b0d4b709fee9...|         agro_industry_and...|   francisco beltrao|            PR|      412.0|          1|         20.4|
|                    2018-02-04|11250b0d4b709fee9...|         agro_industry_and...|         nova iguacu|            RJ|      412.0|          1|        26.26|
|                    2018-03-25|11250b0d4b709fee9...|         agro_industry_and...|      rio de janeiro|            RJ|      412.0|          1|         26.2|
|                    2018-04-22|11250b0d4b709fee9...|         agro_industry_and...|           ararangua|            SC|      412.0|          1|        33.87|
|                    2018-01-21|11250b0d4b709fee9...|         agro_industry_and...|              amparo|            SP|      412.0|          1|        27.72|
+------------------------------+--------------------+-----------------------------+--------------------+--------------+-----------+-----------+-------------+
only showing top 20 rows

### github actions

Go to "Settings" > "Secrets" > Actions > "New repository secret."
KAGGLE_USERNAME
KAGGLE_KEY
