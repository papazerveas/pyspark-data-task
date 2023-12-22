from data_task.tools import Dataset, get_config
from data_task.etl_process_pyspark import create_spark_session, load_data, preprocess_data, save_to_parquet, calc_skew

if __name__ == "__main__":

    # download datasets
    ds = Dataset()
    ds.download()

    config = get_config()
    spark_config = config.get("spark")
    
    # Create Spark session
    spark = create_spark_session()

    # Load data
    customers, orders, order_items, products, translations = load_data(spark, spark_config.get("input_path",'brazilian-ecommerce'))

    # ETL processing
    forecast_ts = preprocess_data(customers, orders, order_items, products, translations)


    # Save to Parquet
    save_to_parquet(
        data=forecast_ts, 
        output_path= spark_config.get("output_path",'output_path'), 
        partition_by = spark_config.get("partition_by",['product_category_name_english']) 
        )



