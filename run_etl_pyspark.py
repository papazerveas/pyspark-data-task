import argparse

from data_task.tools import Dataset, get_config
from data_task.etl_process_pyspark import create_spark_session, load_data, preprocess_data, save_to_parquet

if __name__ == "__main__":

    # Create ArgumentParser object
    parser = argparse.ArgumentParser(description="Etl process arguments")

    # Add arguments
    parser.add_argument("--download_data", type=bool, default=True, help="Download Data from Kaggle")
    parser.add_argument("--config", type=str, default="config.yml", help="YAML configuration")

    # Parse the command-line arguments
    args = parser.parse_args()
    config = get_config(yml_file=args.config)

    if args.download_data:
        print(" download datasets")
        ds = Dataset(config=config)
        ds.download()

    spark_config = config.get("spark")

    # Create Spark session
    spark = create_spark_session()

    # Load data
    customers, orders, order_items, products, translations = load_data(
        spark, spark_config.get("input_path", 'brazilian-ecommerce'))

    # ETL processing
    forecast_ts = preprocess_data(customers, orders, order_items, products, translations)

    forecast_ts.show()
    # Save to Parquet
    save_to_parquet(
        df=forecast_ts,
        output_path=spark_config.get("output_path", 'output_path'),
        partition_by=spark_config.get("partition_by", ['order_purchase_week_end_sunday'])
    )
