from typing import List, Tuple, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum, count, next_day, date_trunc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def create_spark_session(name: str = "SalesForecastingEtl") -> SparkSession:
    return SparkSession.builder.appName(name).getOrCreate()


def load_data(
        spark: SparkSession,
        path: str = 'brazilian-ecommerce') -> Tuple[
            DataFrame, DataFrame, DataFrame, DataFrame, DataFrame]:
    """
        Load relevant tables
    """
    customer_schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_unique_id", StringType()),
        StructField("customer_zip_code_prefix", StringType()),
        StructField("customer_city", StringType()),
        StructField("customer_state", StringType()),
    ])
    customers = spark.read.csv(
        f'{path}/olist_customers_dataset.csv', header=True, schema=customer_schema)

    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType()),
        StructField("order_purchase_timestamp", TimestampType()),
        StructField("order_approved_at", TimestampType()),
        StructField("order_delivered_carrier_date", TimestampType()),
        StructField("order_delivered_customer_date", TimestampType()),
        StructField("order_estimated_delivery_date", TimestampType()),
    ])
    orders = spark.read.csv(
        f'{path}/olist_orders_dataset.csv', header=True, schema=orders_schema)

    order_items_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType()),
        StructField("shipping_limit_date", TimestampType()),
        StructField("price", DoubleType()),
        StructField("freight_value", DoubleType())
    ])
    order_items = spark.read.csv(
        f'{path}/olist_order_items_dataset.csv', header=True, schema=order_items_schema)

    products_schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_category_name", StringType()),
        StructField("product_name_lenght", IntegerType()),
        StructField("product_description_lenght", IntegerType()),
        StructField("product_photos_qty", IntegerType()),
        StructField("product_weight_g", DoubleType()),
        StructField("product_length_cm", IntegerType()),
        StructField("product_height_cm", IntegerType()),
        StructField("product_width_cm", IntegerType())
    ])
    products = spark.read.csv(
        f'{path}/olist_products_dataset.csv', header=True, schema=products_schema)

    translations_schema = StructType([
        StructField("product_category_name", StringType(), False),
        StructField("product_category_name_english", StringType(), False)
    ])
    translations = spark.read.csv(
        f'{path}/product_category_name_translation.csv', header=True, schema=translations_schema)

    # NOT USED IN FORECASTING
    # geolocation = spark.read.csv( -- not needed at forecast
    #     f'{path}/olist_geolocation_dataset.csv', header=True, inferSchema=False)
    # order_payments_schema = StructType([
    #     StructField("order_id", StringType(), False),
    #     StructField("payment_sequential", IntegerType(), False),
    #     StructField("payment_type", StringType()),
    #     StructField("payment_installments", IntegerType()),
    #     StructField("payment_value", StringType())
    # ])
    # order_payments = spark.read.csv(
    #     f'{path}/olist_order_payments_dataset.csv', header=True, schema=order_payments_schema)
    # order_reviews = spark.read.csv(
    #     f'{path}/olist_order_reviews_dataset.csv', header=True, inferSchema=True)
    # sellers = spark.read.csv(
    #     f'{path}/olist_sellers_dataset.csv', header=True, inferSchema=True)

    return customers, orders, order_items, products, translations


def preprocess_data(
        customers: DataFrame,
        orders: DataFrame,
        order_items: DataFrame,
        products: DataFrame,
        translations: DataFrame) -> DataFrame:
    """Perform necessary preprocessing and feature engineering

    Args:
        customers (DataFrame): _description_
        orders (DataFrame): _description_
        order_items (DataFrame): _description_
        products (DataFrame): _description_
        translations (DataFrame): _description_

    Returns:
        DataFrame: _description_
    """

    # orders.select("order_status").distinct().show()
    # +------------+
    # |order_status|
    # +------------+
    # |     shipped|
    # |    canceled|
    # |    invoiced|
    # |     created|
    # |   delivered|
    # | unavailable|
    # |  processing|
    # |    approved|
    # +------------+

    orders_week = (
        orders  # 99441
        # maybe unavailable is reasonable for demand forecasting
        .filter(~col("order_status").isin(["canceled", "unavailable"]))
        # usually for week forecasting we use week end sunday
        .withColumn("order_purchase_week_end_sunday", next_day(
            date_trunc("week", col("order_purchase_timestamp")), "Sun"))
        .select("order_id", "customer_id", "order_purchase_week_end_sunday")
    )  # DataFrame[order_id: string, customer_id: string, order_purchase_week_end_sunday: date

    # DataFrame[order_id: string, product_id: string, sales_value: double, sales_items: bigint, shipping_cost: double]
    order_items_agg = (
        order_items.groupBy(["order_id", "product_id"])  # ,"seller_id"
        .agg(
            sum("price").alias("sales_value"),
            count("price").alias("sales_items"), sum(
                "freight_value").alias("shipping_cost")
        )
    )

    products_en = (
        products
        .join(translations, "product_category_name", "inner")
        .select("product_id", "product_category_name_english")
    )

    # denormalize tables - rows 101953
    denormalized_data = (
        orders_week  # ct: 98207
        .join(order_items_agg, 'order_id', 'inner')  # ct: 101953
        # ct: 101953 -  product category might need for forecasting
        .join(products_en, 'product_id', 'inner')
        # ct: 101953
        .join(customers.select("customer_id", "customer_city", "customer_state"), 'customer_id', 'inner')
        # .join(sellers.select("seller_id","seller_city","seller_state"),"seller_id", 'inner')
        # .join(order_payments, 'order_id', 'inner') -- forecast total revenue here
    )

    # +------------+
    # |payment_type|
    # +------------+
    # |      boleto|
    # | not_defined|
    # | credit_card|
    # |     voucher|
    # |  debit_card|
    # +------------+
    # order_payments.select("payment_type").distinct().show()
    # denormalized_data[seller_id: string, customer_id: string, product_id: string, order_id: string,
    # order_purchase_week_end_sunday: date, sales_value: double, sales_items: bigint, shipping_cost: double,
    # product_category_name: string, customer_city: string, customer_state: string ]
    forecast_data = (
        denormalized_data
        .groupBy([
            "order_purchase_week_end_sunday",
            "product_id",
            "product_category_name_english",
            "customer_city",
            "customer_state"
        ])
        .agg(
            sum("sales_value").alias("sales_value"),
            count("sales_items").alias("sales_items"),
            sum("shipping_cost").alias("shipping_cost")
        )
    )

    # forecast_ts.groupBy("product_id").count().describe().show()
    # forecast_ts.groupBy("product_id").count().select("count").rdd.flatMap(lambda x: x).histogram(10)
    # hist_values, bin_edges = forecast_ts.groupBy(
    #   "product_category_name_english").count().select("count").rdd.flatMap(lambda x: x).histogram(10)
    # plt.figure(figsize=(10, 6))
    # plt.bar(bin_edges[:-1], hist_values, width=(bin_edges[1] - bin_edges[0]), color='blue')
    # forecast_ts.groupBy("product_category_name_english").count().describe().show()

    return (
        forecast_data
        .sort([
            "product_category_name_english",
            "product_id",
            "customer_state",
            "customer_city",
            "order_purchase_week_end_sunday"
        ])
    )


def save_to_parquet(
    df: DataFrame,
    output_path: str,
    partition_by: Union[List[str], str] = 'product_id'
) -> None:
    """
    Save the processed df to Parquet, partitioned by product
    """
    df.write.partitionBy(partition_by).parquet(output_path, mode='overwrite')


def calc_skew(
    df: DataFrame,
    col: Union[List[str], str] = "product_id"
) -> float:
    """calculate skew of a partition"""

    # get count per column
    partition_counts = df.groupBy(col).count()

    # get maximum value of count
    max_partition_size = partition_counts.agg({"count": "max"}).collect()[0][0]

    avg_partition_size: float = df.count() / partition_counts.count()

    # the lowest the better
    skewness_metric = max_partition_size / avg_partition_size
    print("Skewness Metric:", skewness_metric)
    return skewness_metric
