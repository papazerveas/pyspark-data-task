import unittest
from unittest.mock import patch
import data_task.etl_process_pyspark as etl
from data_task.tools import Dataset,get_config

# use fast true for development.
fast_test = False


class PySparTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        if fast_test:
            cls.spark = patch('pyspark.sql.SparkSession').start()
            df = cls.spark.createDataFrame(
                data=[[1, 'test-loc1', 'test-domain1', 'test-odp1'],
                      [2, 'test-loc2', 'test-domain2', 'test-odp2']],
                schema=['id', 'location', 'DOMAIN', 'ODP']
            )
            df.count.return_value = 2
            cls.csv_patch = patch(
                "pyspark.sql.SparkSession.read.csv", return_value=df)
        else:
            cls.spark = etl.create_spark_session(
                "unit-test")  # no point to test connection
            cls.config = get_config(yml_file="config-test.yml")
            d = Dataset(config=cls.config)
            d.download()  # download data on startup

    @classmethod
    def tearDownClass(cls) -> None:
        print("finished")
        cls.spark.stop()


class TestAppendData(PySparTestCase):

    def test_load_data(self):
        if fast_test:
            csv_mock = self.csv_patch.start()
        customers, orders, order_items, products, translations = etl.load_data(
            self.spark)
        if fast_test:
            self.csv_patch.stop()
            csv_mock.assert_called()
            assert csv_mock.call_count == 5
        assert customers.count() > 0
        assert orders.count() > 0
        assert order_items.count() > 0
        assert products.count() > 0
        assert translations.count() > 0

    def test_preprocess_data(self):
        customers, orders, order_items, products, translations = etl.load_data(
            self.spark)
        product_weekly_sales = etl.preprocess_data(
            customers, orders, order_items, products, translations)
        assert product_weekly_sales.count() > 0

    def test_calc_skew(self):
        customers, orders, order_items, products, translations = etl.load_data(
            self.spark)
        product_weekly_sales = etl.preprocess_data(
            customers, orders, order_items, products, translations)
        assert etl.calc_skew(product_weekly_sales) > 0

    def test_save_to_parquet(self):
        customers, orders, order_items, products, translations = etl.load_data(
            self.spark)
        product_weekly_sales = etl.preprocess_data(
            customers, orders, order_items, products, translations)

        # don't export
        df_patch = patch(
            'pyspark.sql.DataFrameWriter.parquet', return_value=None)
        _df_mock = df_patch.start()
        # flake8: noqa F401
        etl.save_to_parquet(product_weekly_sales,
                            "test_output", partition_by=["id"])
        df_patch.stop()
        # df_mock.assert_any_call()
