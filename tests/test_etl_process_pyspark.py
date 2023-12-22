import unittest
import data_task.etl_process_pyspark as etl
from data_task.tools import Dataset

class PySparTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = etl.create_spark_session("unit-test")
        d = Dataset()
        d.download() # download data on startup

    @classmethod
    def tearDownClass(cls) -> None:
        print("finished")
        cls.spark.stop()

class TestAppendData(PySparTestCase):
    
# @pytest.fixture
# def spark():
#     return create_spark_session("unit-test")

    def test_load_data(self):
        customers, orders, order_items, products, translations  = etl.load_data(self.spark)
        assert customers.count() > 0
        assert orders.count() > 0
        assert order_items.count() > 0
        assert products.count() > 0
        assert translations.count() > 0

    def test_preprocess_data(self):
        customers, orders, order_items, products, translations  = etl.load_data(self.spark)
        product_weekly_sales = etl.preprocess_data(customers, orders, order_items, products, translations)
        assert product_weekly_sales.count() > 0


    def test_calc_skew(self):
        customers, orders, order_items, products, translations  = etl.load_data(self.spark)
        product_weekly_sales = etl.preprocess_data(customers, orders, order_items, products, translations)
        assert etl.calc_skew(product_weekly_sales) > 0


    # def test_save_to_parquet(self, tmpdir):
    #     customers, orders, order_items, products, translations  = load_data(self.spark)
    #     product_weekly_sales = preprocess_data(customers, orders, order_items, products, translations)
    #     output_path = str(tmpdir.join("test_output"))
    #     save_to_parquet(product_weekly_sales, output_path)
    #     assert len(tmpdir.listdir()) > 0
