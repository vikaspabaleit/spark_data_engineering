import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, Row
from src.main.spark.com.python.extractor import kpi_hive_extractor
from src.main.common.constants import table_constants as tc

class TestKpiHiveExtractor(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Create a local Spark session for testing
        cls.spark = (
            SparkSession.builder
            .appName("KpiHiveExtractorTest")
            .master("local[*]")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    @patch("src.main.common.utils.spark_utils.read_from_hive")
    def test_kpi_hive_extractor_with_dummy_data(self, mock_read_from_hive):
        """
        Test kpi_hive_extractor function using a dummy DataFrame
        """

        # 1️⃣ Create a dummy DataFrame to simulate Hive table
        sample_data = [
            Row(customer_id=1, name="Alice", email="alice@example.com", age=30, purchase_amount=100),
            Row(customer_id=2, name="Bob", email="bob@example.com", age=35, purchase_amount=200),
        ]
        dummy_df = self.spark.createDataFrame(sample_data)

        # 2️⃣ Mock the Hive read to return the dummy DataFrame
        mock_read_from_hive.return_value = dummy_df

        # 3️⃣ Prepare a dummy param_dict
        mock_logger = MagicMock()
        param_dict = {
            tc.LOGGER_OBJ: mock_logger,
            tc.ENV: "dev",
            tc.SRC_CUSTOMER_SALES_TBL: "dummy_table_path_or_name",
            tc.SPARK_OBJ: self.spark
        }

        # 4️⃣ Call the function
        result = kpi_hive_extractor.kpi_hive_extractor(self.spark, param_dict)

        # 5️⃣ Assertions
        self.assertIn("extracted_cust_sales_df", result)
        df = result["extracted_cust_sales_df"]
        self.assertEqual(df.count(), 2)
        self.assertEqual(set(df.columns), {"customer_id", "name", "email", "age", "purchase_amount"})

        # 6️⃣ Optional: Print DataFrame for debugging
        print("\n===== Extracted DataFrame =====")
        df.show(truncate=False)
        print("===== End of DataFrame =====\n")

        # 7️⃣ Verify that logger info was called
        mock_logger.info.assert_any_call("KPI Extractor Started")
        mock_logger.info.assert_any_call("KPI Extractor Finished")


if __name__ == "__main__":
    unittest.main()
