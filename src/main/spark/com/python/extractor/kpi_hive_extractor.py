import os
os.environ['PYSPARK_PYTHON'] = "python"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python"
print("Environment Variables: ")

from src.main.common.utils import spark_utils as utils
from src.main.common.constants import table_constants as tc
from pyspark.sql import SparkSession

def kpi_hive_extractor(spark,param_dict):

    logger = param_dict[tc.LOGGER_OBJ]
    logger.info("KPI Extractor Started")

    env = param_dict[tc.ENV]
    cust_sales_tbl = param_dict[tc.SRC_CUSTOMER_SALES_TBL]

    cust_sales_df = utils.read_from_hive(spark, cust_sales_tbl,env)
    cust_sales_df.printSchema()
    cust_sales_df.show(5)
    logger.info("KPI Extractor Finished")

    return {"extracted_cust_sales_df": cust_sales_df}