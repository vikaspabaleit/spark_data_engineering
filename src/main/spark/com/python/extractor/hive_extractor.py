import os
os.environ['PYSPARK_PYTHON'] = "python"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python"
print("Environment Variables: ")

from src.main.common.utils import spark_utils as utils
from src.main.common.constants import table_constants as tc

def hive_extractor(spark,param_dict):

    env = param_dict[tc.ENV]
    db_nm = param_dict[tc.DB_NM]
    tbl_nm = param_dict[tc.TBL_NM]
    filter_condition = "customer_id != 10"

    cust_sales_df = utils.extract_from_hive(spark, db_nm, tbl_nm, env,filter_condition)
    cust_sales_df.printSchema()
    cust_sales_df.show()

    return {"extracted_cust_sales_df": cust_sales_df}
