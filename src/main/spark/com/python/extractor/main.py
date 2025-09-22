import os
import sys
import traceback
import uuid
from traceback import print_tb

from src.main.common.utils import spark_utils as utils
from src.main.common.utils.spark_session_factory import create_spark_session
from src.main.common.constants import table_constants as tc
from src.main.spark.com.python.extractor import kpi_hive_extractor as extractor

os.environ['PYSPARK_PYTHON'] = "python"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python"

# main.py
from pyspark.sql import SparkSession
#from src.transformations import clean_customers  # example import

def main():

    param_dict=dict()
    start_time = utils.get_current_timestamp()
    job_nm = "DEFAULT"
    job_layer = "DEFAULT"
    audit_gcs_bkt = "DEFAULT"
    print("  start_time :   ", start_time)
    try:
        param_dict["env"] = "local"
        param_dict["job_nm"] = job_nm
        param_dict["job_layer"] = job_layer
        param_dict["audit_gcs_bkt"] = audit_gcs_bkt
        import os
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        input_path = os.path.join(BASE_DIR, "customers_raw.csv")
        param_dict["src_customer_sales_tbl"] = input_path
        spark, logger = create_spark_session(param_dict)
        param_dict[tc.SPARK_OBJ] = spark
        param_dict[tc.LOGGER_OBJ] = logger

        print(" Extraction process started.")
        extractor.kpi_hive_extractor(spark,param_dict)

        return None

    except Exception as e:
        logger.error("Job execution failed with exception:", e)
        sys.exit(1)

    finally:
        utils.stop_context(spark)


if __name__ == "__main__":
    print("Usage: python main.py <input_path> <output_path>")
    main()
