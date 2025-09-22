import os

from src.test.common.utils import spark_utils as utils
from src.test.common.utils.spark_session_factory import create_spark_session
from src.test.common.constants import table_constants as tc
from src.main.spark.com.python.extractor import hive_extractor as extractor

os.environ['PYSPARK_PYTHON'] = "python"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python"

def test_hive_extractor():

    param_dict=dict()
    start_time = utils.get_current_timestamp()
    print("Start time: {}".format(start_time))

    param_dict["job_nm"] = "hive_extractor"
    param_dict["job_layer"] = "extractor_layer"
    param_dict["audit_gcs_bkt"] = "audit_gcs_bkt"
    param_dict["env"] = "local"
    spark, logger = create_spark_session(param_dict)

    param_dict[tc.SPARK_OBJ] = spark
    param_dict[tc.DB_NM] = "db_nm"
    param_dict[tc.TBL_NM] = "tbl_nm"

    print(" Test Extraction process started.")
    extractor.hive_extractor(spark,param_dict)
    print(" Extraction process has been stopped.")

    return None


if __name__ == "__main__":
    print("test_hive_extractor has been started.")
    test_hive_extractor()
