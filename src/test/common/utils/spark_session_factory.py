import sys, logging
from pyspark import SparkConf
from pyspark.sql import SparkSession

from src.test.common.configs.spark_config import spark_conf_params
from src.test.common.constants import table_constants as tc


def create_spark_session(param_dict):
    """Create a Spark instance.
    :return: None
    """
    job_nm = param_dict[tc.JOB_NM]
    logging.basicConfig(
        format="%(asctime)s %(filename)s %(funcName)s %(lineno)d %(message)s",
        level=logging.INFO
    )
    spark_logger = logging.getLogger(job_nm)
    try:
        sp_conf = SparkConf().setAll(spark_conf_params)
        print('##### Creating the Spark Session object #####')
        spark_builder = (SparkSession
                         .builder
                         .config(conf=sp_conf)
                         .appName(job_nm))

        spark_session = spark_builder.enableHiveSupport().getOrCreate()
        spark_session.sparkContext.setLogLevel("WARN")
        return spark_session, spark_logger
    except ImportError as ie:
        spark_logger.error(ie)
        sys.exit(1)

    except Exception as e:
        spark_logger.error(e)
        print("Exception occurred while creating spark session")
        sys.exit(1)