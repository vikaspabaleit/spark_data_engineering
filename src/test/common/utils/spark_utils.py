import calendar,time
from datetime import date,datetime,timedelta

from pyspark.sql import functions as f

def get_current_timestamp():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return current_time

def stop_context(spark):
    if not (spark._sc._jsc.sc().isStopped()):
        spark.stop()


def read_from_hive(spark, src_customer_sales_tbl, env):
    if env == "dev":
        df = spark.read.option("header", "true").csv(src_customer_sales_tbl)
    elif env == "prod":
        df = spark.read.option("header", "true").csv(src_customer_sales_tbl)
    else:
        df = spark.read.format("csv") \
            .option("header", True) \
            .option("InferSchema", True) \
            .load(src_customer_sales_tbl)

    return df