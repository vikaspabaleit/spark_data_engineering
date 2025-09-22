import calendar,time
from datetime import date,datetime,timedelta

from pyspark.sql import functions as f

def get_current_timestamp():
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return current_time

def stop_context(spark):
    if not (spark._sc._jsc.sc().isStopped()):
        spark.stop()


def extract_from_hive(spark, db_nm: str, tbl_nm: str, env: str, filter_condition: str = None):
    if env in ["dev", "prod"]:
        query = f"SELECT * FROM {db_nm}.{tbl_nm}"
        if filter_condition:
            query += f" WHERE {filter_condition}"
        df = spark.sql(query)
    else:
        # Local/other env fallback to CSV
        df = spark.read.format("csv") \
            .option("header", True) \
            .option("inferSchema", True) \
            .load("D:\\Projects\\spark_data_engineering\\src\\test\\dummydata\\inputdata\\customers_raw.csv")
        if filter_condition:
            df = df.filter(filter_condition)

    return df