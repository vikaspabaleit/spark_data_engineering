# Question: Dealing with duplicate records is a common challenge in data processing. How do you efficiently handle  duplicates in your PySpark workflows?
import os
os.environ['PYSPARK_PYTHON'] = "python"
os.environ['PYSPARK_DRIVER_PYTHON'] = "python"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("FindDuplicates") \
    .getOrCreate()

# Sample data
data = [("Nikhil", "Laptop", 1500),
        ("Akash", "Phone", 800),
        ("Nikhil", "Laptop", 1500),
        ("Bob", "Tablet", 600),
        ("Akash", "Phone", 800),
        ("Dave", "Smartwatch", 300)]
# Create DataFrame
df = spark.createDataFrame(data, ["Customer", "Product", "Amount"])
# Find duplicates based on Customer and Product columns
duplicate_rows = df.groupBy("Customer", "Product").count().where(col("count") > 1)
# Show duplicate rows
#duplicate_rows.show()

from pyspark.sql import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("Customer", "Product").orderBy(col("Amount").desc())
filtered_df = df.withColumn("row_number", row_number().over(window_spec)) \
                .filter(col("row_number") == 1) \
                .drop("row_number")
#filtered_df.show()

df = spark.read.format("csv") \
    .option("header",True) \
    .option("InferSchema",True) \
    .load("D:\Projects\spark_data_engineering\src\main\spark\com\python\extractor\customers_raw.csv")

df.show()


