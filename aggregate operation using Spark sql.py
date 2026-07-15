# ==========================================================
# PROGRAM 6
# Aggregate Operations using Spark DataFrame
#=========================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# ----------------------------------------------------------
# Create Spark Session
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("Aggregate Operations") \
    .master("local[*]") \
    .getOrCreate()

# ----------------------------------------------------------
# Read CSV File
# ----------------------------------------------------------
df = spark.read.csv(
    "employees.csv",
    header=True,
    inferSchema=True
)

print("\nEmployee Dataset")
df.show()

# ----------------------------------------------------------
# Aggregate Functions
# ----------------------------------------------------------

print("\nTotal Number of Employees")
df.select(count("ID").alias("Total Employees")).show()

print("\nTotal Salary")
df.select(sum("Salary").alias("Total Salary")).show()

print("\nAverage Salary")
df.select(avg("Salary").alias("Average Salary")).show()

print("\nMaximum Salary")
df.select(max("Salary").alias("Maximum Salary")).show()

print("\nMinimum Salary")
df.select(min("Salary").alias("Minimum Salary")).show()

# ----------------------------------------------------------
# Department-wise Aggregate
# ----------------------------------------------------------

print("\nDepartment-wise Salary Statistics")

df.groupBy("Department").agg(
    count("*").alias("Employees"),
    sum("Salary").alias("Total Salary"),
    avg("Salary").alias("Average Salary"),
    max("Salary").alias("Highest Salary"),
    min("Salary").alias("Lowest Salary")
).show()

# ----------------------------------------------------------
# Stop Spark
# ----------------------------------------------------------
spark.stop()
