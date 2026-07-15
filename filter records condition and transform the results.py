# ==========================================================
# PROGRAM 4
# Filter and Transform CSV Dataset using Spark RDD
#==========================================================

from pyspark.sql import SparkSession

# Create Spark Session
spark = SparkSession.builder \
    .appName("Filter and Transform CSV") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# Read CSV file
rdd = sc.textFile("employees.csv")

# Remove header
header = rdd.first()
data = rdd.filter(lambda row: row != header)

# Split each row
records = data.map(lambda row: row.split(","))

# Filter employees
# Age >= 21 and Salary >= 40000
filtered = records.filter(
    lambda x: int(x[2]) >= 21 and int(x[3]) >= 40000
)

# Transform data (Increase Salary by 10%)
transformed = filtered.map(
    lambda x: (
        x[0],                    # ID
        x[1],                    # Name
        int(x[2]),               # Age
        int(x[3]),               # Old Salary
        round(int(x[3]) * 1.10, 2)  # New Salary
    )
)

# Display Result
print("\nFiltered Employees with Updated Salary\n")

for employee in transformed.collect():
    print(employee)

# Stop Spark
spark.stop()
