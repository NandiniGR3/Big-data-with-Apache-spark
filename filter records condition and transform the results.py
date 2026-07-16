from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark Session
spark = SparkSession.builder \
    .appName("HR Filter Transformation") \
    .master("local[*]") \
    .getOrCreate()

# Load CSV Dataset
df = spark.read.csv(
    "WA_Fn-UseC_-HR-Employee-Attrition.csv",
    header=True,
    inferSchema=True
)

# Display Dataset
df.show()

# Filter employees with Age > 30
filtered_df = df.filter(col("Age") > 30)

# Transform MonthlyIncome (Increase by 10%)
transformed_df = filtered_df.withColumn(
    "UpdatedSalary",
    col("MonthlyIncome") * 1.10
)

# Display Result
transformed_df.select(
    "Age",
    "Department",
    "JobRole",
    "MonthlyIncome",
    "UpdatedSalary"
).show()

# Stop Spark
spark.stop()