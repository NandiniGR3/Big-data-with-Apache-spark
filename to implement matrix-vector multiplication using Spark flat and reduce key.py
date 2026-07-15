# ==========================================================
# PROGRAM 3
# Matrix-Vector Multiplication using Spark RDD
#==========================================================

from pyspark.sql import SparkSession

# ----------------------------------------------------------
# Create Spark Session
# ----------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Matrix Vector Multiplication")
    .master("local[*]")
    .getOrCreate()
)

sc = spark.sparkContext

# ----------------------------------------------------------
# Input Matrix and Vector
# ----------------------------------------------------------

matrix = [
    (0, [1, 2, 3]),
    (1, [4, 5, 6]),
    (2, [7, 8, 9])
]

vector = [1, 1, 1]

# ----------------------------------------------------------
# Create RDD
# ----------------------------------------------------------

matrix_rdd = sc.parallelize(matrix)

# ----------------------------------------------------------
# Data Validation
# ----------------------------------------------------------

matrix_rdd = matrix_rdd.filter(
    lambda row: len(row[1]) == len(vector)
)

matrix_rdd = matrix_rdd.filter(
    lambda row: all(isinstance(x, (int, float)) for x in row[1])
)

# ----------------------------------------------------------
# Matrix-Vector Multiplication
# ----------------------------------------------------------

result = (
    matrix_rdd
    .flatMap(
        lambda row: [
            (row[0], row[1][i] * vector[i])
            for i in range(len(vector))
        ]
    )
    .reduceByKey(lambda a, b: a + b)
)

# ----------------------------------------------------------
# Display Result
# ----------------------------------------------------------

print("\n========== MATRIX ==========")
for row in matrix:
    print(row[1])

print("\nVector")
print(vector)

print("\n========== RESULT ==========")

for row, value in result.collect():
    print(f"Row {row} = {value}")

# ----------------------------------------------------------
# Stop Spark
# ----------------------------------------------------------

spark.stop()
