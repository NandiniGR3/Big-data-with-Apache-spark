# ==========================================================
# PROGRAM 9
# Server Log & Web Traffic Trend Analysis using Spark RDD
#==========================================================

from pyspark.sql import SparkSession

# ----------------------------------------------------------
# Create Spark Session
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("Server Log & Web Traffic Analysis") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# ----------------------------------------------------------
# Read Server Log File
# ----------------------------------------------------------
logs = sc.textFile("server_log.txt")

# Split each log line
data = logs.map(lambda line: line.split())

# ==========================================================
# 1. Requests Per IP Address
# ==========================================================
ip_count = (
    data.map(lambda x: (x[2], 1))
        .reduceByKey(lambda a, b: a + b)
)

print("\n========== REQUESTS PER IP ==========")

for ip, count in ip_count.collect():
    print(f"{ip:<15} : {count}")

# ==========================================================
# 2. HTTP Methods
# ==========================================================
method_count = (
    data.map(lambda x: (x[3], 1))
        .reduceByKey(lambda a, b: a + b)
)

print("\n========== HTTP METHODS ==========")

for method, count in method_count.collect():
    print(f"{method:<5} : {count}")

# ==========================================================
# 3. Status Codes
# ==========================================================
status_count = (
    data.map(lambda x: (x[5], 1))
        .reduceByKey(lambda a, b: a + b)
)

print("\n========== STATUS CODES ==========")

for status, count in status_count.collect():
    print(f"{status} : {count}")

# ==========================================================
# 4. Most Visited Pages
# ==========================================================
page_count = (
    data.map(lambda x: (x[4], 1))
        .reduceByKey(lambda a, b: a + b)
)

print("\n========== PAGE VISITS ==========")

for page, count in page_count.collect():
    print(f"{page:<20} : {count}")

# ==========================================================
# 5. Web Traffic Trend (Requests Per Minute)
# ==========================================================
traffic_trend = (
    data.map(lambda x: (x[1][:5], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
)

print("\n========== WEB TRAFFIC TREND ==========")

for time, count in traffic_trend.collect():
    print(f"{time} --> {count} Requests")

# ----------------------------------------------------------
# Stop Spark
# ----------------------------------------------------------
spark.stop()
