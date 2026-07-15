# ==========================================================
# PROGRAM 8
# Sentiment Analysis using Spark RDD
#==========================================================

from pyspark.sql import SparkSession

# ----------------------------------------------------------
# Create Spark Session
# ----------------------------------------------------------
spark = SparkSession.builder \
    .appName("Sentiment Analysis") \
    .master("local[*]") \
    .getOrCreate()

sc = spark.sparkContext

# ----------------------------------------------------------
# Read CSV File
# ----------------------------------------------------------
rdd = sc.textFile("reviews.csv")

# Remove Header
header = rdd.first()
reviews = rdd.filter(lambda row: row != header)

# ----------------------------------------------------------
# Positive and Negative Words
# ----------------------------------------------------------
positive_words = {
    "love", "amazing", "excellent",
    "fantastic", "awesome", "good",
    "great", "best", "happy"
}

negative_words = {
    "hate", "bad", "terrible",
    "poor", "worst", "slow",
    "awful", "sad"
}

# ----------------------------------------------------------
# Sentiment Function
# ----------------------------------------------------------
def sentiment(review):

    words = review.lower().split()

    positive = sum(1 for word in words if word in positive_words)
    negative = sum(1 for word in words if word in negative_words)

    if positive > negative:
        return (review, "Positive")

    elif negative > positive:
        return (review, "Negative")

    else:
        return (review, "Neutral")

# ----------------------------------------------------------
# Perform Sentiment Analysis
# ----------------------------------------------------------
result = reviews.map(sentiment)

# ----------------------------------------------------------
# Display Results
# ----------------------------------------------------------
print("\nSentiment Analysis Results\n")

for review, label in result.collect():
    print(f"{review:<35} --> {label}")

# ----------------------------------------------------------
# Stop Spark
# ----------------------------------------------------------
spark.stop()
