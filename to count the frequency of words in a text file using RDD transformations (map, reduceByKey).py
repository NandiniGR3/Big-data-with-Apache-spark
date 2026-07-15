import re
import nltk
from nltk.corpus import stopwords
from pyspark.sql import SparkSession

# Download stopwords (only once)
nltk.download("stopwords")

# Create Spark Session
spark = (
    SparkSession.builder
    .appName("Word Frequency")
    .master("local[*]")
    .getOrCreate()
)

sc = spark.sparkContext

# Load external text file
rdd = sc.textFile("sample.txt")

# English stopwords
stop_words = set(stopwords.words("english"))

# Text preprocessing
words = (
    rdd.map(lambda line: line.lower())
       .map(lambda line: re.sub(r"[^a-zA-Z\s]", "", line))
       .flatMap(lambda line: line.split())
       .filter(lambda word: word not in stop_words and word != "")
)

# Count word frequency
word_count = (
    words.map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)
         .sortBy(lambda x: x[1], ascending=False)
)

# Display output
print("\nWord Frequency:\n")
for word, count in word_count.collect():
    print(f"{word} : {count}")

# Stop Spark
spark.stop()
