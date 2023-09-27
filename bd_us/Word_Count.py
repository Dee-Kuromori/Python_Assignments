# Import necessary libraries
# Set the PYSPARK_PYTHON environment variable
import os
from pyspark.sql import SparkSession
python_executable = r"C:\Users\Consultant\AppData\Local\Programs\Python\Python311\python.exe"
os.environ["PYSPARK_PYTHON"] = python_executable


# Create a Spark session
spark = SparkSession.builder.appName("Word_Count").getOrCreate()

# Read a text file
text_file = spark.read.text("C:\\Users\\Consultant\\Documents\\Assignments_training_bd\\training_notes\\week-9_18-9_22\\student.txt")

# Split lines into words
words = text_file.rdd.flatMap(lambda line: line[0].split(" "))

# Map each word to a (word, 1) tuple
word_tuples = words.map(lambda word: (word, 1))

# Reduce by key to count occurrences of each word
word_counts = word_tuples.reduceByKey(lambda a, b: a + b)

# Display the results
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Stop the Spark session
spark.stop()