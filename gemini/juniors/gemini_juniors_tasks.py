# 1.SparkSession Initialization: Write a script that initializes a SparkSession
from pyspark.sql import SparkSession

# This is the entry point to Spark functionality.
# 'appName' sets a name for your application shown in the Spark UI.
# 'getOrCreate()' gets an existing SparkSession or creates a new one.
spark = SparkSession.builder \
    .appName("Gemini tasks") \
    .getOrCreate()

# 2.Basic RDD Creation: Create an RDD from a Python list of integers.
spark_array = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# 3.Loading Text File into RDD: Load a plain text file
# (e.g., README.md from a Spark installation) into an RDD, where each line is an element.
readme_file = spark.sparkContext.textFile('README.md')

# 4. Counting Elements: Given an RDD, count the total number of elements it contains.
count_n = spark_array.count()

# 5. Filtering RDD: From an RDD of numbers, create a new RDD containing only the even numbers.
evens = spark_array.filter(lambda x: x % 2 == 0)

# 6. Mapping RDD: Given an RDD of strings, transform it into a new RDD
# where each element is the length of the original string.
readme_file_lens = readme_file.map(lambda x: len(x))

# 7. FlatMap RDD: Given an RDD of sentences, transform it into an RDD of individual words.
words = readme_file.flatMap(lambda x: x.split())

# 8. Union of RDDs: Combine two RDDs of numbers into a single RDD.
second_rdd = spark.sparkContext.parallelize([5, 7, 8, 9, 10])
united = spark_array.union(second_rdd)

# 9. Intersection of RDDs: Find the common elements between two RDDs of numbers.
intersection = spark_array.intersection(second_rdd)

# 10. Distinct Elements: From an RDD with duplicate values,
# create a new RDD containing only the unique elements.
new_rdd_numbers = spark.sparkContext.parallelize([5, 5, 8, 9, 9, 11, 12, 13, 1, 1, 2, 3, 8, 114])
unique_numbers = new_rdd_numbers.distinct().collect()
