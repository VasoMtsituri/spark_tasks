# 1.SparkSession Initialization: Write a script that initializes a SparkSession
from pyspark.sql import SparkSession

# This is the entry point to Spark functionality.
# 'appName' sets a name for your application shown in the Spark UI.
# 'getOrCreate()' gets an existing SparkSession or creates a new one.
spark = SparkSession.builder \
    .appName("Gemini tasks") \
    .getOrCreate()
# 1.1 Get sparkContext from SparkSession
sc = spark.sparkContext
# 2.Basic RDD Creation: Create an RDD from a Python list of integers.
spark_array = sc.parallelize([1, 2, 3, 4, 5])

# 3.Loading Text File into RDD: Load a plain text file
# (e.g., README.md from a Spark installation) into an RDD, where each line is an element.
readme_file = spark.read.text('README.md')

# 4. Counting Elements: Given an RDD, count the total number of elements it contains.
count_n = spark_array.count()

# 5. Filtering RDD: From an RDD of numbers, create a new RDD containing only the even numbers.
evens = spark_array.filter(lambda x: x % 2 == 0)

# 6. Mapping RDD: Given an RDD of strings, transform it into a new RDD
# where each element is the length of the original string.
readme_file_lens = readme_file.rdd.map(lambda x: len(x))

# 7. FlatMap RDD: Given an RDD of sentences, transform it into an RDD of individual words.
words = readme_file.rdd.flatMap(lambda x: x.value.split())

# 8. Union of RDDs: Combine two RDDs of numbers into a single RDD.
second_rdd = sc.parallelize([5, 7, 8, 9, 10])
united = spark_array.union(second_rdd)

# 9. Intersection of RDDs: Find the common elements between two RDDs of numbers.
intersection = spark_array.intersection(second_rdd)

# 10. Distinct Elements: From an RDD with duplicate values,
# create a new RDD containing only the unique elements.
new_rdd_numbers = sc.parallelize([5, 5, 8, 9, 9, 11, 12, 13, 1, 1, 2, 3, 8, 114])
unique_numbers = new_rdd_numbers.distinct().collect()

# 11. Grouping by Key: Given an RDD of key-value pairs (e.g., (word, count)),
# group the values by key.
pairs_rdd = sc.parallelize([(1, 2), (1, 3), (2, 2), (3, 4), (4, 5), (4, 10)])
grouped_pairs = pairs_rdd.groupByKey().collect()
grouped_pairs_as_list = pairs_rdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collect()

# 12. Reducing by Key: Given an RDD of key-value pairs where the values are numbers,
# sum the values for each key.
sum_of_numbers = pairs_rdd.reduceByKey(lambda x, y: x + y)

# 13.Word Count (RDD): Implement a classic word count program using
# RDD transformations and actions.
# 13.1 - Read file and split lines into words (flatMap)
words_rdd = readme_file.rdd.flatMap(lambda x: x.value.lower().split())
# 13.2 - Assign count of 1 to each word (map)
word_pairs_rdd = words_rdd.map(lambda x: (x, 1))
# 13.3 - Aggregate counts by word (reduceByKey)
word_counts_rdd = word_pairs_rdd.reduceByKey(lambda x, y: x + y)

# 14. Collecting Results: Take the first N elements from an RDD and print them to the console.
first_5_words = word_counts_rdd.take(5)
print(first_5_words)

# 15. Saving RDD to Text File: Save an RDD of strings to a text file.
word_counts_rdd.saveAsTextFile('word_counts.txt')
