import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
# you may add more import if you need to

# hdfs dfs -put ./student_files/data/TA_restaurants_curated_cleaned.csv /assignment2/part1/input/
# spark-submit q1.py localhost
# hadoop fs -ls -R / | grep "^d"
# hdfs dfs -cat /assignment2/output/question1/*

# don't change this line
hdfs_nn = sys.argv[1]
port = 9000

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

input_path = f"hdfs://{hdfs_nn}:{port}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

result = df.filter((col("Number of Reviews").isNotNull()) & (col("Rating") >= 1.0))

output_path = f"hdfs://{hdfs_nn}:{port}/assignment2/output/question1/"
result.coalesce(1).write.mode('overwrite').option('header','true').csv(output_path)

spark.stop()