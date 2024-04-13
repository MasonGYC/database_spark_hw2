import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, from_json, explode

# you may add more import if you need to

# hdfs dfs -put ./student_files/data/TA_restaurants_curated_cleaned.csv /assignment2/part1/input/
# spark-submit q4.py localhost
# hadoop fs -ls -R / | grep "^d"
# hdfs dfs -cat /assignment2/output/question4/*

# don't change this line
hdfs_nn = sys.argv[1]
port = 9000

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

# Read input 
input_path = f"hdfs://{hdfs_nn}:{port}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

df1 = df.withColumn("Cuisine", from_json("Cuisine Style", 'array<string>'))
df2 = df1.select("City",explode("Cuisine").alias("Cuisine"))
df2.printSchema()
df2.show()
result = df2.groupBy("City", "Cuisine").agg(count("*").alias("count"))

# Write output to HDFS as CSV
output_path = f"hdfs://{hdfs_nn}:{port}/assignment2/output/question4/"
result.coalesce(1).write.mode('overwrite').option('header','true').csv(output_path)

# Stop Spark session
spark.stop()
