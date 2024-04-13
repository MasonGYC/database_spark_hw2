import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit

# you may add more import if you need to

# hdfs dfs -put ./student_files/data/TA_restaurants_curated_cleaned.csv /assignment2/part1/input/
# spark-submit q3.py localhost
# hadoop fs -ls -R / | grep "^d"
# hdfs dfs -cat /assignment2/output/question3/*

# don't change this line
hdfs_nn = sys.argv[1]
port = 9000

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

input_path = f"hdfs://{hdfs_nn}:{port}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

avg_ratings = df.groupBy("City", "Name").agg(avg("Rating").alias("AverageRating"))

top_cities = avg_ratings.groupBy("City").avg("AverageRating").orderBy(col("avg(AverageRating)").desc()).limit(3)
bottom_cities = avg_ratings.groupBy("City").avg("AverageRating").orderBy(col("avg(AverageRating)").asc()).limit(3)

top_cities = top_cities.withColumn("RatingGroup", lit("Top"))
bottom_cities = bottom_cities.withColumn("RatingGroup", lit("Bottom"))

result = top_cities.union(bottom_cities).withColumnRenamed("avg(AverageRating)", "AverageRating").orderBy(col("AverageRating").desc())

output_path = f"hdfs://{hdfs_nn}:{port}/assignment2/output/question3/"
result.coalesce(1).write.mode('overwrite').option('header','true').csv(output_path)

spark.stop()