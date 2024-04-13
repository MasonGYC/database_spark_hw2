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

# Read input data from HDFS
input_path = f"hdfs://{hdfs_nn}:{port}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Calculate average rating per restaurant and city
avg_ratings = df.groupBy("City", "Name").agg(avg("Rating").alias("AverageRating"))

# Find top and bottom 3 cities based on average rating
top_cities = avg_ratings.groupBy("City").avg("AverageRating").orderBy(col("avg(AverageRating)").desc()).limit(3)
bottom_cities = avg_ratings.groupBy("City").avg("AverageRating").orderBy(col("avg(AverageRating)").asc()).limit(3)

# Add 'RatingGroup' column to identify top and bottom cities
top_cities = top_cities.withColumn("RatingGroup", lit("Top"))
bottom_cities = bottom_cities.withColumn("RatingGroup", lit("Bottom"))

# Combine top and bottom cities
result = top_cities.union(bottom_cities).withColumnRenamed("avg(AverageRating)", "AverageRating").orderBy(col("AverageRating").desc())

# Write output to HDFS as CSV
output_path = f"hdfs://{hdfs_nn}:{port}/assignment2/output/question3/"
result.coalesce(1).write.mode('overwrite').option('header','true').csv(output_path)

# Stop Spark session
spark.stop()