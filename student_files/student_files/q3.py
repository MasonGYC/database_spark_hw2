import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# you may add more import if you need to

# hdfs dfs -put E:\hw2\student_files\student_files\data\TA_restaurants_curated_cleaned.csv /assignment2/part1/input/

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

# Read input data from HDFS
input_path = "hdfs:///assignment2/part1/input/TA_restaurants_curated_cleaned.csv" 
df = spark.read.csv(input_path, header=True, inferSchema=True)

# Calculate average rating per restaurant and city
avg_ratings = df.groupBy("City", "Restaurant").agg(avg("Rating").alias("AverageRating"))

# Find top and bottom 3 cities based on average rating
top_cities = avg_ratings.groupBy("City").avg("AverageRating").orderBy(col("avg(AverageRating)").desc()).limit(3)
bottom_cities = avg_ratings.groupBy("City").avg("AverageRating").orderBy(col("avg(AverageRating)").asc()).limit(3)

# Add 'RatingGroup' column to identify top and bottom cities
top_cities = top_cities.withColumn("RatingGroup", "Top")
bottom_cities = bottom_cities.withColumn("RatingGroup", "Bottom")

# Combine top and bottom cities
result = top_cities.union(bottom_cities).orderBy("City")

# Write output to HDFS as CSV
output_path = "hdfs://assignment2/output/question3".format(hdfs_nn)
result.write.csv(output_path, header=True, mode="overwrite")

# Stop Spark session
spark.stop()
