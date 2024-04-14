import sys 
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import from_json, col, explode, array, array_sort, count
# you may add more import if you need to

# hdfs dfs -put ./student_files/data/tmdb_5000_credits.parquet /assignment2/part2/input/
# spark-submit q5.py localhost
# hadoop fs -ls -R / | grep "^d"
# hdfs dfs -cat /assignment2/output/question5/*

# don't change this line
hdfs_nn = sys.argv[1]
port = 9000

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

input_path = f"hdfs://{hdfs_nn}:{port}/assignment2/part2/input/tmdb_5000_credits.parquet"

df = spark.read.option("header",True).parquet(input_path).drop("crew")

df = df.withColumn("actor1", explode(from_json(col("cast"), ArrayType(StructType([StructField("name", StringType(), False)]))).getField("name")))
df = df.withColumn("actor2", explode(from_json(col("cast"), ArrayType(StructType([StructField("name", StringType(), False)]))).getField("name")))

df = df.select("movie_id", "title", "actor1", "actor2").filter(col("actor1") != col("actor2"))

df = df.withColumn("co-cast", array(col("actor1"), col("actor2")))
df = df.withColumn("co-cast", array_sort(col("co-cast")).cast("string"))


df = df.dropDuplicates(["movie_id", "title", "co-cast"]).sort(col("co-cast").desc())

df1 = df.groupBy("co-cast").agg(count("*").alias("cnt")).filter(col("cnt") >= 2).sort(col("co-cast").desc())

result = df.join(df1, ["co-cast"], "inner").drop("co-cast", "cnt")
result.show()

# Write the result to Parquet files
output_path = f"hdfs://{hdfs_nn}:{port}/assignment2/output/question5/"
result.write.parquet(output_path)

# Stop the SparkSession
spark.stop()