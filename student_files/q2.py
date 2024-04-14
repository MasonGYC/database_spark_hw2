import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min
# you may add more import if you need to


# hdfs dfs -put ./student_files/data/TA_restaurants_curated_cleaned.csv /assignment2/part1/input/
# spark-submit q2.py localhost
# hadoop fs -ls -R / | grep "^d"
# hdfs dfs -cat /assignment2/output/question2/*
# "_c0", "Name", "City", "Cuisine Style","Ranking", "Rating","Price Range","Number of Reviews","Reviews" ,"URL_TA","ID_TA"

# don't change this line
hdfs_nn = sys.argv[1]
port = 9000

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

input_path = f"hdfs://{hdfs_nn}:{port}/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

df_filtered = df.filter(col("Price Range").isNotNull())

df1 = df_filtered.groupBy("City", "Price Range").agg(max(col("Rating")).alias("Best Rating"), min(col("Rating")).alias("Worst Rating"))
df1.show()

best_res = df.join(df1, (df1["City"] == df_filtered["City"]) &
                                             (df1["Price Range"] == df_filtered["Price Range"]) &
                                             (df1["Best Rating"] == df_filtered["Rating"]), "leftsemi") \
                                                  .dropDuplicates()
worst_res = df.join(df1, (df1["City"] == df_filtered["City"]) &
                                             (df1["Price Range"] == df_filtered["Price Range"]) &
                                             (df1["Worst Rating"] == df_filtered["Rating"]), "leftsemi") \
                                                  .dropDuplicates()

result = best_res.union(worst_res).dropDuplicates().orderBy(col("City").asc())
result.show()
output_path = f"hdfs://{hdfs_nn}:{port}/assignment2/output/question2/"
result.coalesce(1).write.mode('overwrite').option('header','true').csv(output_path)

spark.stop()