# Movie rankings using Spark.

import os
import shutil
from pyspark.sql import functions as func
from pyspark import SparkConf, SparkContext, SQLContext


class MovieRank:
	# Constructor.
	def __init__(self, movies_path, reviews_path, output_path):
		# Create SparkContext object.
		conf = SparkConf().setMaster("local[8]").setAppName("Movie Rank").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
		self.sc = SparkContext.getOrCreate(conf=conf)
		# Check and set path variables.
		self.movies_path = movies_path
		self.reviews_path = reviews_path
		self.output_path = output_path

	# Spark job using RDD to find the 10 movies with highest number of reviews.
	def rdd_num_reviews(self):
		# Create and filter RDDs.
		movies_rdd = self.sc.textFile(self.movies_path) \
			.filter(lambda row: row != 'movieId,title,genres') \
			.map(lambda row: (row.split(",")[0], row.split(",")[1]))
		reviews_rdd = self.sc.textFile(self.reviews_path) \
			.filter(lambda row: row != 'userId,movieId,rating,timestamp') \
			.map(lambda row: (row.split(",")[1], 1)) \
			.reduceByKey(lambda a, b: a + b) \
			.takeOrdered(10, lambda x: -x[1])
		# Check if output directory exists, if so delete.
		if os.path.exists(self.output_path) and os.path.isdir(self.output_path):
			shutil.rmtree(self.output_path)
		# Join RDDs.
		reviews_rdd_para = self.sc.parallelize(reviews_rdd)
		res_rdd = reviews_rdd_para.join(movies_rdd).map(lambda x: x[1])
		# Write output to CSV or display to console.
		res_rdd.coalesce(1).saveAsTextFile(self.output_path)
		# res_rdd.show()

	# Spark job using RDD to find average reviews over 4 stars.
	def rdd_avg_reviews(self):
		# Create and filter RDDs.
		movies_rdd = self.sc.textFile(self.movies_path) \
			.filter(lambda row:row != 'movieId,title,genres') \
			.map(lambda row : ( row.split(",")[0], row.split(",")[1]))
		reviews_rdd = self.sc.textFile(self.reviews_path) \
			.filter(lambda row:row != 'userId,movieId,rating,timestamp') \
			.map(lambda row : [row.split(",")[1], (1, float(row.split(",")[2]))]) \
			.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) \
			.filter(lambda x: x[1][0]>10 ) \
			.mapValues(lambda x:  x[1]/x[0]) \
			.filter(lambda x: x[1]>4)
		# Join RDDs.
		res_rdd = reviews_rdd.join(movies_rdd).map(lambda x: [x[1][1], x[1][0]])
		# Write output to CSV or display to console.
		res_rdd.coalesce(1).saveAsTextFile(self.output_path)
		# res_rdd.show()

	# Spark job using DataFrame to find the 10 movies with highest number of reviews.
	def dataframe_num_reviews(self):
		# Create SQLContext
		sql_context = SQLContext(self.sc)
		# Create and filter DataFrames.
		movies_df = sql_context.read.option("header", "true").csv(self.movies_path)
		reviews_df = sql_context.read.option("header", "true").csv(self.reviews_path) \
			.groupBy("movieId") \
			.agg(func.count('rating')) \
			.select(func.col("movieId").alias("movieId"), func.col("count(rating)").alias("num_ratings")) \
			.sort(func.desc("num_ratings")) \
			.limit(10)
		# Join DataFrames.
		res_df = reviews_df.join(movies_df, reviews_df.movieId == movies_df.movieId) \
			.select(func.col("num_ratings"), func.col("title"))
		# Write output to CSV or display to console.
		res_df.coalesce(1).write.format("csv").save(self.output_path)
		# res_df.show()

	# Spark job using DataFrame to find average reviews over 4 stars.
	def dataframe_avg_reviews(self):
		# Create SQLContext
		sql_context = SQLContext(self.sc)
		# Create and filter DataFrames.
		movies_df = sql_context.read.option("header", "true").csv(self.movies_path)
		reviews_avg_df = sql_context.read.option("header", "true").csv(self.reviews_path) \
			.groupBy("movieId") \
			.agg(func.avg("rating")) \
			.select(func.col("movieId").alias("movieId_avg"), func.col("avg(rating)").alias("avg_ratings")) \
			.filter(func.col("avg_ratings") >= 4)
		reviews_num_df = sql_context.read.option("header", "true").csv(self.reviews_path) \
			.groupBy("movieId") \
			.agg(func.count("rating")) \
			.select(func.col("movieId").alias("movieId_num"), func.col("count(rating)").alias("num_ratings")) \
			.filter(func.col("num_ratings") >= 10)
		# Join both review DataFrames.
		reviews_final_df = reviews_avg_df.join(reviews_num_df, reviews_avg_df.movieId_avg == reviews_num_df.movieId_num) \
			.select(func.col("movieId_avg").alias("movieId"), func.col("avg_ratings"))
		# Join movie and final review DataFrames.
		res_df = reviews_final_df.join(movies_df, reviews_final_df.movieId == movies_df.movieId) \
			.select(func.col("avg_ratings"), func.col("title"))
		# Write output to CSV or display to console.
		res_df.coalesce(1).write.format("csv").save(self.output_path)
		# res_df.show()


# Driver code for testing class.
if __name__ == "__main__":
	pass
