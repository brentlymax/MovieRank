# Movie rankings using Spark.

import os
import shutil
from pyspark import SparkConf, SparkContext
from pyspark.shell import sqlContext
from pyspark.sql import Row, Column
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StringType, StructType


class MovieRank:
	# Constructor.
	def __init__(self, movies_path, reviews_path, output_path):
		# Create SparkContext object.
		conf = SparkConf().setMaster("local").setAppName("Movie Rank")
		self.sc = SparkContext.getOrCreate(conf=conf)

		# Check and set path variables.
		self.movies_path = movies_path
		self.reviews_path = reviews_path
		self.output_path = output_path

	# Spark job using RDD.
	def rdd_job(self):
		movies_rdd = self.sc.textFile(self.movies_path) \
			.filter(lambda row: row != 'movieId,title,genres') \
			.map(lambda row: (row.split(",")[0], row.split(",")[1]))
		reviews_rdd = self.sc.textFile(self.reviews_path) \
			.filter(lambda row: row != 'userId,movieId,rating,timestamp') \
			.map(lambda row: (row.split(",")[1], 1)) \
			.reduceByKey(lambda a, b: a + b) \
			.takeOrdered(10, lambda x: -x[1])
		reviews = self.sc.parallelize(reviews_rdd)

		# Check if output directory exists, if so delete.
		if os.path.exists(self.output_path) and os.path.isdir(self.output_path):
			shutil.rmtree(self.output_path)

		res_rdd = reviews.join(movies_rdd).map(lambda x: x[1])
		res_rdd.coalesce(1).saveAsTextFile(self.output_path)

	# Spark job using DataFrame.
	def data_frame_job(self, movies_path, reviews_path):
		pass

	# Spark job using DataSet.
	def data_set_job(self, movies_path, reviews_path):
		pass


# Driver code for testing class.
if __name__ == "__main__":
	pass
