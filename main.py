# Main function.

from movie_rank import MovieRank
from os import path
import shutil
import time


def main():
	# Set CSV paths and output path.
	movies_path = "C:/Programming/Workspace/Python/MovieRank/data/movies.csv"
	reviews_path = "C:/Programming/Workspace/Python/MovieRank/data/reviews.csv"
	output_path = "C:/Programming/Workspace/Python/MovieRank/output"

	# If output directory exists, delete it.
	if path.exists(output_path):
		shutil.rmtree(output_path)

	# Create MovieRank object and call a job function.
	movie_rank = MovieRank(movies_path, reviews_path, output_path)
	# movie_rank.rdd_num_reviews()
	# movie_rank.rdd_avg_reviews()
	# movie_rank.dataframe_num_reviews()
	# movie_rank.dataframe_avg_reviews()


if __name__ == "__main__":
	main()
