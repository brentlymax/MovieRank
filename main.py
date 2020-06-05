# Main function.

from movie_rank import MovieRank
import time


def main():
	movies_path = "C:/Programming/Workspace/Python/MovieRank/data/movies.csv"
	reviews_path = "C:/Programming/Workspace/Python/MovieRank/data/reviews.csv"
	output_path = "C:/Programming/Workspace/Python/MovieRank/data/output/"

	movie_rank = MovieRank(movies_path, reviews_path, output_path)
	movie_rank.rdd_job()


if __name__ == "__main__":
	main()
