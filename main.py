# Main function.

from movie_rank import MovieRank
import time


def main():
	movies_path = "C:/Programming/Workspace/Python/MovieRank/data/movies.csv"
	reviews_path = "C:/Programming/Workspace/Python/MovieRank/data/reviews.csv"
	output_path = "C:/Programming/Workspace/Python/MovieRank/data/output/"

	movie_rank = MovieRank(movies_path, reviews_path, output_path)
	#movie_rank.rdd_num_reviews()
	#movie_rank.rdd_avg_reviews()
	#movie_rank.dataframe_num_reviews()
	#movie_rank.dataframe_avg_reviews()
	#movie_rank.dataset_num_reviews()
	#movie_rank.dataset_avg_reviews()


if __name__ == "__main__":
	main()
