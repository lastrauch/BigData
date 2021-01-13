import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

confCluster = SparkConf().setAppName("NetflixAnalysis")
sc = SparkContext(conf=confCluster)
sqlContext = sql.SQLContext(sc)
sc.setLogLevel("ERROR")

# ======================== tv shows and movies available on Netflix as of 2019 =========================================
    # - show_id
    # - type
    # - title
    # - director
    # - cast
    # - country
    # - data_added
    # - release_year
    # - rating
    # - duration
netflix_shows = sqlContext.read.option("header",True).csv("netflix_titles.csv")

col_drop_netflix = ['type', 'show_id', 'director', 'cast', 'country', 'release_year', 'rating', 'duration', 'listed_in', 'description']
netflix_shows = netflix_shows.drop(*col_drop_netflix)
netflix_shows = netflix_shows.selectExpr("title as Title", "date_added as Date_Added")
print("Netflix_shows:")
netflix_shows.printSchema()
# ======================================================================================================================

# ======================== IMDb Dataset including up to three genres associated with the title =========================
    # - tconst (string): Alphanumeric unique identifier of the title.
    # - titleType (string): The type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc).
    # - primaryTitle (string): The more popular title / the title used by the filmmakers on promotional materials at the point of release.
    # - originalTitle (string): Original title, in the original language.
    # - isAdult (boolean): 0 - non-adult title; 1 - adult title.
    # - startYear (YYYY): Represents the release year of a title. In the case of TV Series, it is the series start year.
    # - endYear (YYYY): TV Series end year. for all other title types.
    # - runtimeMinutes: Primary runtime of the title, in minutes.
    # - genres (string array): Includes up to three genres associated with the title.
    # - averageRating: weighted average of all the individual user ratings.
genre_imdb = sqlContext.read.option("header",True).option("sep", "\t").csv("genre.tsv")

rating_imdb = sqlContext.read.option("header",True).option("sep", "\t").csv("title_ratings.tsv")
col_drop_rating_imdb = ['numVotes']

imdb_combined = genre_imdb.join(rating_imdb, genre_imdb.tconst == rating_imdb.tconst, how='left')

col_drop_imdb_combined = ['tconst', 'primaryTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes', 'numVotes']
imdb_combined = imdb_combined.drop(*col_drop_imdb_combined)
imdb_combined = imdb_combined.selectExpr("titleType as Type", "originalTitle as Title", "genres as Genre", "averageRating as Rating")

imdb_combined_series = imdb_combined

imdb_combined = imdb_combined.filter("Type == 'tvSeries' or Type == 'movie'")
imdb_combined_series = imdb_combined_series.where(imdb_combined_series.Type == 'tvSeries')
print("imdb_combined:")
imdb_combined.printSchema()
# ======================================================================================================================

# ================== collection of top 50 trending Tv shows currently streaming on Netflix =============================
    # - Titles: It contains the name of the Top Tv shows.
    # - Year: Which Year this Tv Show Released?
    # - Netflix_Rating: Rated By Netflix.
    # - IMDB_Rating: Rated By IMDB.
    # - Netflix: Currently Streaming on Netflix or Not.
trending = sqlContext.read.option("header",True).csv("trending.csv")

drop_col_trending = ['Year', 'Rating', 'IMDB_Rating', 'Netflix']
trending = trending.drop(*drop_col_trending)
trending = trending.selectExpr("Titles as Title")
# ======================================================================================================================

# ===================================== Original Movies and Shows created by Netflix ===================================
    # - Title:
    # - Genre:
    # - Original Network:
    # - Premiere:
    # - Seasons:
    # - Length:
    # - Netflix Exclusive Regions:
    # - Status: Abgeschlossen oder nicht.
original = []
original_netflix = sqlContext.read.option("header",True).csv("netflix_originals.csv")

drop_col_original = ['Genre', 'Premiere', 'Seasons', 'Length', 'Status']
original_netflix = original_netflix.drop(*drop_col_original)
print("Original:")
original_netflix.printSchema()
original_netflix = original_netflix.withColumn('Original', lit(True))
# ======================================================================================================================

# =========================== A collection of tv shows found on these streaming platforms ==============================
    # - Title:
    # - Year: The year in which the movie was produced
    # - Age: Target age group
    # - IMDb: IMDb rating
    # - Rotten Tomatoes: Rotten Tomatoes %
    # - Netflix: Whether the movie is found on Netflix (0 or 1)
    # - Hulu: Whether the movie is found on Hulu
    # - Prime Video: Whether the movie is found on Prime Video
    # - Disney+: Whether the tv show is found on Disney+
series = sqlContext.read.option("header",True).csv("all_tv_shows.csv")
movies = sqlContext.read.option("header",True).csv("MoviesOnStreamingPlatforms_updated.csv")
drop_col_movies = ['ID', 'Directors', 'Genres', 'Country', 'Language', 'Runtime']
movies =movies.drop(*drop_col_movies)
series_and_movies = series.union(movies)

drop_col_series_and_movies = ['_c0', 'Year', 'IMDb', 'Rotten Tomatoes', 'Type', 'type', 'ID', 'Directors', 'Genres', 'Country', 'Language', 'Runtime']
series_and_movies = series_and_movies.drop(*drop_col_series_and_movies)
print("series_and_movies")
series_and_movies.printSchema()
# ======================================================================================================================
collection = netflix_shows.join(series_and_movies, 'Title', 'full')
print("collection:")
collection.printSchema()

collection_original = collection.join(original_netflix, 'Title', 'left')
print("collection_original:")
collection_original.printSchema()

netflix_collection_imdb = collection_original.join(imdb_combined, 'Title', 'left')
print("netflix_collection_imdb:")
netflix_collection_imdb.printSchema()

netflix_collection_imdb.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets/rdd_netflix_imdb_combined.csv', encoding='utf-8', index=False)

trending_netflix = trending.join(netflix_shows, 'Title', 'left')
trending_netflix_imdb = trending_netflix.join(imdb_combined_series, 'Title', 'left')
trending_netflix_imdb.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets/rdd_trending_netflix_imdb_combined.csv', encoding='utf-8', index=False)