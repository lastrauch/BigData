import pandas as pd

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
netflix_shows = pd.read_csv("datasets/netflix_titles.csv")

netflix_shows.drop(columns=['type', 'show_id', 'director', 'cast', 'country', 'release_year', 'rating', 'duration', 'listed_in', 'description'], inplace=True)
netflix_shows.columns = ['Title', 'Date Added']
print("Netflix_shows: ",netflix_shows.columns)
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
genre_imdb = pd.read_csv("datasets/genre.tsv", sep='\t')
genre_imdb.drop(columns=['primaryTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes'], inplace=True)

rating_imdb = pd.read_csv("datasets/title_ratings.tsv", sep='\t')
rating_imdb.drop(columns=['numVotes'], inplace=True)

imdb_combined = pd.merge(genre_imdb, rating_imdb, on='tconst', how='left')

imdb_combined.drop(columns=['tconst'], inplace=True)
imdb_combined.columns = ['Type', 'Title', 'Genre', 'Rating']

imdb_combined_series = imdb_combined

imdb_combined = imdb_combined[imdb_combined.Type == 'tvSeries']
imdb_combined = imdb_combined[imdb_combined.Type == 'movie']
imdb_combined_series = imdb_combined_series[imdb_combined_series.Type == 'tvSeries']
print("imdb: ", imdb_combined.columns)
# ======================================================================================================================

# ================== collection of top 50 trending Tv shows currently streaming on Netflix =============================
    # - Titles: It contains the name of the Top Tv shows.
    # - Year: Which Year this Tv Show Released?
    # - Netflix_Rating: Rated By Netflix.
    # - IMDB_Rating: Rated By IMDB.
    # - Netflix: Currently Streaming on Netflix or Not.
trending = pd.read_csv("datasets/trending.csv")

trending.drop(columns=['Year', 'Rating', 'IMDB_Rating', 'Netflix'], inplace=True)
trending.columns = ['Title']
print("trending: ",trending.columns)
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
original_netflix = pd.read_csv("datasets/netflix_originals.csv")
original_netflix.drop(columns=['Genre', 'Premiere', 'Seasons', 'Length', 'Status'], inplace=True)
print("original: ", original_netflix.columns)
for _ in range(len(original_netflix)):
    original.append(True)
original_netflix['Original'] = original
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
series = pd.read_csv("datasets/all_tv_shows.csv")
movies = pd.read_csv("datasets/MoviesOnStreamingPlatforms_updated.csv")
series_and_movies = series.append(movies)

series_and_movies.drop(columns=['Unnamed: 0', 'Year', 'Rotten Tomatoes', 'Type', 'type', 'ID', 'Directors', 'Genres', 'Country', 'Language', 'Runtime'], inplace=True)
print("both: ",series_and_movies.columns)
# ======================================================================================================================


collection = pd.merge(netflix_shows, series_and_movies, on='Title', how='outer')
collection_original = pd.merge(collection, original_netflix, on='Title', how='left')
netflix_collection_imdb = pd.merge(collection_original, imdb_combined, on='Title', how='left')
netflix_collection_imdb.to_csv('datasets/netflix_imdb_combined.csv', encoding='utf-8', index=False)

trending_netflix = pd.merge(trending, netflix_shows, on='Title', how='left')
trending_netflix_imdb = pd.merge(trending_netflix, imdb_combined_series, on='Title', how='left')
trending_netflix_imdb.to_csv('datasets/trending_netflix_imdb_combined.csv', encoding='utf-8', index=False)