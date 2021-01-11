import pandas as pd
import math

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
all = pd.read_csv("datasets/netflix_titles.csv")
years_preCorona = []
years_2020 = []
years = []
all_type = []
all_pg = []
all_title = []
all_added = []
for year, typ, title, rating, added in zip(all['release_year'],all['type'], all['title'], all['rating'], all['date_added']):
    all_title.append(title)
    years.append(year)
    all_type.append(typ)
    all_pg.append(rating)
    if pd.isnull(added):
        all_added.append("-")
    else:
        if "," in str(added):
            x = str(added).split(", ")
            all_added.append(x[1])
        else:
            all_added.append(added)

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
rating_imdb = pd.read_csv("datasets/title_ratings.tsv", sep='\t')

#join 'genre.tsv' and 'title_ratings.tsv' at column 'tconst'
imdb_combined = pd.concat([genre_imdb.set_index('tconst'),rating_imdb.set_index('tconst')], axis=1, join='inner')


genre_title = {}
ratings = {}
for title, genre, rating in zip(imdb_combined['originalTitle'], imdb_combined['genres'], imdb_combined['averageRating']):
    genre_title[title] = genre
    ratings[title] = rating

# ======================================================================================================================

# ================== collection of top 50 trending Tv shows currently streaming on Netflix =============================
    # - Titles: It contains the name of the Top Tv shows.
    # - Year: Which Year this Tv Show Released?
    # - Netflix_Rating: Rated By Netflix.
    # - IMDB_Rating: Rated By IMDB.
    # - Netflix: Currently Streaming on Netflix or Not.
trending = pd.read_csv("datasets/trending.csv")

trending_title = []
trending_year = []
trending_rating = []
for title, year, rating in zip(trending['Titles'], trending['Year'], trending['IMDB_Rating']):
    trending_title.append(title)
    trending_year.append(year)
    trending_rating.append(rating)
# ======================================================================================================================

# ===================================== Original Movies and Show created by Netflix ====================================
    # - Title:
    # - Genre:
    # - Original Network:
    # - Premiere:
    # - Seasons:
    # - Length:
    # - Netflix Exclusive Regions:
    # - Status: Abgeschlossen oder nicht.
original_title = []
original_region = []
test = {}
original = pd.read_csv("datasets/netflix_originals.csv")
for title, region in zip(original['Title'], original['Netflix Exclusive Regions']):
    test[title] = region
    original_title.append(title)
    original_region.append(region)
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
tv_shows = pd.read_csv("datasets/all_tv_shows.csv")

netflixs = {}
hulus = {}
primes = {}
disneys = {}
plattforms = [netflixs, hulus, primes, disneys]
for title, netflix, hulu, prime, disney in zip(tv_shows['Title'], tv_shows['Netflix'], tv_shows['Hulu'], tv_shows['Prime Video'], tv_shows['Disney+']):
    if netflix == 1:
        netflixs[title] = "Netflix"
    if hulu == 1:
        hulus[title] = "Hulu"
    if prime == 1:
        primes[title] = "Prime"
    if disney == 1:
        disneys[title] = "Disney+"

# ======================================================================================================================

# ============================ A collection of movies found on these streaming platforms ===============================
    # - Title:
    # - Year: The year in which the movie was produced
    # - Age: Target age group
    # - IMDb: IMDb rating
    # - Rotten Tomatoes: Rotten Tomatoes %
    # - Netflix: Whether the movie is found on Netflix (0 or 1)
    # - Hulu: Whether the movie is found on Hulu
    # - Prime Video: Whether the movie is found on Prime Video
movies = pd.read_csv("datasets/MoviesOnStreamingPlatforms_updated.csv")
# ======================================================================================================================

trending_genres = []
for title in trending_title:
    if title in genre_title:
        trending_genres.append(genre_title[title])
    else:
        trending_genres.append('-')

genres = []
originals = []
regions = []
rating = []
plattform = []
for title in all_title:
    if title in original_title:
        originals.append(True)
        regions.append(test[title])
    else:
        originals.append(False)
    if title in genre_title:
        rating.append(ratings[title])
        genres.append(genre_title[title])
    else:
        rating.append('-')
        genres.append('-')
    if title in netflixs:
        plattform.append(netflixs[title])
    elif title in hulus:
        plattform.append(hulus[title])
    elif title in primes:
        plattform.append(primes[title])
    elif title in disneys:
        plattform.append(disneys[title])
    else:
        plattform.append("-")

print(len(plattform))
print(len(genres))


columns = {"Title": all_title,
            "Release Year": years,
            "Date added": all_added,
            "Genre": genres,
            "Type": all_type,
            "PG": all_pg,
            "Original": originals,
            "Rating": rating,
            "Platform": plattform}

table_frame = pd.DataFrame(columns)
table_frame.to_csv('/Users/lstrauch/Documents/Uni/Semester_3/Big_Data/Projekt/combined.csv', encoding='utf-8', index=False)

columns2 = {"Title": trending_title,
            "Year": trending_year,
            "Genre": trending_genres,
            "Rating": trending_rating}

table_frame2 = pd.DataFrame(columns2)
table_frame2.to_csv('/Users/lstrauch/Documents/Uni/Semester_3/Big_Data/Projekt/trending_combined.csv', encoding='utf-8', index=False)