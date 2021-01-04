import pandas as pd

# ======================== tv shows and movies available on Netflix as of 2019 =========================================
    # - shwo_id
    # - type
    # - title
    # - director
    # - cast
    # - country
    # - data_added
    # - release_year
    # - rating
    # - duration
all = pd.read_csv("netflix_titles.csv")
years_preCorona = []
years_2020 = []
years = []
all_type = []
all_rating = []
all_title = []
print(all.columns)
for year, typ, title, rating in zip(all['release_year'],all['type'], all['title'], all['rating']):
    all_title.append(title)
    years.append(year)
    all_type.append(typ)
    all_rating.append(rating)

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
genre_imdb = pd.read_csv("genre.tsv", sep='\t')

genre_title = {}
for title, genre in zip(genre_imdb['originalTitle'], genre_imdb['genres']):
    genre_title[title] = genre
# ======================================================================================================================

# ================== collection of top 50 trending Tv shows currently streaming on Netflix =============================
    # - Titles: It contains the name of the Top Tv shows.
    # - Year: Which Year this Tv Show Released?
    # - Netflix_Rating: Rated By Netflix.
    # - IMDB_Rating: Rated By IMDB.
    # - Netflix: Currently Streaming on Netflix or Not.
trending = pd.read_csv("trending.csv")
trending_years_preCorona = []
trending_years2020 = []
# ======================================================================================================================


genres = []
for title in all_title:
    if title in genre_title:
        genres.append(genre_title[title])
    else:
        genres.append('-')

columns = {"Title": all_title,
            "Year": year,
            "Genre": genres,
            "Type": all_type,
            "Rating": all_rating}

table_frame = pd.DataFrame(columns)
print(table_frame)
table_frame.to_csv('/Users/lstrauch/Documents/Uni/Semester_3/Big_Data/Projekt/combined.csv', encoding='utf-8', index=False)

c = 0
for genre in genres:
    if genre == '-':
        c += 1

print(c, "out of ", len(genres), " is None")

#print(df2.columns)
#print(df.columns)
#print("2019: ", len(years_2019))
#print("2020: ", len(years_2020))
#print(years_2020)