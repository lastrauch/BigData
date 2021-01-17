import pandas as pd
import matplotlib.pyplot as plt
from  pyspark  import  SparkConf ,  SparkContext

confCluster = SparkConf().setAppName("NetflixTrendingCluster")
confCluster.set("spark.executor.memory", "1g")
confCluster.set("spark.executor.cores", "4")

repartition_count = 32

dataset = pd.read_csv("combined.csv")

# ============================= Definiere Dictionaries und Listen zur leichteren Bearbeitung ===========================
all = []
genres = []
years = []
comedy = {}
adventure = {}
animation = {}
action = {}
horror = {}
documentary = {}
crime = {}
drama = {}
romance = {}
short = {}
biography = {}
news = {}
thriller = {}
talk_show = {}
reality_tv = {}
fantasy = {}
history = {}
sci_fi = {}
mystery = {}
family = {}
western = {}
music = {}
adult = {}
musical = {}
game_show = {}
sport = {}
war = {}
other = {}


# ===================================== Bekomme alle exitierenden Genre ================================================

for genre in dataset['Genre']:
    if genre not in genres and genre != "-" and genre != "\\N":
        if ',' in genre:
            x = genre.split(",")
            for g in x:
                if g not in genres:
                    genres.append(g)
        else:
            genres.append(genre)

# ======================================= Initialisiere Dictionaries ===================================================

for i in range(2014, 2021):
    comedy[i] = 0
    adventure[i] = 0
    animation[i] = 0
    action[i] = 0
    horror[i] = 0
    documentary[i] = 0
    crime[i] = 0
    drama[i] = 0
    romance[i] = 0
    short[i] = 0
    biography[i] = 0
    news[i] = 0
    thriller[i] = 0
    talk_show[i] = 0
    reality_tv[i] = 0
    fantasy[i] = 0
    history[i] = 0
    sci_fi[i] = 0
    mystery[i] = 0
    family[i] = 0
    western[i] = 0
    music[i] = 0
    adult[i] = 0
    musical[i] = 0
    game_show[i] = 0
    sport[i] = 0
    war[i] = 0
    other[i] = 0

# ============================ Zähle wie oft ein Genre im Jahr hinzugefügt wurde =======================================

for year_mixed, genre in zip(dataset['Date added'], dataset['Genre']):
    if year_mixed != '-' and int(year_mixed) >= 2014:
        year = int(year_mixed)
        if "Comedy" in genre:
            comedy[year] += 1
        elif "Adventure" in genre:
            adventure[year] += 1
        elif "Animation" in genre:
            animation[year] += 1
        elif "Action" in genre:
            action[year] += 1
        elif "Horror" in genre:
            horror[year] += 1
        elif "Documentary" in genre:
            documentary[year] += 1
        elif "Crime" in genre:
            crime[year] += 1
        elif "Drama" in genre:
            drama[year] += 1
        elif "Romance" in genre:
            romance[year] += 1
        elif "Short" in genre:
            short[year] += 1
        elif "Biography" in genre:
            biography[year] += 1
        elif "News" in genre:
            news[year] += 1
        elif "Thriller" in genre:
            thriller[year] += 1
        elif "Talk-Show" in genre:
            talk_show[year] += 1
        elif "Reality-TV" in genre:
            reality_tv[year] += 1
        elif "Fantasy" in genre:
            fantasy[year] += 1
        elif "History" in genre:
            history[year] += 1
        elif "Sci-Fi" in genre:
            sci_fi[year] += 1
        elif "Mystery" in genre:
            mystery[year] += 1
        elif "Family" in genre:
            family[year] += 1
        elif "Western" in genre:
            western[year] += 1
        elif "Music" in genre:
            music[year] += 1
        elif "Adult" in genre:
            adult[year] += 1
        elif "Musical" in genre:
            musical[year] += 1
        elif "Game-Show" in genre:
            game_show[year] += 1
        elif "Sport" in genre:
            sport[year] += 1
        elif "War" in genre:
            war[year] += 1

# ======================================================================================================================

all.append(adventure)
all.append(animation)
all.append(comedy)
all.append(action)
all.append(horror)
all.append(documentary)
all.append(crime)
all.append(drama)
all.append(romance)
all.append(short)
all.append(biography)
all.append(news)
all.append(thriller)
all.append(talk_show)
all.append(reality_tv)
all.append(fantasy)
all.append(history)
all.append(sci_fi)
all.append(mystery)
all.append(family)
all.append(western)
all.append(music)
all.append(adult)
all.append(musical)
all.append(game_show)
all.append(sport)
all.append(war)

# ========================================= Plotting ==========================================================
# -Schaue ob ein Genre mehr als 50 mal in einem Jahr hinzugefügt wurde, ansonsten rechne es zu 'other' dazu
# -Sortiere das Dictionariy von der kleinstens zu groessten jahreszahl
# -Plotte die Dictionaries

for dict, genre in zip(all, genres):
    if max(dict.values()) < 50:
        for year, value in dict.items():
            other[year] += value
        continue
    sorted_list = sorted(dict.items())
    sorted_years, sorted_genre = zip(*sorted_list)
    plt.plot(sorted_years, sorted_genre, label=genre)

other_list = sorted(other.items())
other_years, other_genre = zip(*other_list)
plt.plot(other_years, other_genre, label="Other")

# ======================================================================================================================

plt.title("Anzahl hinzugefügter Titel im jeweiligen Genre", fontsize=10)
plt.xlabel("Jahr")
plt.ylabel("Anzahl")
plt.legend(fontsize=8)
plt.show()
