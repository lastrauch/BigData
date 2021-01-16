import pandas as pd
import matplotlib.pyplot as plt

dataset = pd.read_csv("../trending_netflix_imdb_combined_preprocessed.csv")


all = []
genres = []
years = []
crime = [0, 'Crime']
drama = [0, 'Drama']
thriller = [0, 'Thriller']
action = [0, 'Action']
adventure = [0, 'Adventure']
animation = [0, 'Animation']
comedy = [0, 'Comedy']
mystery = [0, 'Mystery']
sci_fi = [0, 'Sci-Fi']
fantasy = [0, 'Fantasy']
horror = [0, 'Horror']
romance = [0, 'Romance']
news = [0, 'News']
music = [0, 'Music']
reality_tv = [0, 'Reality-TV']
game_show = [0, 'Game-Show']
biography = [0, 'Biography']


for genre in dataset['Genre']:
    if genre not in genres and genre != "-" and genre != "\\N":
        if ',' in genre:
            x = genre.split(",")
            for g in x:
                if g not in genres:
                    genres.append(g)
        else:
            genres.append(genre)

print(genres)

for genre in dataset['Genre']:
    if "Comedy" in genre:
        comedy[0] += 1
    if "Adventure" in genre:
        adventure[0] += 1
    if "Animation" in genre:
        animation[0] += 1
    if "Action" in genre:
        action[0] += 1
    if "Horror" in genre:
        horror[0] += 1
    if "Crime" in genre:
        crime[0] += 1
    if "Drama" in genre:
        drama[0] += 1
    if "Romance" in genre:
        romance[0] += 1
    if "Biography" in genre:
        biography[0] += 1
    if "News" in genre:
        news[0] += 1
    if "Thriller" in genre:
        thriller[0] += 1
    if "Reality-TV" in genre:
        reality_tv[0] += 1
    if "Fantasy" in genre:
        fantasy[0] += 1
    if "Sci-Fi" in genre:
        sci_fi[0] += 1
    if "Mystery" in genre:
        mystery[0] += 1
    if "Music" in genre:
        music[0] += 1
    if "Game-Show" in genre:
        game_show[0] += 1
# ======================================================================================================================

all.append(adventure)
all.append(animation)
all.append(comedy)
all.append(action)
all.append(horror)
all.append(crime)
all.append(drama)
all.append(romance)
all.append(biography)
all.append(news)
all.append(thriller)
all.append(reality_tv)
all.append(fantasy)
all.append(sci_fi)
all.append(mystery)
all.append(music)
all.append(game_show)

# ========================================= Plotting ==========================================================

for list in all:
    if list[0] > 5:
        plt.bar(list[1], list[0])

# ======================================================================================================================

plt.title("Anzahl beliebter Title im jeweiligen Genre", fontsize=10)
plt.xticks(fontsize=6)
plt.show()

