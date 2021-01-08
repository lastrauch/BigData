import pandas as pd
import matplotlib.pyplot as plt

dataset = pd.read_csv("trending_combined.csv")


all = []
genres = []
years = []
comedy = []
adventure = []
animation = []
action = []
horror = []
documentary = []
crime = []
drama = []
romance = []
short = []
biography = []
news = []
thriller = []
talk_show = []
reality_tv = []
fantasy = []
history = []
sci_fi = []
mystery = []
family = []
western = []
music = []
adult = []
musical = []
game_show = []
sport = []
war = []
other = []


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

for i in range(2014, 2021):
    comedy.append(0)
    comedy.append("Comedy")
    adventure.append(0)
    adventure.append("Adventure")
    animation.append(0)
    animation.append("Animation")
    action.append(0)
    action.append("Action")
    horror.append(0)
    horror.append("Horror")
    documentary.append(0)
    documentary.append("Documentary")
    crime.append(0)
    crime.append("Crime")
    drama.append(0)
    drama.append("Drama")
    romance.append(0)
    romance.append("Romance")
    short.append(0)
    short.append("Short")
    biography.append(0)
    biography.append("Biography")
    news.append(0)
    news.append("News")
    thriller.append(0)
    thriller.append("Thriller")
    talk_show.append(0)
    talk_show.append("Talk-Show")
    reality_tv.append(0)
    reality_tv.append("Reality-TV")
    fantasy.append(0)
    fantasy.append("Fantasy")
    history.append(0)
    history.append("History")
    sci_fi.append(0)
    sci_fi.append("Sci-Fi")
    mystery.append(0)
    mystery.append("Mystery")
    family.append(0)
    family.append("Family")
    western.append(0)
    western.append("Western")
    music.append(0)
    music.append("Music")
    adult.append(0)
    adult.append("Adult")
    musical.append(0)
    musical.append("Musical")
    game_show.append(0)
    game_show.append("Game-Show")
    sport.append(0)
    sport.append("Sport")
    war.append(0)
    war.append("War")

for genre in dataset['Genre']:
    if "Comedy" in genre:
        comedy[0] += 1
    elif "Adventure" in genre:
        adventure[0] += 1
    elif "Animation" in genre:
        animation[0] += 1
    elif "Action" in genre:
        action[0] += 1
    elif "Horror" in genre:
        horror[0] += 1
    elif "Documentary" in genre:
        documentary[0] += 1
    elif "Crime" in genre:
        crime[0] += 1
    elif "Drama" in genre:
        drama[0] += 1
    elif "Romance" in genre:
        romance[0] += 1
    elif "Short" in genre:
        short[0] += 1
    elif "Biography" in genre:
        biography[0] += 1
    elif "News" in genre:
        news[0] += 1
    elif "Thriller" in genre:
        thriller[0] += 1
    elif "Talk-Show" in genre:
        talk_show[0] += 1
    elif "Reality-TV" in genre:
        reality_tv[0] += 1
    elif "Fantasy" in genre:
        fantasy[0] += 1
    elif "History" in genre:
        history[0] += 1
    elif "Sci-Fi" in genre:
        sci_fi[0] += 1
    elif "Mystery" in genre:
        mystery[0] += 1
    elif "Family" in genre:
        family[0] += 1
    elif "Western" in genre:
        western[0] += 1
    elif "Music" in genre:
        music[0] += 1
    elif "Adult" in genre:
        adult[0] += 1
    elif "Musical" in genre:
        musical[0] += 1
    elif "Game-Show" in genre:
        game_show[0] += 1
    elif "Sport" in genre:
        sport[0] += 1
    elif "War" in genre:
        war[0] += 1
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

for list in all:
    if list[0] > 1:
        plt.bar(list[1], list[0])

# ======================================================================================================================

plt.title("Anzahl beliebter Title im jeweiligen Genre", fontsize=10)
plt.xticks(fontsize=6)
plt.show()