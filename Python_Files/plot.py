import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import cm
import random
import matplotlib.colors as mcolors

df = pd.read_csv("../datasets_pyspark/combined_year_count.csv")
df2 = pd.read_csv("../datasets_pyspark/combined_count.csv")
df3 = pd.read_csv("../datasets_pyspark/trending_genre_count.csv")
df4 = pd.read_csv("../datasets_pyspark/release_plattform_mean_var.csv")
df5 = pd.read_csv("../datasets_pyspark/original_genre.csv")
df6 = pd.read_csv("../datasets_pyspark/union_original.csv")


def plot_gesamt_rausgekommen():
    plt.bar(df['Date_Added'], df['Anzahl'], color = 'seagreen', edgecolor='blue')
    plt.title("Anzahl hinzugefügter Titel über alle Plattformen im jeweiligen Jahr", fontsize=10)
    plt.xlabel("Jahr")
    plt.ylabel("Anzahl")
    plt.show()


def plot_genres_year():
    df_3 = df2[(df2['Genre'] != '-') & (df2['Genre'] != '\\N')]
    df_31 = df_3.drop('Genre', 1)
    df_31 = df_31.transpose()

    for column, genre in zip(df_31, df_3['Genre']):
        plt.plot(df_31[column], label=genre)

    plt.xlabel("Jahr")
    plt.ylabel("Anzahl")
    plt.legend(fontsize=8)
    plt.title("Anzahl hinzugefügter Genre über alle Plattformen im jeweiligen Jahr", fontsize=10)
    plt.show()

def plot_trending_genres():
    df_3 = df3[(df3['Genre'] != '-') & (df3['Genre'] != '\\N')]
    labels=df_3["Genre"]

    plt.pie(df_3["Anzahl"], labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85)
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    plt.axis('equal')
    plt.tight_layout()
    plt.title("Genreverteilung der beliebten Serien auf Netflix 2019 ", fontsize=10)

    plt.show()


def plot_original_genres():
    df_5 = df5[(df5['Genre']!='-') & (df5['Genre']!='\\N')]
    labels=df_5["Genre"]
    ax = df_5.plot(kind='bar', width=0.9)
    ax.set_xticklabels(labels, fontsize=6)
    plt.title("Genreverteilung der Original Netflix-Titel", fontsize=10)
    plt.show()


def plot_union_original():
    labels=['Original', 'Nicht Original']
    fig = plt.figure(figsize=(5, 3))
    plt.pie(df6["Anzahl"], autopct='%1.1f%%', startangle=90, pctdistance=0.85)
    plt.legend(labels, loc="best")
    plt.axis('equal')
    plt.tight_layout()
    plt.title("Anteil an Netflix-Originals in den beliebten Serien 2019 auf Netflix ", fontsize=10)

    plt.show()


def plot_release_platform():
    df = pd.read_csv("../datasets_pyspark/release_plattform_count.csv")

    df = df.drop('Jahr', 1)
    ax = df.plot(kind = 'bar', width=0.9)
    ax.set_xticklabels( ('2014', '2015','2016','2017','2018','2019','2020') )
    ax.legend(labels=['Netflix', 'Prime', 'Hulu', 'Disney+'])
    plt.title("Anzahl hinzugefügter Titel der unterschiedlichen Plattformen im jeweiligen Jahr", fontsize=10)
    plt.show()

def errorbar_rating():
    labels = df4["Platform"]
    plt.errorbar(labels, df4["mean"], yerr=df4["variance"], linestyle='None', marker='o' )
    plt.ylim([0,10])
    plt.title("Varianz und Durchschnitt des IMDB Ratings der unterschiedlichen Plattformen", fontsize=10)
    plt.show()

def published_trending():
    df = pd.read_csv("../datasets_pyspark/trending_year_count.csv")
    labels = df["Date_Added"]

    plt.pie(df["Anzahl"], autopct='%1.1f%%', startangle=80, pctdistance=0.85)
    plt.legend(labels, loc="best")
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    plt.axis('equal')
    plt.title("Veröffentlichungsjahre der beliebten Serien aus 2019 von Netflix", fontsize=10)
    plt.tight_layout()
    plt.show()

def market_value():
    market = pd.read_csv("../datasets/netflix_marketvalue.csv")
    release = pd.read_csv("../datasets_pyspark/release_plattform_count.csv")

    plt.figure()

    plt.ylim([0,3500])
    labels = release['Jahr']
    plt.bar(labels, release['Netflix_Anzahl'], label = labels, color = 'darkred')
    plt.ylabel("Released movies by Netflix")

    axes2 = plt.twinx()
    axes2 = plt.twiny()
    axes2.plot(market['Date'],market['Open'], color='darkorange')
    axes2.set_ylim(0,700)
    axes2.set_ylabel("marketvalue of Netflix")
    axes2.axes.get_xaxis().set_visible(False)
    plt.show()



if __name__ == "__main__":
    # plot_gesamt_rausgekommen()
    # plot_genres_year()
    # plot_trending_genres()
    # plot_release_platform()
    # errorbar_rating()
    # published_trending()
    # plot_original_genres()
    # plot_union_original()
    market_value()
