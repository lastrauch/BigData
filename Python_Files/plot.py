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


def plot_gesamt_rausgekommen():
    plt.bar(df['Date_Added'], df['Anzahl'], color = 'seagreen', edgecolor='blue')
    plt.title("Anzahl hinzugefügter Titel über alle Plattformen im jeweiligen Jahr", fontsize=10)
    plt.xlabel("Jahr")
    plt.ylabel("Anzahl")
    plt.show()


def plot_genres_year():
    df3 = df2.drop('Genre', 1)
    df3 = df3.transpose()

    for column, genre in zip(df3, df2['Genre']):
        plt.plot(df3[column], label=genre)

    plt.xlabel("Jahr")
    plt.ylabel("Anzahl")
    plt.legend(fontsize=8)
    plt.title("Anzahl hinzugefügter Genre über alle Plattformen im jeweiligen Jahr", fontsize=10)
    plt.show()

def plot_trending_genres():
    labels=df3["Genre"]

    plt.pie(df3["Anzahl"], labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85)
    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    plt.axis('equal')
    plt.tight_layout()
    plt.title("Genreverteilung der beliebten Serien auf Netflix 2019 ", fontsize=10)

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
    plt.tight_layout()
    plt.title("Veröffentlichungsjahre der beliebten Serien aus 2019 von Netflix", fontsize=10)
    plt.tight_layout()
    plt.show()

def originals_genres():
    print('pimmel')


if __name__ == "__main__":
    plot_gesamt_rausgekommen()
    plot_genres_year()
    plot_trending_genres()
    plot_release_platform()
    errorbar_rating()
    published_trending()
