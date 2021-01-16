import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("/Users/lstrauch/Documents/Uni/Semester_3/Big_Data/Projekt/datasets_pyspark/combined_year_count.csv")
df2 = pd.read_csv("/Users/lstrauch/Documents/Uni/Semester_3/Big_Data/Projekt/datasets_pyspark/combined_count.csv")


def plot_gesamt_rausgekommen():
    plt.bar(df['Date_Added'], df['Anzahl'])
    plt.title("Anzahl hinzugefügter Titel über alle Plattformen im jeweiligen Jahr", fontsize=10)
    plt.xlabel("Jahr")
    plt.ylabel("Anzahl")
    plt.show()


df2.plot(x="Genre", y=["Jahr_2016", "Jahr_2017", "Jahr_2018", "Jahr_2019", "Jahr_2020"], kind="bar")

plt.show()