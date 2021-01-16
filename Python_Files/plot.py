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

df3 = df2.drop('Genre', 1)
df3 = df3.transpose()

for column, genre in zip(df3, df2['Genre']):
    plt.plot(df3[column], label=genre)

plt.xlabel("Jahr")
plt.ylabel("Anzahl")
plt.legend(fontsize=8)
plt.show()