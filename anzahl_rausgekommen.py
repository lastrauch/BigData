import pandas as pd
import matplotlib.pyplot as plt

dataset = pd.read_csv("combined.csv")

y_2014 = [2014, 0]
y_2015 = [2015, 0]
y_2016 = [2016, 0]
y_2017 = [2017, 0]
y_2018 = [2018, 0]
y_2019 = [2019, 0]
y_2020 = [2020, 0]
years = []

for year in dataset['Date added']:
    if year == '2014':
        y_2014[1] += 1
    elif year == '2015':
        y_2015[1] += 1
    elif year == '2016':
        y_2016[1] += 1
    elif year == '2017':
        y_2017[1] += 1
    elif year == '2018':
        y_2018[1] += 1
    elif year == '2019':
        y_2019[1] += 1
    elif year == '2020':
        y_2020[1] += 1

years.append(y_2014)
years.append(y_2015)
years.append(y_2016)
years.append(y_2017)
years.append(y_2018)
years.append(y_2019)
years.append(y_2020)

for year in years:
    print(year[1])
    if year[1] > 1:
        plt.bar(year[0], year[1])

plt.title("Anzahl hinzugefügter Titel im jeweiligen Jahr", fontsize=10)
plt.xlabel("Jahr")
plt.ylabel("Anzahl")
plt.show()

