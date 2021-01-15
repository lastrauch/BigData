import pandas as pd
import matplotlib.pyplot as plt

dataset = pd.read_csv("netflix_imdb_combined_preprocessed.csv")



#================================ Order by year by publishers===============================
#
netflix = dict()
hulu = dict()
prime = dict()
disney = dict()


for year, is_netflix, is_hulu, is_prime, is_disney in zip(dataset['Date Added'], dataset['Netflix'], dataset['Hulu'], dataset['Prime Video'], dataset['Disney+']):
    if year not in netflix.keys():
        netflix[year] = 0
    if year not in hulu:
        hulu[year] = 0
    if year not in disney:
        disney[year] = 0
    if year not in prime:
        prime[year] = 0
    if is_netflix == '1.0':
        netflix[year] += 1
    if is_hulu =='1.0':
        hulu[year]+=1
    if is_prime =='1.0':
        prime[year]+=1
    if is_disney =='1.0':
        disney[year]+=1

print(netflix)
print(hulu)
print(prime)
print(disney)
