import pandas as pd
import matplotlib.pyplot as plt

# dataset = pd.read_csv('../datasets_pyspark/rdd_netflix_imdb_combined.csv')
#
#
#
# #================================ Order by year by publishers===============================
#
# netflix = dict()
# hulu = dict()
# prime = dict()
# disney = dict()
#
#
# for year, is_netflix, is_hulu, is_prime, is_disney in zip(dataset['Date_Added'], dataset['Netflix'], dataset['Hulu'], dataset['Prime Video'], dataset['Disney+']):
#     if year not in netflix:
#         netflix[year] = 0
#     if year not in hulu:
#         hulu[year] = 0
#     if year not in disney:
#         disney[year] = 0
#     if year not in prime:
#         prime[year] = 0
#     if is_netflix == '1.0':
#         netflix[year] += 1
#     if is_hulu =='1.0':
#         hulu[year]+=1
#     if is_prime =='1.0':
#         prime[year]+=1
#     if is_disney =='1.0':
#         disney[year]+=1
#
# print(netflix)
# print(hulu)
# print(prime)
# print(disney)
#
# #========================= Sort by publisher =============================
#
# publisher = {   "netflix" : 0,
#                 "hulu":0,
#                 "prime":0,
#                 "disney":0
#                 }
#
# for is_netflix, is_hulu, is_prime, is_disney in zip(dataset['Netflix'], dataset['Hulu'], dataset['Prime Video'], dataset['Disney+']):
#     if is_netflix == '1.0':
#         publisher["netflix"] += 1
#     if is_hulu =='1.0':
#         publisher["hulu"]+=1
#     if is_prime =='1.0':
#         publisher["prime"]+=1
#     if is_disney =='1.0':
#         publisher["disney"]+=1
#
# print(publisher)
#
#
# #====================== Count age ========================================
#
# netflix_age = dict()
# hulu_age = dict()
# prime_age = dict()
# disney_age =dict()
#
# for age, is_netflix, is_hulu, is_prime, is_disney in zip(dataset['Age'], dataset['Netflix'], dataset['Hulu'], dataset['Prime Video'], dataset['Disney+']):
#     if age not in netflix_age:
#         netflix_age[age] = 0
#     if age not in hulu_age:
#         hulu_age[age] = 0
#     if age not in prime_age:
#         prime_age[age] = 0
#     if age not in disney_age:
#         disney_age[age] = 0
#     if is_netflix == '1.0':
#         netflix_age[age]+=1
#     if is_hulu == '1.0':
#         hulu_age[age]+=1
#     if is_prime == '1.0':
#         prime_age[age]+=1
#     if is_disney =='1.0':
#         disney_age[age]+=1
#
# print(netflix_age)
# print(hulu_age)
# print(prime_age)
# print(disney_age)


#====================== Sort by Rating ==================================

df = pd.read_csv('../datasets_pyspark/rdd_netflix_imdb_combined_preprocessed.csv', na_values=['-'])

netflix_rating = df.loc[df['Netflix']==1, 'Rating']
hulu_rating = df.loc[df['Hulu']==1, 'Rating']
prime_rating = df.loc[df['Prime Video']==1, 'Rating']
disney_rating = df.loc[df['Disney+']==1, 'Rating']

print(netflix_rating.mean())
print(netflix_rating.var())
print(hulu_rating.mean())
print(hulu_rating.var())
print(prime_rating.mean())
print(prime_rating.var())
print(disney_rating.mean())
print(disney_rating.var())


netflix_rating = dict()
hulu_rating = dict()
prime_rating = dict()
disney_rating = dict()
