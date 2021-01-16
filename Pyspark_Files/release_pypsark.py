from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, explode, concat, col, lit
from pyspark.sql.functions import mean as _mean, stddev as _stddev, variance as _var
confCluster = SparkConf().setAppName("NetflixAnalysis")
sc = SparkContext(conf=confCluster)
sqlContext = sql.SQLContext(sc)
sc.setLogLevel("ERROR")

df2 = sqlContext.read.option("header",True).csv("rdd_netflix_imdb_combined_preprocessed.csv")

df_2014_netflix = df2.filter("Netflix == 1 and Date_Added == 2014").groupBy('Date_Added').count()
df_2014_netflix = df_2014_netflix.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2015_netflix = df2.filter("Netflix == 1 and Date_Added == 2015").groupBy('Date_Added').count()
df_2015_netflix = df_2015_netflix.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2016_netflix = df2.filter("Netflix == 1 and Date_Added == 2016").groupBy('Date_Added').count()
df_2016_netflix = df_2016_netflix.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2017_netflix = df2.filter("Netflix == 1 and Date_Added == 2017").groupBy('Date_Added').count()
df_2017_netflix = df_2017_netflix.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2018_netflix = df2.filter("Netflix == 1 and Date_Added == 2018").groupBy('Date_Added').count()
df_2018_netflix = df_2018_netflix.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2019_netflix = df2.filter("Netflix == 1 and Date_Added == 2019").groupBy('Date_Added').count()
df_2019_netflix = df_2019_netflix.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2020_netflix = df2.filter("Netflix == 1 and Date_Added == 2020").groupBy('Date_Added').count()
df_2020_netflix = df_2020_netflix.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_netflix = df_2014_netflix.union(df_2015_netflix)
df_netflix = df_netflix.union(df_2016_netflix)
df_netflix = df_netflix.union(df_2017_netflix)
df_netflix = df_netflix.union(df_2018_netflix)
df_netflix = df_netflix.union(df_2019_netflix)
df_netflix = df_netflix.union(df_2020_netflix)
df_netflix = df_netflix.selectExpr("Jahr as Jahr", "Anzahl as Netflix_Anzahl")
df_netflix.show()

# ================================================================================================

df_2014_prime = df2.filter("'Prime Video' == 1 and Date_Added == 2014").groupBy('Date_Added').count()
df_2014_prime = df_2014_prime.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2015_prime = df2.filter("'Prime Video' == 1 and Date_Added == 2015").groupBy('Date_Added').count()
df_2015_prime = df_2015_prime.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2016_prime = df2.filter("'Prime Video' == 1 and Date_Added == 2016").groupBy('Date_Added').count()
df_2016_prime = df_2016_prime.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2017_prime = df2.filter("'Prime Video' == 1 and Date_Added == 2017").groupBy('Date_Added').count()
df_2017_prime = df_2017_prime.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2018_prime = df2.filter("'Prime Video' == 1 and Date_Added == 2018").groupBy('Date_Added').count()
df_2018_prime = df_2018_prime.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2019_prime = df2.filter("'Prime Video' == 1 and Date_Added == 2019").groupBy('Date_Added').count()
df_2019_prime = df_2019_prime.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2020_prime = df2.filter("'Prime Video' == 1 and Date_Added == 2020").groupBy('Date_Added').count()
df_2020_prime = df_2020_prime.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_prime = df_2014_prime.union(df_2015_netflix)
df_prime = df_prime.union(df_2016_netflix)
df_prime = df_prime.union(df_2017_netflix)
df_prime = df_prime.union(df_2018_netflix)
df_prime = df_prime.union(df_2019_netflix)
df_prime = df_prime.union(df_2020_netflix)
df_prime = df_prime.selectExpr("Jahr as Jahr", "Anzahl as Prime_Anzahl")
df_prime.show()

# ================================================================================================

df_2014_hulu = df2.filter("Hulu == 1 and Date_Added == 2014").groupBy('Date_Added').count()
df_2014_hulu = df_2014_hulu.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2015_hulu = df2.filter("Hulu == 1 and Date_Added == 2015").groupBy('Date_Added').count()
df_2015_hulu = df_2015_hulu.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2016_hulu = df2.filter("Hulu == 1 and Date_Added == 2016").groupBy('Date_Added').count()
df_2016_hulu = df_2016_hulu.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2017_hulu = df2.filter("Hulu == 1 and Date_Added == 2017").groupBy('Date_Added').count()
df_2017_hulu = df_2017_hulu.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2018_hulu = df2.filter("Hulu == 1 and Date_Added == 2018").groupBy('Date_Added').count()
df_2018_hulu = df_2018_hulu.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2019_hulu = df2.filter("Hulu == 1 and Date_Added == 2019").groupBy('Date_Added').count()
df_2019_hulu = df_2019_hulu.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2020_hulu = df2.filter("Hulu == 1 and Date_Added == 2020").groupBy('Date_Added').count()
df_2020_hulu = df_2020_hulu.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_hulu = df_2014_hulu.union(df_2015_netflix)
df_hulu = df_hulu.union(df_2016_netflix)
df_hulu = df_hulu.union(df_2017_netflix)
df_hulu = df_hulu.union(df_2018_netflix)
df_hulu = df_hulu.union(df_2019_netflix)
df_hulu = df_hulu.union(df_2020_netflix)
df_hulu = df_hulu.selectExpr("Jahr as Jahr", "Anzahl as Hulu_Anzahl")
df_hulu.show()

# ================================================================================================

df_2014_disney = df2.filter("'Disney+' == 1 and Date_Added == 2014").groupBy('Date_Added').count()
df_2014_disney = df_2014_disney.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2015_disney = df2.filter("'Disney+' == 1 and Date_Added == 2015").groupBy('Date_Added').count()
df_2015_disney = df_2015_disney.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2016_disney = df2.filter("'Disney+' == 1 and Date_Added == 2016").groupBy('Date_Added').count()
df_2016_disney = df_2016_disney.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2017_disney = df2.filter("'Disney+' == 1 and Date_Added == 2017").groupBy('Date_Added').count()
df_2017_disney = df_2017_disney.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2018_disney = df2.filter("'Disney+' == 1 and Date_Added == 2018").groupBy('Date_Added').count()
df_2018_disney = df_2018_disney.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2019_disney = df2.filter("'Disney+' == 1 and Date_Added == 2019").groupBy('Date_Added').count()
df_2019_disney = df_2019_disney.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_2020_disney = df2.filter("'Disney+' == 1 and Date_Added == 2020").groupBy('Date_Added').count()
df_2020_disney = df_2020_disney.selectExpr("Date_Added as Jahr", "count as Anzahl")

df_disney = df_2014_disney.union(df_2015_netflix)
df_disney = df_disney.union(df_2016_netflix)
df_disney = df_disney.union(df_2017_netflix)
df_disney = df_disney.union(df_2018_netflix)
df_disney = df_disney.union(df_2019_netflix)
df_disney = df_disney.union(df_2020_netflix)
df_disney = df_disney.selectExpr("Jahr as Jahr", "Anzahl as Disney_Anzahl")
df_hulu.show()

# ================================================================================================

union = df_netflix.join(df_prime, 'Jahr', 'left')
union = union.join(df_hulu, 'Jahr', 'left')
union = union.join(df_disney, 'Jahr', 'left')
union.show()
union.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets_pyspark/release_plattform_count.csv', encoding='utf-8', index=False)

# ================================================================================================

netflix_rating = df2.filter("Netflix == '1'").select(_mean(col('Rating')).alias('mean'), _var(col('Rating')).alias('variance')).withColumn('Platform', lit('Netflix'))
hulu_rating = df2.filter("Hulu == '1'").select(_mean(col('Rating')).alias('mean'), _var(col('Rating')).alias('variance')).withColumn('Platform', lit('Hulu'))
prime_rating = df2.filter(col('Prime Video') == '1').select(_mean(col('Rating')).alias('mean'), _var(col('Rating')).alias('variance')).withColumn('Platform', lit('Prime'))
disney_rating = df2.filter(col('Disney+') == '1').select(_mean(col('Rating')).alias('mean'), _var(col('Rating')).alias('variance')).withColumn('Platform', lit('Disney+'))

union = netflix_rating.union(hulu_rating).union(prime_rating).union(disney_rating)
union.show()
union.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets_pyspark/release_plattform_mean_var.csv', encoding='utf-8', index=False)