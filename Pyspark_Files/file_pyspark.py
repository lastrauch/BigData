from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, explode, concat, col, lit
confCluster = SparkConf().setAppName("NetflixAnalysis")
sc = SparkContext(conf=confCluster)
sqlContext = sql.SQLContext(sc)
sc.setLogLevel("ERROR")

df2 = sqlContext.read.option("header",True).csv("rdd_netflix_imdb_combined_preprocessed.csv")

split_genre = lambda genre: genre.split(',')

df_2014 = df2.filter("Date_Added == 2014")
df_2014 = df_2014.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df_2014 = df_2014.selectExpr("col as Genre", "count as Jahr_2014")

df_2015 = df2.filter("Date_Added == 2015")
df_2015 = df_2015.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df_2015 = df_2015.selectExpr("col as Genre", "count as Jahr_2015")

df_2016 = df2.filter("Date_Added == 2016")
df_2016 = df_2016.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df_2016 = df_2016.selectExpr("col as Genre", "count as Jahr_2016")

df_2017 = df2.filter("Date_Added == 2017")
df_2017 = df_2017.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df_2017 = df_2017.selectExpr("col as Genre", "count as Jahr_2017")

df_2018 = df2.filter("Date_Added == 2018")
df_2018 = df_2018.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df_2018 = df_2018.selectExpr("col as Genre", "count as Jahr_2018")

df_2019 = df2.filter("Date_Added == 2019")
df_2019 = df_2019.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df_2019 = df_2019.selectExpr("col as Genre", "count as Jahr_2019")

df_2020 = df2.filter("Date_Added == 2020")
df_2020 = df_2020.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df_2020 = df_2020.selectExpr("col as Genre", "count as Jahr_2020")

union = df_2014.join(df_2015, 'Genre', 'left')
union = union.join(df_2016, 'Genre', 'left')
union = union.join(df_2017, 'Genre', 'left')
union = union.join(df_2018, 'Genre', 'left')
union = union.join(df_2019, 'Genre', 'left')
union = union.join(df_2020, 'Genre', 'left')
union.show()
union.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets/combined_count.csv', encoding='utf-8', index=False)