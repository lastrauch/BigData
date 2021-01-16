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

df1 = sqlContext.read.option("header",True).csv("rdd_trending_netflix_imdb_combined_preprocessed.csv")
split_genre = lambda genre: genre.split(',')
df1 = df1.withColumn('Genre', udf(split_genre, ArrayType(StringType()))('Genre')).select(explode('Genre')).groupBy('col').count()
df1 = df1.selectExpr("col as Genre", "count as Anzahl")
df1.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets/trending_genre_count.csv', encoding='utf-8', index=False)