from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.functions import udf, explode, concat, col, lit, desc
confCluster = SparkConf().setAppName("NetflixAnalysis")
sc = SparkContext(conf=confCluster)
sqlContext = sql.SQLContext(sc)
sc.setLogLevel("ERROR")

df1 = sqlContext.read.option("header",True).csv("rdd_trending_netflix_imdb_combined_preprocessed.csv")

df1 = df1.filter("Date_Added != '-'")
df1 = df1.groupBy('Date_Added').count().sort(desc("Date_Added"))
df1 = df1.selectExpr("Date_Added as Date_Added", "count as Anzahl")
df1.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets/trending_year_count.csv', encoding='utf-8', index=False)

df2 = sqlContext.read.option("header",True).csv("rdd_netflix_imdb_combined_preprocessed.csv")

df2 = df2.filter("Date_Added != '-' and Date_Added > 2012")
df2 = df2.groupBy('Date_Added').count().sort(desc("Date_Added"))
df2 = df2.selectExpr("Date_Added as Date_Added", "count as Anzahl")
df2.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets/combined_year_count.csv', encoding='utf-8', index=False)