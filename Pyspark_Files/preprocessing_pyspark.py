from __future__ import print_function
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import sql
from pyspark.sql.functions import udf
confCluster = SparkConf().setAppName("NetflixAnalysis")
sc = SparkContext(conf=confCluster)
sqlContext = sql.SQLContext(sc)
sc.setLogLevel("ERROR")


@udf("string")
def release_year(date):
    if "," in date:
        x = date.split(", ")
        return x[1]
    else:
        return date


if __name__ == '__main__':
    df1 = sqlContext.read.option("header",True).csv("rdd_netflix_imdb_combined.csv")
    df1 = df1.na.fill({'Original':'False'})
    df1 = df1.na.fill("-")
    df1 = df1.withColumn('Date_Added', release_year('Date_Added'))
    df1.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets_pyspark/rdd_netflix_imdb_combined_preprocessed.csv', encoding='utf-8', index=False)

    df2 = sqlContext.read.option("header",True).csv("rdd_trending_netflix_imdb_combined.csv")
    df2 = df2.na.fill(value="-")
    df2.withColumn('Date_Added', release_year('Date_Added'))
    df2.toPandas().to_csv('/home/ko93jiy/BigData/Projekt/datasets_pyspark/rdd_trending_netflix_imdb_combined_preprocessed.csv', encoding='utf-8', index=False)
