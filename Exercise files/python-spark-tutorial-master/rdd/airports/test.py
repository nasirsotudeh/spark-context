# %% 
from pyspark.sql.types import * 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


# %% 
sc = SparkContext('local')
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# %%

lines = sc.textFile("airports.text")
    
# %%
parts = lines.map(lambda l: l.split(","))
sps = spark.createDataFrame(parts)
sps =sps.toDF('id', 'name', 'city','country', 'IATAcode','ICAO code', 'latitude','longitude', 'altitiude','timezoneDST' ,'U', 'timezone osland')
sps.show(2000)
# %%
mrdd=sps.rdd.map(list)
mrdd.saveAsTextFile("muaout/airports_in_usa.text")
# %%


# %%

# %%