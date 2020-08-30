
# %%
from pyspark import SparkContext
from pyspark.sql import SparkSession

 # %% 
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("dataframe").getOrCreate()
df = spark.read.format("csv")\
    .option("header" , "true")\
    .option("inferschema" , "true")\
    .load("data\online-retail-dataset.csv")\
    .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")
# %% 
df.count()
# %% 
df.printSchema()
# %% 
from pyspark.sql.functions import count
from pyspark.sql.functions import countDistinct
df.select(count("stockCode")).show()
df.select(countDistinct("StockCode")).show()
# %%
from pyspark.sql.functions import first , last
# %%
df.select(first("StockCode"), last("StockCode")).show()
# %%
from pyspark.sql.functions import min , max
# %%
df.select(count("StockCode")).show()
df.select(min("StockCode") ,max("StockCode")).show()
# %%
from pyspark.sql.functions import sum, sumDistinct
# %%
# df.select(sum("Quantity")).show()
df.select(sumDistinct("Quantity")).show()
# %%
df.groupBy("InvoiceNo", "CustomerID").count().show()
# %%
from pyspark.sql.functions import count , expr
# %%
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show(5)
# %%


