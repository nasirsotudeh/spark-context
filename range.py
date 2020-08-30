# %%
from pyspark import SparkContext
sc = SparkContext.getOrCreate
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD").getOrCreate()
# %%
spark.range(1, 10 ,6).collect()

# %%
spark.range(10).rdd
# %%
spark.range(10).rdd.toDF()

# %%
myCollection = "Spark is a unified analytics engine" \
" for large-scale data" \
" processing. It provides high-level APIs in Scala" \
" Java, Python" \
" , and R, and an optimized engine that supports " \
" general computation graphs for data analysis." \
" It also supports a rich set of higher-level tools " \
" including Spark SQL for SQL and DataFrames," \
" MLlib for machine learning, GraphX for graph processing," \
" and Structured Streaming for stream processing." \
.split(" ")
# %%
words = spark.sparkContext.parallelize(myCollection , 2)
words.setName("myWord")
# %%
words.count()
# %%
words.distinct().count()
# %%
# def start words by s
def startWithS(ind):
    return ind.startswith("M")

words.filter(lambda word:startWithS(word)).distinct().collect()
# %%
