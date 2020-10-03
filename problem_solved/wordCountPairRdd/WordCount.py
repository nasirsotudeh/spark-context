# %%
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":
    conf = SparkConf().setAppName("wordCounts").setMaster("local[3]")
    sc = SparkContext(conf = conf)

    lines = sc.textFile("word_count.text")
    wordRdd = lines.flatMap(lambda line: line.split(" "))
    wordPairRdd = wordRdd.map(lambda word: (word, 1))
# %%
    wordPairRdd.collect() 
# %%

# %%
    wordCounts = wordPairRdd.reduceByKey(lambda x, y: x + y)
# %%
    wordCounts.collect()
# %%
    
    for word, count in wordCounts.collect():
        print("{} : {}".format(word, count))
