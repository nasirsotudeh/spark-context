from pyspark import SparkContext
import sys
sys.path.insert(0, '.')
from pyspark import SparkContext, SparkConf


def role(line : str):
    return not (line.startswith("host") and "bytes" in line)
    
def splitComma(line: str):
    splits = line.split[4]
    return "{}".format(splits[0])

if __name__ == "__main__":
    conf = SparkConf().setAppName("Nasa_logs").setMaster("local[*]")
    sc = SparkContext(conf = conf)

    July_Nasa_Logs = sc.textFile("nasa_19950701.tsv").map(lambda line: line.split("\t"))
    Agust_Nasa_logs = sc.textFile("nasa_19950801.tsv")


    spsp = July_Nasa_Logs.map(lambda line: line[0])
    spsp.saveAsTextFile("aot10/sw.csv")
    
    




	# '''
    # "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    # "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    # Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    # Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    # Example output:
    # vagrant.vf.mmc.com
    # www-a1.proxy.aol.com
    # .....    

    # Keep in mind, that the original log files contains the following header lines.
    # host    logname    time    method    url    response    bytes

    # Make sure the head lines are removed in the resulting RDD.
    # '''
