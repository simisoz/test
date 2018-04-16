from pyspark import SparkConf, SparkContext

from operator import add
import sys

APP_NAME = " HelloWorld of Big Data"



def main(sc, filename):
   text_file = sc.textFile(filename)
   words = text_file.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1))
   wordcount = words.reduceByKey(add)
   for wc in wordcount.collect():
      print wc[0], wc[1]
   wordcount.saveAsTextFile(sys.argv[2])


if __name__ == "__main__":

   # Configure Spark
   conf = SparkConf().setAppName(APP_NAME)
   sc = SparkContext(conf=conf)
   filename = sys.argv[1]
   main(sc, filename)
