import threading
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContx = SQLContext(sc)

file = sc.textFile("/inFiles/shakespeare.txt").flatMap(lambda line: line.split(" ")).cache()

#https://www.cloudera.com/documentation/enterprise/5-8-x/topics/spark_develop_run.html
def wordCount(file):
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).toDF(["Word","Count"])
    wordCounts.write.mode("overwrite").parquet("/outFiles/wordcount")
    
def charCount(file):
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
    charCounts = wordCounts.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2).toDF()
    charCounts.write.mode("overwrite").parquet("/outFiles/charcount")

T1 = threading.Thread(target=wordCount, args=(file,))
T2 = threading.Thread(target=charCount, args=(file,))

T1.start()
T2.start()

T1.join()
T2.join()