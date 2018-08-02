import threading
import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlContx = SQLContext(sc)

# Carregando arquivo de entrada e quebrando em palavras separadas por espaço.
# Loading input file and breaking into words separated by space.
file = sc.textFile("/inFiles/shakespeare.txt").flatMap(lambda line: line.split(" ")).cache()

# https://www.cloudera.com/documentation/enterprise/5-8-x/topics/spark_develop_run.html
def wordCount(file):
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2).toDF(["Word","Count"])
    wordCounts.write.mode("overwrite").parquet("/outFiles/wordcount")
    
def charCount(file):
    wordCounts = file.map(lambda word: (word, 1)).reduceByKey(lambda v1,v2:v1 +v2)
    charCounts = wordCounts.flatMap(lambda pair:pair[0]).map(lambda c: c).map(lambda c: (c, 1)).reduceByKey(lambda v1,v2:v1 +v2).toDF()
    charCounts.write.mode("overwrite").parquet("/outFiles/charcount")

# Instanciando variáveis threads com funções na memória.
# Instantiating thread variables with functions in memory.
T1 = threading.Thread(target=wordCount, args=(file,))
T2 = threading.Thread(target=charCount, args=(file,))

# Iniciando execução das threads.
# Starting execution of threads.
T1.start()
T2.start()

# Pausando execução das threads para seguir o fluxo principal. Sem eles, a thread rodará em paralelo.
# Pausing thread execution to follow main stream. Without them, the thread will run in parallel.
T1.join()
T2.join()
