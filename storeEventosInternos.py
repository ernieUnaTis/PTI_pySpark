import findspark
import os
import shutil

spark_location='/home/ernesto/Descargas/spark-2.4.5-bin-hadoop2.7/' # Set your own
java8_location= '/usr/lib64/jvm/java-1.8.0-openjdk-1.8.0/jre' # Set your own
os.environ['JAVA_HOME'] = java8_location
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars /home/ernesto/Descargas/spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar pyspark-shell'
findspark.init()

import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition
from datetime import datetime
import random
from pyspark.sql import SQLContext, Row
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType



#VariablesHadoop


sc = pyspark.SparkContext()
ssc = StreamingContext(sc, 10)
sqlContext = SQLContext(sc)

topic = "notificacion_eventos_internos"
brokers = "127.0.0.1:9092"
partition = 0
start = 0
topicpartion = TopicAndPartition(topic, partition)
fromoffset = {topicpartion: int(start)}

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers}, fromOffsets = fromoffset)
data = kvs.map(lambda line: line)
#data.write.parquet("hdfs://data.parquet")
schema = StructType([StructField(str(i), StringType(), True) for i in range(2)])

def saveData(rdd):
    now = datetime.now()
    current_time = now.strftime("%H%M%S")
    rdd.saveAsTextFile("resultados_eventos_internos/salida_"+current_time)
    if not rdd.isEmpty():
        df = sqlContext.createDataFrame(rdd,schema)
        print('  writing file')
        df.write.parquet("resultados_eventos_internos/parquet_"+current_time, mode='append')



data.foreachRDD(saveData)
data.pprint()

ssc.start()
ssc.awaitTermination()
sc.stop()
