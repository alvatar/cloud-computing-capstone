from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    #cluster = Cluster()
    #session = cluster.connect('airports')
    #https://academy.datastax.com/demos/getting-started-apache-cassandra-and-python-part-i
    #http://apache-spark-user-list.1001560.n3.nabble.com/Unable-to-ship-external-Python-libraries-in-PYSPARK-td14074.html


    sc = SparkContext(appName="PythonStreamingKafkaTest")
    sc.setLogLevel("WARN")

    ssc = StreamingContext(sc, 1)

    brokers, topic = sys.argv[1:]
    ks = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)

    lines = ks.map(toCSVLine)

    #lines.saveAsTextFiles("testing/tests")

#    ks.map(lambda x: x[1])
    lines.pprint()

    ssc.start()
    ssc.awaitTermination()
