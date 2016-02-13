from __future__ import print_function

import sys
import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 42})

    cassandraCluster = Cluster(contact_points=[os.getenv('CASSANDRA', 'localhost')])
    cassandra = cassandraCluster.connect('spark')

    def producePerFlight(line):
        val = line[1].split("\t")
        # Origin, Carrier, DepDelay
        return((val[6], val[4]), float(val[8]))

    def movingAvg(newValues, movingAvg):
        prevAvg, prevN = movingAvg or (0,0)
        currentN = len(newValues)
        return (float(prevAvg*prevN + sum(newValues)) / (prevN + currentN), prevN + currentN)

    digest = ks.map(producePerFlight)\
             .updateStateByKey(movingAvg)\
             .map(lambda x: (x[0][0], (x[0][1], x[1][0])))\
             .groupByKey()\
             .map(lambda x: (x[0], sorted(x[1], key=lambda val: val[1])[:10]))

    cassandra.execute(
        """
        insert into test (key, val) values ('asdf', 'alvatar')
        """
    )

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    lines.pprint()

    ssc.start()
    ssc.awaitTermination()
