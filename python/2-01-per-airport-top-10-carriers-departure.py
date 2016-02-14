from __future__ import print_function

import sys
import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from cassandra.cluster import Cluster

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <zk> <topic> <cassandra>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic, cassandraAddr = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 42})

    def processInput(line):
        fields = line[1].split("\t")
        # (Origin, Carrier), DepDelay
        return((str(fields[6]), str(fields[4])), float(fields[8]))

    def movingAvg(newValues, movingAvg):
        prevAvg, prevN = movingAvg or (0,0)
        currentN = len(newValues)
        return (float(prevAvg*prevN + sum(newValues)) / (prevN + currentN), prevN + currentN)

    def cassandraStore(iter):
        cassandraCluster = Cluster(contact_points=[cassandraAddr])
        cassandra = cassandraCluster.connect('spark')
        for record in iter:
            cassandra.execute(
                "insert into test (key, val) values ('%s', '%s')" % \
                (str(record[0]), str(record[1]).replace('\'',''))
            )
        cassandra.shutdown()

    # Generate a (Origin, Carrier) -> DepDelay mapping
    # Moving average DepDelay
    # Remap key: Origin -> (Carrier, DepDelay)
    # Sort and select the 10 best
    digest = ks.map(processInput)\
             .updateStateByKey(movingAvg)\
             .map(lambda ((origin, carrier), delay): (origin, (carrier, delay)))\
             .groupByKey()\
             .map(lambda (origin, flight): (origin, sorted(flight, key=lambda (carrier, delay): carrier)[:10]))\

    digest.foreachRDD(lambda rdd: rdd.foreachPartition(cassandraStore))

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    lines.pprint()

    ssc.start()
    ssc.awaitTermination()
