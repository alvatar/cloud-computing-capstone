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
        # (Origin, Destination), DepDelay
        return((str(fields[6]), str(fields[7])), float(fields[8]))

    def movingAvg(newValues, movingAvg):
        prevAvg, prevN = movingAvg or (0,0)
        currentN = len(newValues)
        return (float(prevAvg*prevN + sum(newValues)) / (prevN + currentN), prevN + currentN)

    def cassandraStore(iter):
        cassandraCluster = Cluster(contact_points=[cassandraAddr])
        cassandra = cassandraCluster.connect('spark')
        batch = ''.join(["insert into top_destinations (airport, d01, d02, d03, d04, d05, d06, d07, d08, d09, d10) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') " % \
                         tuple([str(record[0])] + [str(val).replace('\'','') for val in record[1] + ['None']*(10 - len(record[1]))])
                         for record in iter])
        cassandra.execute("BEGIN BATCH\n" + batch + "APPLY BATCH")
        cassandra.shutdown()

    # Generate a (Origin, Dest) -> DepDelay mapping
    # Moving average DepDelay
    # Remap key: Origin -> (Dest, DepDelay)
    # Sort and select the 10 best
    digest = ks.map(processInput)\
             .updateStateByKey(movingAvg)\
             .map(lambda ((origin, dest), (delay, count)): (origin, (dest, delay)))\
             .groupByKey()\
             .map(lambda (origin, flight): (origin, sorted(flight, key=lambda (dest, delay): delay)[:10]))\

    digest.foreachRDD(lambda rdd: rdd.foreachPartition(cassandraStore))
    digest.pprint()

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    ssc.start()
    ssc.awaitTermination()
