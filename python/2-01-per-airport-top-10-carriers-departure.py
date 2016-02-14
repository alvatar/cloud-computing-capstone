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
        batch = ''.join(["insert into top_carriers (airport, c01, c02, c03, c04, c05, c06, c07, c08, c09, c10) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s') " % \
                         tuple([str(record[0])] + [str(val).replace('\'','') for val in record[1] + ['None']*(10 - len(record[1]))])
                         for record in iter])
        cassandra.execute("BEGIN BATCH\n" + batch + "APPLY BATCH")
        cassandra.shutdown()

    # Generate a (Origin, Carrier) -> DepDelay mapping
    # Moving average DepDelay
    # Remap key: Origin -> (Carrier, DepDelay)
    # Sort and select the 10 best
    digest = ks.map(processInput)\
             .updateStateByKey(movingAvg)\
             .map(lambda ((origin, carrier), (delay, count)): (origin, (carrier, delay)))\
             .groupByKey()\
             .map(lambda (origin, flight): (origin, sorted(flight, key=lambda (carrier, delay): delay)[:10]))\

    digest.foreachRDD(lambda rdd: rdd.foreachPartition(cassandraStore))
    digest.pprint()

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    ssc.start()
    ssc.awaitTermination()
