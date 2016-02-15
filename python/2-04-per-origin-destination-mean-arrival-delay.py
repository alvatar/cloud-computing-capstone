from __future__ import print_function

import sys

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
        # (Origin, Destination), ArrDelay
        return((str(fields[6]), str(fields[7])), float(fields[9]))

    def movingAvg(newValues, movingAvg):
        prevAvg, prevN = movingAvg or (0,0)
        currentN = len(newValues)
        return (float(prevAvg*prevN + sum(newValues)) / (prevN + currentN), prevN + currentN)

    def cassandraStore(iter):
        cassandraCluster = Cluster(contact_points=[cassandraAddr])
        cassandra = cassandraCluster.connect('spark')
        batch = ''.join(["insert into mean_arrivals (origin, destination, mean_arrival) values ('%s', '%s', %s) " % \
                         (record[0], record[1], record[2]) for record in iter])
        cassandra.execute("BEGIN BATCH\n" + batch + "APPLY BATCH")
        cassandra.shutdown()

    digest = ks.map(processInput)\
             .updateStateByKey(movingAvg)\
             .map(lambda ((origin, destination), (mean_arrival, count)): (origin, destination, mean_arrival))

    # def toCSVLine(data):
    #     return ','.join('"' + str(d) + '"' for d in data)
    # lines = digest.map(toCSVLine)

    digest.foreachRDD(lambda rdd: rdd.foreachPartition(cassandraStore))
    digest.pprint()

    ssc.start()
    ssc.awaitTermination()
