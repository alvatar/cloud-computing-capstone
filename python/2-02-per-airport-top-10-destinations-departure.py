from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    brokers, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    context = StreamingContext(sc, 2)
    context.checkpoint("checkpoint")

    ks = KafkaUtils.createDirectStream(context, [topic], {"metadata.broker.list": brokers})

    def producePerFlight(line):
        val = line[1].split("\t")
        # Origin, Destination, DepDelay
        return((val[6], val[7]), float(val[8]))

    def movingAvg(newValues, movingAvg):
        if movingAvg is None:
            movingAvg = (0,0)
        prevAvg, prevN = movingAvg
        currentN = len(newValues)
        return (float(prevAvg*prevN + sum(newValues)) / (prevN + currentN), prevN + currentN)

    digest = ks.map(producePerFlight)\
             .updateStateByKey(movingAvg)\
             .map(lambda x: (x[0][0], (x[0][1], x[1][0])))\
             .groupByKey()\
             .map(lambda x: (x[0], sorted(x[1], key=lambda val: val[1])[:10]))

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    digest.pprint()

    context.start()
    context.awaitTermination()
