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

    def producePerDay(line):
        val = line[1].split("\t")
        return(val[3], float(val[9]))

    def updateFunction(newValues, movingAvg):
        if movingAvg is None:
            movingAvg = (0,0)
        prevAvg, prevN = movingAvg
        currentN = len(newValues)
        return (float(prevAvg*prevN + sum(newValues)) / (prevN + currentN), prevN + currentN)

    digest = ks.map(producePerDay)\
             .updateStateByKey(updateFunction)\
             .map(lambda x: (x[0], x[1][0]))\
             .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=True))

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    lines.pprint()

    context.start()
    context.awaitTermination()
