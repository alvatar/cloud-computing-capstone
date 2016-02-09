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

    checkpointDirectory = "spark-checkpoint"

    def createContext():
        sc = SparkContext(appName="PythonStreamingKafkaTest")
        sc.setLogLevel("WARN")
        ssc = StreamingContext(sc, 1)
        ssc.checkpoint(checkpointDirectory)
        return ssc

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    context = StreamingContext(sc, 1)
    context.checkpoint("checkpoint")

    #context = StreamingContext.getOrCreate(checkpointDirectory, createContext)

    ks = KafkaUtils.createDirectStream(context, [topic], {"metadata.broker.list": brokers})

    def producePerOriginOrDest(line):
        return ((airport, 1) for airport in line[1].split("\t")[1:2])

    def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)

    res = ks.flatMap(producePerOriginOrDest)\
            .updateStateByKey(updateFunction)

    res.pprint()

    # def toCSVLine(data):
    #     return ','.join('"' + str(d) + '"' for d in data)
    # lines = ks.map(toCSVLine)

    context.start()
    context.awaitTermination()
