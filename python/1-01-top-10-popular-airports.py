from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: script.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")

    # numStreams = 6
    # kss = [KafkaUtils.createStream(ssc,\
    #                                zkQuorum,\
    #                                "spark-streaming-consumer",\
    #                                {topic: 12})\
    #                                for _ in range (numStreams)]
    # ks = ssc.union(*kss)

    #ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 12})

    ks = ssc.socketTextStream("localhost", 9999)

    def producePerOriginOrDest(line):
        year, origin, dest = line[1].split("\t")
        return ((origin, 1), (dest, 1))

    def processInput(line):
        year, origin, dest = line.split("\t")
        return ((origin, 1), (dest, 1))

    def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)

    digest = ks.flatMap(processInput)\
               .updateStateByKey(updateFunction)\
               .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

        
    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    #digest.saveAsTextFiles("/user/ubuntu/testing/see")
    digest.pprint()

    ssc.start()
    ssc.awaitTermination()
