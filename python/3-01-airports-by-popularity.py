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
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("checkpoint")

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 42})

    def processInput(line):
        fields = line[1].split("\t")
        return ((str(fields[6]), 1), (str(fields[7]), 1))

    def updateFunction(newValues, runningCount):
        return sum(newValues, runningCount or 0)

    digest = ks.flatMap(processInput)\
               .updateStateByKey(updateFunction)\
               .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False)\
                                         .map(lambda (x, y): y).zipWithIndex().map(lambda (x, y): (y, x))
               )

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    lines.saveAsTextFiles("3-01-airports-by-popularity/output")
    digest.pprint()

    ssc.start()
    ssc.awaitTermination()
