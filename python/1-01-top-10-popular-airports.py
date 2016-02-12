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
    #brokers, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 5)
    ssc.checkpoint("checkpoint")

    #ks = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 24})

    def producePerOriginOrDest(line):
        return ((airport, 1) for airport in line[1].split("\t")[1:2])

    def updateFunction(newValues, runningCount):
        if runningCount is None:
            runningCount = 0
        return sum(newValues, runningCount)

    # class DictAccumulatorParam(AccumulatorParam):
    #     def zero(self, init):
    #         return {}

    #     def addInPlace(self, v1, v2):
    #         skey = str(v2[0])
    #         val = int(v2[1])
    #         oval = v1.get(skey, 0) + int(v2[1])
    #         v1[skey] = oval
    #         return v1

    # accum = DictAccumulatorParam({})
    # def addToPrevious(key, newValue):
    #     accum.add([key, newValue])

    counts = {}
    def addToPrevious(key, newValue):
        skey = str(key)
        val = counts.get(skey, 0) + int(newValue)
        counts[skey] = val
        return val
            #.updateStateByKey(updateFunction)\
                #.map(lambda (x, y): (x, (addToPrevious(x, y))))\
                        #.reduceByKey(lambda x, y: x + y)\

    digest = ks.flatMap(producePerOriginOrDest)\
               .updateStateByKey(updateFunction)\
               .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    #digest.saveAsTextFiles("/user/ubuntu/testing/see")
    digest.pprint()

    ssc.start()
    ssc.awaitTermination()
