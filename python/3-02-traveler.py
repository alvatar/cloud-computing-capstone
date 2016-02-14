from __future__ import print_function

import sys
from datetime import datetime

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <zk> <topic> <cassandra>", file=sys.stderr)
        exit(-1)

    brokers, topic = sys.argv[1:]

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    context = StreamingContext(sc, 2)
    context.checkpoint("checkpoint")

    ks = KafkaUtils.createDirectStream(context, [topic], {"metadata.broker.list": brokers})

    # 0 Year:int,
    # 1 FlightDate:datetime,
    # 2 FlightDateTime:datetime,
    # 3 DayOfWeek:chararray,
    # 4 Carrier:chararray,
    # 5 FlightNum:chararray,
    # 6 Origin:chararray,
    # 7 Dest:chararray,
    # 8 DepDelay:int,
    # 9 ArrDelay:int);

    def formatDates(data):
        flight = data[1].split("\t")
        depDate = datetime.strptime(flight[1], "%Y-%m-%d")
        depDateTime = datetime.strptime(flight[2], "%Y-%m-%dT%H:%M:%S.%fZ")
        return (flight[6], flight[7], depDateTime), [int(flight[0]),
                                                     depDate,
                                                     depDateTime,
                                                     int(flight[3]),
                                                     flight[4],
                                                     flight[5],
                                                     flight[6],
                                                     flight[7],
                                                     float(flight[8]),
                                                     float(flight[9])]

    origins = ks.map(formatDates)\
                .filter(lambda x: x[0][2].hour < 12)\
                .groupByKey()\
                .map(lambda x: (x[0], sorted(x[1], key=lambda val: val[9])[0]))\
                .map(lambda x: (x[0][1], x[1])) # Use only destination as key

    destinations = ks.map(formatDates)\
                .filter(lambda x: x[0][2].hour >= 12)\
                .groupByKey()\
                .map(lambda x: (x[0], sorted(x[1], key=lambda val: val[9])[0]))\
                .map(lambda x: (x[0][0], x[1])) # Use only origin as key

    digest = origins.join(destinations)\
                      .filter(lambda (key, (first, second)): (second[1]-first[1]).days == 2)\
                      .map(lambda (key, (first, second)): ((first[6], first[7], second[7], first[2]),
                                                           (first[0],
                                                            first[4])))

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = digest.map(toCSVLine)

    digest.pprint()

    context.start()
    context.awaitTermination()
