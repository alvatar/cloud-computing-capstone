from __future__ import print_function

import eventlet
eventlet.monkey_patch()

import time

import sys
from calendar import timegm
from datetime import datetime
import itertools

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import cassandra
from cassandra.cluster import Cluster

from cassandra.io import twistedreactor


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <zk> <topic> <cassandra>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic, cassandraConf = sys.argv[1:]
    cassandraHosts = cassandraConf.split(",")
    interval = 6
    window = 12

    useCassandra = True
    if useCassandra:
        print("Connecting to Cassandra instances %s" % cassandraHosts)

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, interval)
    #ssc.checkpoint("checkpoint")

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 32})

    ssc.remember(interval*2)

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

    isoFormatDate = "%Y-%m-%d"
    isoFormatTime = "%Y-%m-%dT%H:%M:%S.%fZ"

    def formatDates(data):
        flight = data[1].split("\t")
        depDate = datetime.strptime(flight[1], isoFormatDate)
        depDateTime = datetime.strptime(flight[2], isoFormatTime)
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

    # Result should only destination as key
    origins = ks.window(window, interval)\
                .map(formatDates)\
                .filter(lambda ((origin, dest, depTime), flight): depTime.hour < 12)\
                .groupByKey()\
                .map(lambda ((origin, dest, depTime), flights): (dest, sorted(flights, key=lambda fields: fields[9])[0]))\

    # Result should only origin as key
    destinations = ks.window(window, interval)\
                     .map(formatDates)\
                     .filter(lambda ((origin, dest, depTime), flight): depTime.hour >= 12)\
                     .groupByKey()\
                     .map(lambda ((origin, dest, depTime), flights): (origin, sorted(flights, key=lambda fields: fields[9])[0]))\

    combinations = origins.join(destinations)\
                          .filter(lambda (key, (first, second)): (second[1]-first[1]).days == 2)\

    ks.count().pprint()

    pretty = combinations.map(lambda (key, (first, second)): (first[1].strftime(isoFormatDate),
                                                              first[2].strftime(isoFormatTime),
                                                              str(first[4]),
                                                              str(first[5]),
                                                              str(first[6]),
                                                              str(first[7]),
                                                              float(first[9]),
                                                              second[1].strftime(isoFormatDate),
                                                              second[2].strftime(isoFormatTime),
                                                              str(second[4]),
                                                              str(second[5]),
                                                              str(second[7]),
                                                              float(second[9])))

    if useCassandra:
        toCassandra = combinations.map(lambda (key, (first, second)): (timegm(first[1].timetuple()) * 1000,
                                                                       timegm(first[2].timetuple()) * 1000,
                                                                       str(first[4]),
                                                                       str(first[5]),
                                                                       str(first[6]),
                                                                       str(first[7]),
                                                                       float(first[9]),
                                                                       timegm(second[1].timetuple()) * 1000,
                                                                       timegm(second[2].timetuple()) * 1000,
                                                                       str(second[4]),
                                                                       str(second[5]),
                                                                       str(second[7]),
                                                                       float(second[9])))

    def groupsgen(seq, size):
        it = iter(seq)
        while True:
            values = ()
            for n in xrange(size):
                values += (it.next(),)
                yield values

    def cassandraStoreBatch(iter):
        cassandraCluster = Cluster(contact_points=cassandraHosts,
                                   #connection_class=twistedreactor.TwistedConnection,
                                   connect_timeout=60,
                               )
        cas = cassandraCluster.connect('spark')
        for b in groupsgen(iter, 100):
            cBa = ''.join(["insert into trip_combinations "
                             "(origin_date, origin_date_time, origin_carrier, origin_flight, origin, connection, connection_arr_delay, "
                             "connection_date, connection_date_time, connection_carrier, connection_flight, destination, destination_arr_delay) "
                             "values ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', %s)\n" % \
                             record for record in b])
            cas.execute_async("BEGIN BATCH\n" + cBa + "APPLY BATCH")
            time.sleep(0.001)
            # cassandra.execute("insert into test (k, v) values ('as', 's')")
        cas.shutdown()

    def cassandraStoreSingle(iter):
        cassandraCluster = Cluster(contact_points=cassandraHosts,
                                   #connection_class=geventreactor.GeventConnection,
                                   connect_timeout=60,
                               )
        cas = cassandraCluster.connect('spark')
        for record in iter:
            cas.execute_async("insert into trip_combinations "
                              "(origin_date, origin_date_time, origin_carrier, origin_flight, origin, connection, connection_arr_delay, "
                              "connection_date, connection_date_time, connection_carrier, connection_flight, destination, destination_arr_delay) "
                             "values ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', %s)\n" % \
                              record)
        cas.shutdown()

    def toCSVLine(data):
        return ','.join('"' + str(d) + '"' for d in data)
    lines = pretty.map(toCSVLine)

    pretty.pprint()

    if useCassandra:
        toCassandra.foreachRDD(lambda rdd: rdd.foreachPartition(cassandraStoreBatch))
    else:
        lines.saveAsTextFiles("3-02-traveler/output")

    ssc.start()
    ssc.awaitTermination()
