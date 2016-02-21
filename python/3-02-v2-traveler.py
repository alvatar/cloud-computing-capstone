from __future__ import print_function

import time

import sys
from calendar import timegm
from datetime import datetime
from datetime import timedelta
import itertools

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import cassandra
from cassandra.cluster import Cluster
from cassandra.query import named_tuple_factory


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

def toStdout(key, (first, second)):
    return (first[1].strftime(isoFormatDate),
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
            float(second[9]))

def toCassandra(key, (first, second)):
    (timegm(first[1].timetuple()) * 1000,
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
     float(second[9]))


def groupsgen(seq, size):
    it = iter(seq)
    while True:
        values = ()
        for n in xrange(size):
            values += (it.next(),)
            yield values

def cassandraStoreBatch(iter):
    cassandraCluster = Cluster(contact_points=cassandraHosts)
    cas = cassandraCluster.connect('spark')
    for b in groupsgen(iter, 100):
        cBa = ''.join(["insert into trip_combinations "
                       "(origin_date, origin_date_time, origin_carrier, origin_flight, origin, connection, connection_arr_delay, "
                       "connection_date, connection_date_time, connection_carrier, connection_flight, destination, destination_arr_delay) "
                       "values ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', %s)\n" % \
                       record for record in b])
        cas.execute_async("BEGIN BATCH\n" + cBa + "APPLY BATCH")
    cas.shutdown()

def cassandraStoreSingle(iter):
    cassandraCluster = Cluster(contact_points=cassandraHosts)
    cas = cassandraCluster.connect('spark')
    for record in iter:
        cas.execute_async("insert into trip_combinations "
                          "(origin_date, origin_date_time, origin_carrier, origin_flight, origin, connection, connection_arr_delay, "
                          "connection_date, connection_date_time, connection_carrier, connection_flight, destination, destination_arr_delay) "
                          "values ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', %s)\n" % \
                          record)
    cas.shutdown()

def cassandraStoreOrigins(iter):
    cassandraCluster = Cluster(contact_points=cassandraHosts)
    cas = cassandraCluster.connect('spark')
    for b in groupsgen(iter, 100):
        cBa = ''.join(["insert into trip_origins "
                       "(date, date_time, carrier, flight, origin, destination, arrival_delay)"
                       "values (%s, %s, '%s', '%s', '%s', '%s', %s)\n" % \
                       record for record in b])
        cas.execute("BEGIN BATCH\n" + cBa + "APPLY BATCH")
    cas.shutdown()

def cassandraStoreDestinations(iter):
    cassandraCluster = Cluster(contact_points=cassandraHosts)
    cas = cassandraCluster.connect('spark')
    for b in groupsgen(iter, 100):
        cBa = ''.join(["insert into trip_destinations "
                       "(date, date_time, carrier, flight, origin, destination, arrival_delay)"
                       "values (%s, %s, '%s', '%s', '%s', '%s', %s)\n" % \
                       record for record in b])
        cas.execute("BEGIN BATCH\n" + cBa + "APPLY BATCH")
    cas.shutdown()

def makeCombinations(iter):
    cassandraCluster = Cluster(contact_points=cassandraHosts)
    cas = cassandraCluster.connect('spark')
    cas.row_factory = named_tuple_factory
    for second in iter:
        targetDate = datetime.fromtimestamp(second[0]/1000) + timedelta(days=-2)
        result = cas.execute("SELECT * FROM trip_origins WHERE destination = '%s' AND date = '%s'" % (second[4], targetDate.isoformat()))
        for first in result:
            cas.execute_async(
                "insert into trip_combinations "
                "(origin_date, origin_date_time, origin_carrier, origin_flight, origin, connection, connection_arr_delay, "
                "connection_date, connection_date_time, connection_carrier, connection_flight, destination, destination_arr_delay) "
                "values ('%s', '%s', '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', %s)\n" % \
                (first.date, first.date_time, first.carrier, first.flight, first.origin, first.destination, first.arrival_delay,
                 second[0], second[1], second[2], second[3], second[5], second[6])
                )
    cas.shutdown()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: script.py <zk> <topic> <cassandra>", file=sys.stderr)
        exit(-1)

    zkQuorum, topic, cassandraConf = sys.argv[1:]
    cassandraHosts = cassandraConf.split(",")

    sc = SparkContext(appName="KafkaSparkStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 10)
    #ssc.checkpoint("checkpoint")

    ks = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 32})

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

    # Result should only destination as key
    origins = ks.map(formatDates)\
                .filter(lambda ((origin, dest, depTime), flight): depTime.hour < 12)\
                .groupByKey()\
                .map(lambda ((origin, dest, depTime), flights): (dest, sorted(flights, key=lambda fields: fields[9])[0]))\
                .map(lambda (k, v):
                     (timegm(v[1].timetuple()) * 1000,
                      timegm(v[2].timetuple()) * 1000,
                      v[4], v[5], v[6], v[7], v[8]))\

    origins.foreachRDD(lambda rdd: rdd.foreachPartition(cassandraStoreOrigins))

    # Result should only origin as key
    destinations = ks.map(formatDates)\
                     .filter(lambda ((origin, dest, depTime), flight): depTime.hour >= 12)\
                     .groupByKey()\
                     .map(lambda ((origin, dest, depTime), flights): (origin, sorted(flights, key=lambda fields: fields[9])[0]))\
                     .map(lambda (k, v):
                          (timegm(v[1].timetuple()) * 1000,
                           timegm(v[2].timetuple()) * 1000,
                           v[4], v[5], v[6], v[7], v[8]))\

    destinations.foreachRDD(lambda rdd: rdd.foreachPartition(cassandraStoreDestinations))
    destinations.foreachRDD(lambda rdd: rdd.foreachPartition(makeCombinations))

    origins.pprint()

    ssc.start()
    ssc.awaitTermination()
