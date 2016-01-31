CREATE SCHEMA airports WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 2
};

USE airports;

TRUNCATE trip_combinations;

CREATE TABLE trip_combinations ( origin_date timestamp,
                                 origin_date_time timestamp,
                                 origin_carrier text,
                                 origin_flight text,
                                 origin text,
                                 connection text,
                                 connection_arr_delay float,
                                 connection_date timestamp,
                                 connection_date_time timestamp,
                                 connection_carrier text,
                                 connection_flight text,
                                 destination text,
                                 destination_arr_delay float,
                                 PRIMARY KEY (origin, connection, destination, origin_date)
                                 );

EXIT;
