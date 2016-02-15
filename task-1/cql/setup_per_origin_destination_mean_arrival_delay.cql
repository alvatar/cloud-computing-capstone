CREATE SCHEMA airports WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 2
};

USE airports;

TRUNCATE mean_delays;

CREATE TABLE mean_delays ( origin text,
                           destination text,
                           mean_delay float,
                           PRIMARY KEY (origin, destination)
                           );

EXIT;