CREATE SCHEMA airports WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 2
};

USE airports;

CREATE TABLE top_carriers ( airport text,
                            carriers text,
                            PRIMARY KEY (airport)
                            );

EXIT;