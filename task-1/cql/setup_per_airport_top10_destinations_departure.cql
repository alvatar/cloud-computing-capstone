CREATE SCHEMA airports WITH replication = {
  'class' : 'SimpleStrategy',
  'replication_factor' : 2
};

USE airports;

CREATE TABLE top_destinations ( airport text,
                            destinations text,
                            PRIMARY KEY (airport)
                            );

EXIT;
