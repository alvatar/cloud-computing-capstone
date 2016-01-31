USE airports;

TRUNCATE trip_combinations;

COPY trip_combinations (
                        origin_date,
                        origin_date_time,
                        origin_carrier,
                        origin_flight,
                        origin,
                        connection,
                        connection_arr_delay,
                        connection_date,
                        connection_date_time,
                        connection_carrier,
                        connection_flight,
                        destination,
                        destination_arr_delay
                        ) FROM 'traveler.csv';

EXIT;
