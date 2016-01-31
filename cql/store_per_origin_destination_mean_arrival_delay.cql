USE airports;

TRUNCATE mean_delays;

COPY mean_delays (origin, destination, mean_delay) FROM 'per_origin_destination_mean_arrival_delay/part-r-00000';

EXIT;
