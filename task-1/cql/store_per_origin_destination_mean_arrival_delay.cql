USE airports;

TRUNCATE mean_delays;

COPY mean_delays (origin, destination, mean_delay) FROM 'results/per_origin_destination_mean_arrival_delay.csv';

EXIT;
