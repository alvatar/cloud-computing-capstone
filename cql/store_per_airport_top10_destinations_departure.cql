USE airports;

TRUNCATE top_destinations;

COPY top_destinations (airport, destinations) FROM 'results/per_airport_top10_destinations_departure.csv';

EXIT;
