USE airports;

TRUNCATE top_destinations;

COPY top_destinations (airport, destinations) FROM 'per_airport_top10_destinations_departure/part-r-00000';

EXIT;
