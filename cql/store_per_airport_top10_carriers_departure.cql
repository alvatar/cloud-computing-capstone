USE airports;

TRUNCATE top_carriers;

COPY top_carriers (airport, carriers) FROM 'per_airport_top10_carriers_departure/part-r-00000';

EXIT;
