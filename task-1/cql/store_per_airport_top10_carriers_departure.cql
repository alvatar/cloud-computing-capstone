USE airports;

TRUNCATE top_carriers;

COPY top_carriers (airport, carriers) FROM 'results/per_airport_top10_carriers_departure.csv';

EXIT;
