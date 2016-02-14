A = LOAD '/user/ubuntu/clean_origin_destination' using PigStorage() as
  (Year,
  Origin,
  Dest);

B = LOAD '/user/ubuntu/clean_origin_destination' using PigStorage() as
  (Year,
  Origin,
  Dest);

by_origin = GROUP A BY Origin;
count_origin = FOREACH by_origin GENERATE $0 as Airport, COUNT($1) as Count;

by_dest = GROUP B BY Dest;
count_dest = FOREACH by_dest GENERATE $0 as Airport, COUNT($1) as Count;

count_both = JOIN count_origin BY Airport LEFT, count_dest BY Airport;
count_both = FOREACH count_both GENERATE $0 as Airport, $1 + $3 as Total;

count_ordered = ORDER count_both BY Total DESC;
count_ordered = LIMIT count_ordered 10;


STORE count_ordered into '/user/ubuntu/results/most_popular_airports';
