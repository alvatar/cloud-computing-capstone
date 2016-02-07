data = LOAD '/user/ubuntu/clean_on_time' using PigStorage() as
  (Year:int,
  FlightDate:datetime,
  FlightDateTime:datetime,
  DayOfWeek:chararray,
  Carrier:chararray,
  FlightNum:chararray,
  Origin:chararray,
  Dest:chararray,
  DepDelay:int,
  ArrDelay:int);

grp_pairs = GROUP data BY (Origin, Dest);

by_avg = FOREACH grp_pairs GENERATE FLATTEN(group), AVG(data.ArrDelay) as AvgDelay;


STORE by_avg INTO '/user/ubuntu/results/per_origin_destination_mean_arrival_delay'
  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');
