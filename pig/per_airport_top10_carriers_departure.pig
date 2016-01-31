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

grouped_pairs = GROUP data BY (Origin, Carrier);

by_avg = FOREACH grouped_pairs GENERATE FLATTEN(group), AVG(data.DepDelay) as AvgDelay;

by_airport = GROUP by_avg BY Origin;

sorted = FOREACH by_airport {
       by_avg_delay = ORDER by_avg BY AvgDelay ASC;
       by_avg_delay = LIMIT by_avg_delay 10;
       GENERATE group, by_avg_delay.Carrier;
}


STORE sorted INTO '/user/ubuntu/results/per_airport_top10_carriers_departure'
  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');
