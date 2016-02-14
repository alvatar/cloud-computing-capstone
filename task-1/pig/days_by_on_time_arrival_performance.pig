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

by_day = GROUP data BY DayOfWeek;

by_avg = FOREACH by_day GENERATE group as DayOfWeek, AVG(data.ArrDelay) as AvgDelay;

sorted_by_avg = ORDER by_avg BY AvgDelay ASC;

STORE sorted_by_avg into '/user/ubuntu/results/days_by_on_time_arrival_performance';