A = LOAD '/user/ubuntu/clean_on_time_2008' using PigStorage() as
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

B = LOAD '/user/ubuntu/clean_on_time_2008' using PigStorage() as
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


DEFINE SelectMinArr org.apache.pig.piggybank.evaluation.ExtremalTupleByNthField('7', 'min');

origins = FOREACH A GENERATE FlightDate, FlightDateTime, Carrier, FlightNum, Origin, Dest, ArrDelay;
origins = FILTER origins BY (GetHour(FlightDateTime) < 12);
orig_per_day = GROUP origins BY (Origin, Dest, FlightDate);
orig_per_day_best = FOREACH orig_per_day GENERATE FLATTEN( SelectMinArr(origins) );

destinations = FOREACH B GENERATE FlightDate, FlightDateTime, Carrier, FlightNum, Origin, Dest, ArrDelay;
destinations = FILTER destinations BY (GetHour(FlightDateTime) > 12);
dest_per_day = GROUP destinations BY (Origin, Dest, FlightDate);
dest_per_day_best = FOREACH dest_per_day GENERATE FLATTEN( SelectMinArr(destinations) );


DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();

all_combinations = JOIN orig_per_day_best by Dest, dest_per_day_best by Origin USING 'replicated';


trips = FILTER all_combinations BY (DaysBetween(destinations::FlightDateTime, origins::FlightDateTime) == 2);

-- Remove 12th field, since it is repeated after joining
trips_wo_rep = FOREACH trips GENERATE $0, $1, $2, $3, $4, $5 ,$6, $7, $8, $9, $10, $12, $13;


STORE trips_wo_rep INTO '/user/ubuntu/results/traveler'
  USING org.apache.pig.piggybank.storage.CSVExcelStorage(',');