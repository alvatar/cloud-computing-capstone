/*
A = LOAD '/user/ubuntu/sources_origin_destination/*.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') as
  (ItinID,
  MktID,
  SeqNum,
  Coupons,
  Year:int,
  Quarter,
  Origin:chararray,
  OriginAptInd,
  OriginCityNum,
  OriginCountry,
  OriginStateFips,
  OriginState,
  OriginStateName,
  OriginWac,
  Dest:chararray,
  DestAptInd,
  DestCityNum,
  DestCountry,
  DestStateFips,
  DestState,
  DestStateName,
  DestWac,
  Break,
  CouponType,
  TkCarrier,
  OpCarrier,
  RPCarrier,
  Passengers,
  FareClass,
  Distance,
  DistanceGroup,
  Gateway,
  ItinGeoType,
  CouponGeoType);

A = FILTER A BY Year > 1980;
A = FOREACH A GENERATE Year, Origin, Dest;

STORE A into '/user/ubuntu/clean_origin_destination';
*/

B = LOAD '/user/ubuntu/sources_on_time/*.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',') as
  (Year:int,
  Quarter,
  Month:chararray,
  DayOfMonth:int,
  DayOfWeek:chararray,
  FlightDate,
  UniqueCarrier:chararray,
  AirlineID,
  Carrier,
  TailNum,
  FlightNum,
  Origin:chararray,
  OriginCityName,
  OriginState,
  OriginStateFips,
  OriginStateName,
  OriginWac,
  Dest:chararray,
  DestCityName,
  DestState,
  DestStateFips,
  DestStateName,
  DestWac,
  CRSDepTime,
  DepTime,
  DepDelay:float,
  DepDelayMinutes,
  DepDel15,
  DepartureDelayGroups,
  DepTimeBlk,
  TaxiOut,
  WheelsOff,
  WheelsOn,
  TaxiIn,
  CRSArrTime,
  ArrTime,
  ArrDelay:float,
  ArrDelayMinutes,
  ArrDel15,
  ArrivalDelayGroups,
  ArrTimeBlk,
  Cancelled,
  CancellationCode,
  Diverted,
  CRSElapsedTime,
  ActualElapsedTime,
  AirTime,
  Flights,
  Distance,
  DistanceGroup,
  CarrierDelay,
  WeatherDelay,
  NASDelay,
  SecurityDelay,
  LateAircraftDelay);

DEFINE ISOToUnix org.apache.pig.piggybank.evaluation.datetime.convert.ISOToUnix();
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO();

ok_dates = FILTER B BY (Year > 1980)
                       --AND (CRSDepTime is not null)
                       AND (((int)CRSDepTime) < 2400)
                       --AND (NOT (CRSDepTime MATCHES '..[6-9].'))
                       ;

Z = FOREACH ok_dates GENERATE
            Year,
            FlightDate,
            -- Combined Flight date with expected departure time
            UnixToISO(ISOToUnix(FlightDate) + ISOToUnix(ToString(ToDate(CRSDepTime, 'HHmm')))),
            DayOfWeek,
            UniqueCarrier,
            FlightNum,
            Origin,
            Dest,
            (DepDelay is null ? 0.0 : DepDelay),
            (ArrDelay is null ? 0.0 : ArrDelay);

STORE Z into '/user/ubuntu/clean_on_time';


ok_dates_2008 = FILTER ok_dates BY Year == 2008;

ZZ = FOREACH ok_dates_2008 GENERATE
            Year,
            FlightDate,
            -- Combined Flight date with expected departure time
            UnixToISO(ISOToUnix(FlightDate) + ISOToUnix(ToString(ToDate(CRSDepTime, 'HHmm')))),
            DayOfWeek,
            UniqueCarrier,
            FlightNum,
            Origin,
            Dest,
            (DepDelay is null ? 0.0 : DepDelay),
            (ArrDelay is null ? 0.0 : ArrDelay);

STORE ZZ into '/user/ubuntu/clean_on_time_2008';