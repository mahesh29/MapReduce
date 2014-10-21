 REGISTER file:/home/hadoop/lib/pig/piggybank.jar;
 DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

 -- Set 10 Reducers 
 SET default_parallel 10;
 
 -- Define the source of Data
 -- 'local source' : /Users/mahesh/Documents/workspace/data/mapreduce/3/actual/data.csv
 -- 'AWS source' :
 Flights1 = LOAD 's3://hw3flightchallenge/data/data.csv/' USING CSVLoader;
 Flights2 = LOAD 's3://hw3flightchallenge/data/data.csv/' USING CSVLoader;
 

 
 -- Get Required Fields from the CSV Data
 Flights1 = FOREACH Flights1 GENERATE (chararray)$5 as flightdate, 
                               		  (chararray)$11 as origin, 
                               		  (chararray)$17 as dest, 
                               		  (int) $24 as depTime, 
                               		  (int) $35 as arrTime, 
                               		  (float)$37 as arrDelayMins, 
                               		  (int) $41 as cancelled, 
                               		  (int) $43 as diverted;

 Flights2 = FOREACH Flights2 GENERATE (chararray)$5 as flightdate, 
                               		  (chararray)$11 as origin, 
                               		  (chararray)$17 as dest, 
                               		  (int) $24 as depTime, 
                               		  (int) $35 as arrTime, 
                               		  (float)$37 as arrDelayMins, 
                               		  (int) $41 as cancelled, 
                               		  (int) $43 as diverted;
 
 -- Filter out the Data that that contains cancelled/diverted flight data
 Flights1 = FILTER Flights1 BY (cancelled != 1) AND (diverted!=1);
 Flights2 = FILTER Flights2 BY (cancelled != 1) AND (diverted!=1);
 
 
 -- We have 2 Legs in the Problem Statement
 -- Flights1 = Flight data Originating from ORD but Landing on Airport terminal-X
 -- Flights1 = Flight data Originating from Airport terminal-X but Landing on JFK
 Flights1 = FILTER Flights1 BY (origin == 'ORD' AND dest!='JFK');
 Flights2 = FILTER Flights2 BY (origin != 'ORD' AND dest=='JFK');
 

 
 -- ------------------------------- JOIN -----------------------------------
 -- We Join Flights1(LEG1) and Flights2(LEG2) on FlightDate 
 joinOnDate = JOIN Flights1 BY (dest, flightdate), Flights2 BY (origin, flightdate);
 
 -- We check whether the arrival Time is lesser than the Departure time, on Terminal-X
 arrLessThanDep = FILTER joinOnDate BY $4 < $11;
  -- ------------------------------- --- -----------------------------------



 -- ------------------------------- FILTER -----------------------------------

 -- Now we just filter out records having dates between (end of may'07 and start of june'08)
 -- $0 is the position for the Flights1 Flight's date, for arrival to terminal-X
 -- $8 is the position for the Flights2 Flight's date, for departure from terminal-X (*Removed*)
 lastJoin = FILTER arrLessThanDep BY ToDate($0,'yyyy-MM-dd') > ToDate('2007-05-31', 'yyyy-MM-dd') 
	 								 AND ToDate($0, 'yyyy-MM-dd') < ToDate('2008-06-01', 'yyyy-MM-dd');
 
 result = FOREACH lastJoin GENERATE (float)($5 + $13) as sumOfDelays;
 
  -- ------------------------------- --- -----------------------------------
 
 -- Group the final 'result' Relation 
 gResult = GROUP result ALL;
 
 -- Iterate through the grouped result and Run Aggregate function AVG on it
 average_delay = FOREACH gResult GENERATE AVG(result.sumOfDelays);
 
 --dump average_delay;
 STORE average_delay INTO 's3://hw3flightchallenge/outputs/2s3://hw3flightchallenge/data/data.csv//' USING PigStorage();