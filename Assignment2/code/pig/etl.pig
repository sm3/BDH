-- ***************************************************************************
-- Aggregate events into features of patient and generate training, testing data
-- for mortality prediction. Steps have been provided to guide you. 
-- You can include as many intermediate steps as required to complete the calculations.
-- ***************************************************************************

-- ***************************************************************************
-- TESTS
-- To test, please change the LOAD path for events and mortality to ../../test/events.csv and ../../test/mortality.csv
-- 6 tests have been provided to test all the subparts in this exercise.
-- Manually compare the output of each test against the csv's in test/expected folder.
-- ***************************************************************************

-- register a python UDF for converting data into SVMLight format
REGISTER utils.py USING jython AS utils;

-- load events file 
events = LOAD '../../data/events.csv' USING PigStorage(',') AS (patientid:int, eventid:chararray, eventdesc:chararray, timestamp:chararray, value:float);

-- select required columns from events
events = FOREACH events GENERATE patientid, eventid, ToDate(timestamp, 'yyyy-MM-dd') AS etimestamp, value;

-- load mortality files
mortality = LOAD '../../data/mortality.csv' USING PigStorage(',') as (patientid:int, timestamp:chararray, label:int);

mortality = FOREACH mortality GENERATE patientid, ToDate(timestamp, 'yyyy-MM-dd') AS mtimestamp, label;

--DISPLAY mortality (can remove this statement)


-- ***************************************************************************
-- Compute the index dates for dead and alive patients
-- ***************************************************************************


--eventswithmort = -- perform join of events and mortality by patientid;



events_m = JOIN events by patientid FULL, mortality by patientid;
eventswithmort = FOREACH events_m GENERATE  $0 AS patientid, $1 as eventid, $2 AS timestamp, $3 AS value,  $5 AS time, ($6 IS NOT NULL ? $6 :0) as label;


--deadevents = -- detect the events of dead patients and create it of the form (patientid, eventid, value, label, time_difference) where time_difference is the days between index date and each event timestamp



deadevents = FILTER eventswithmort BY label == 1;
deadevents = FOREACH deadevents GENERATE $0 AS patientid, $1 as eventid, $2 as timestamp, $3 AS value, $4 AS time, $5 AS label , ($4 IS NOT NULL ?  (chararray)'P30D':(chararray)'P30D') as duration;

deadevents = FOREACH deadevents GENERATE $0 AS patientid, $1 as eventid, $2 as timestamp, $3 AS value, $5 AS label , SubtractDuration($4, $6) AS indx_date;
deadevents = FOREACH deadevents GENERATE $0 AS patientid, $1 as eventid, $3 AS value, $4 AS label , DaysBetween($5,$2) AS time_difference;



--aliveevents = -- detect the events of alive patients and create it of the form (patientid, eventid, value, label, time_difference) where time_difference is the days between index date and each event timestamp

filteralive= FILTER eventswithmort BY label == 0;
--raw = GROUP filteralive by $0;
--maxalive = FOREACH raw GENERATE group, MAX(filteralive.timestamp);

r1 = foreach (group filteralive by $0) generate group, MAX(filteralive.timestamp) as max_datetime;

aliveevents = JOIN filteralive by patientid FULL, r1 by $0;
aliveevents = FOREACH aliveevents GENERATE $0 AS patientid, $1 as eventid, $3 AS value, $5 AS label, DaysBetween($7, $2) AS time_difference;




--TEST-1
deadevents = ORDER deadevents BY patientid, eventid;
aliveevents = ORDER aliveevents BY patientid, eventid;
STORE aliveevents INTO 'aliveevents' USING PigStorage(',');
STORE deadevents INTO 'deadevents' USING PigStorage(',');

-- ***************************************************************************

-- Filter events within the observation window and remove events with missing values
-- ***************************************************************************
--filtered = -- contains only events for all patients within an observation window of 2000 days and is of the form (patientid, eventid, value, label, time_difference)
filtered = UNION aliveevents, deadevents;
filtered = FILTER filtered BY time_difference < 2000;
filtered = FILTER filtered BY value is not null;

--TEST-2
filteredgrpd = GROUP filtered BY 1;
filtered = FOREACH filteredgrpd GENERATE FLATTEN(filtered);
filtered = ORDER filtered BY patientid, eventid,time_difference;
STORE filtered INTO 'filtered' USING PigStorage(',');


-- ***************************************************************************
-- Aggregate events to create features
-- ***************************************************************************
--featureswithid = -- for group of (patientid, eventid), count the number of  events occurred for the patient and create it (patientid, eventid, featurevalue)

featureswithid = GROUP filtered BY (patientid, eventid);
featureswithid = FOREACH featureswithid GENERATE group.$0, group.$1 as eventid, COUNT(filtered.eventid) AS featurevalue;


--TEST-3
featureswithid = ORDER featureswithid BY patientid, eventid;
STORE featureswithid INTO 'features_aggregate' USING PigStorage(',');

-- ***************************************************************************
-- Generate feature mapping
-- ***************************************************************************
--all_features = -- compute the set of distinct eventids obtained from previous step and rank features by eventid to create (idx, eventid)


feature_names = FOREACH featureswithid GENERATE eventid;
feature_names = DISTINCT feature_names;
all_features = RANK feature_names by eventid DENSE;
all_features = FOREACH all_features GENERATE $0-1 AS index, $1;

-- store the features as output file. The count obtained in the last row from this file will be used to determine input parameter f in train.py
STORE all_features INTO 'features' using PigStorage(' ');

--features = -- perform join of featureswithid and all_features by eventid and replace eventid with idx. It is of the form (patientid, idx, featurevalue)
features = JOIN featureswithid BY eventid , all_features BY eventid;
features = FOREACH features GENERATE $0 AS patientid, all_features::index AS idx, featureswithid::featurevalue AS featurevalue;

--TEST-4
features = ORDER features BY patientid, idx;
STORE features INTO 'features_map' USING PigStorage(',');

-- ***************************************************************************
-- Normalize the values using min-max normalization
-- ***************************************************************************
--maxvalues = -- group events by idx and compute the maximum feature value in each group, is of the form (idx, maxvalues)


x = GROUP features BY idx;
maxvalues = FOREACH x GENERATE group, MAX(features.featurevalue) as maxvalue;
maxvalues = FOREACH maxvalues GENERATE $0 as idx , $1 as maxvalue;
--normalized = -- join features and maxvalues by idx
normalized = JOIN features by idx, maxvalues by idx;

--features = -- compute the final set of normalized features of the form (patientid, idx, normalizedfeaturevalue)
features = FOREACH normalized GENERATE $0 AS patientid, $1 AS idx, (double)featurevalue/maxvalue AS normalizedfeaturevalue;

--TEST-5
features = ORDER features BY patientid, idx;
STORE features INTO 'features_normalized' USING PigStorage(',');

-- ***************************************************************************
-- Generate features in svmlight format 
-- features is of the form (patientid, idx, normalizedfeaturevalue) and is the output of the previous step
-- e.g.  1,1,1.0
--  	 1,3,0.8
--	     2,1,0.5
--       3,3,1.0
-- ***************************************************************************
grpd = GROUP features BY patientid;

grpd_order = ORDER grpd BY $0;

features = FOREACH grpd_order 
{
    sorted = ORDER features BY idx;
    generate group as patientid, utils.bag_to_svmlight(sorted) as sparsefeature;
}

-- ***************************************************************************
-- Split into train and test set
-- labels is of the form (patientid, label) and contains all patientids followed by label of 1 for dead and 0 for alive 
-- e.g. 1,1
--	    2,0
--      3,1
-- ***************************************************************************

--labels = -- create it of the form (patientid, label)
labels = FOREACH filtered GENERATE patientid as patientid, label as label;

samples = JOIN features BY patientid, labels BY patientid;
samples = DISTINCT samples PARALLEL 1;
samples = ORDER samples BY $0;
samples = FOREACH samples GENERATE $3 AS label, $1 AS sparsefeature;


--TEST-6
STORE samples INTO 'samples' USING PigStorage(' ');
-- randomly split data for training and testing

samples = FOREACH samples GENERATE RANDOM() as assignmentkey, *;
SPLIT samples INTO testing IF assignmentkey <= 0.20, training OTHERWISE;
training = FOREACH training GENERATE $1..;
testing = FOREACH testing GENERATE $1..;

-- save training and tesing data
STORE testing INTO 'testing' USING PigStorage(' ');
STORE training INTO 'training' USING PigStorage(' ');