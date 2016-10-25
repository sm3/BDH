-- ***************************************************************************
-- Loading Data:
-- create external table mapping for events.csv and mortality_events.csv

-- IMPORTANT NOTES:
-- You need to put events.csv and mortality.csv under hdfs directory 
-- '/input/events/events.csv' and '/input/mortality/mortality.csv'
-- 
-- To do this, run the following commands for events.csv, 
-- 1. sudo su - hdfs
-- 2. hdfs dfs -mkdir -p /input/events
-- 3. hdfs dfs -chown -R vagrant /input
-- 4. exit 
-- 5. hdfs dfs -put /path/to/events.csv /input/events/
-- Follow the same steps 1 - 5 for mortality.csv, except that the path should be 
-- '/input/mortality'
-- ***************************************************************************
-- create events table 
DROP TABLE IF EXISTS events;
CREATE EXTERNAL TABLE events (
  patient_id STRING,
  event_id STRING,
  event_description STRING,
  time DATE,
  value DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/events';

-- create mortality events table 
DROP TABLE IF EXISTS mortality;
CREATE EXTERNAL TABLE mortality (
  patient_id STRING,
  time DATE,
  label INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/mortality';

-- ******************************************************
-- Task 1:
-- By manipulating the above two tables, 
-- generate two views for alive and dead patients' events
-- ******************************************************
-- find events for alive patients
DROP VIEW IF EXISTS alive_events;

-- ***** your code below *****

CREATE VIEW alive_events
AS
SELECT e.patient_id, e.event_id, e.time FROM events e
WHERE NOT EXISTS (SELECT m.patient_id FROM mortality m WHERE e.patient_id = m.patient_id);






-- find events for dead patients
DROP VIEW IF EXISTS dead_events;

-- ***** your code below *****

CREATE VIEW dead_events
AS
SELECT e.patient_id, e.event_id, e.time FROM events e
WHERE EXISTS (SELECT m.patient_id FROM mortality m WHERE e.patient_id = m.patient_id);






-- ************************************************
-- Task 2: Event count metrics
-- Compute average, min and max of event counts 
-- for alive and dead patients respectively  
-- ************************************************

-- ***** your code below *****

select avg(a.event_count), min(a.event_count), max(a.event_count)
FROM (select count(*) as event_count FROM alive_events GROUP BY patient_id)a;


-- ***** your code below *****


select avg(a.event_count), min(a.event_count), max(a.event_count)
FROM (select count(*) as event_count FROM dead_events GROUP BY patient_id)a;


-- ************************************************
-- Task 3: Encounter count metrics 
-- Compute average, min and max of encounter counts 
-- for alive and dead patients respectively
-- ************************************************

-- ***** your code below *****

select avg(a.encounter_count), min(a.encounter_count), max(a.encounter_count)
FROM (select count(distinct time) as encounter_count FROM alive_events GROUP BY patient_id)a;


-- dead

-- ***** your code below *****

select avg(d.encounter_count), min(d.encounter_count), max(d.encounter_count)
FROM (select count(distinct time) as encounter_count FROM dead_events GROUP BY patient_id)d;




-- ************************************************
-- Task 4: Record length metrics
-- Compute average, min and max of record lengths
-- for alive and dead patients respectively
-- ************************************************

-- ***** your code below *****


select avg(a.diffdate), min(a.diffdate), max(a.diffdate) FROM
 (SELECT datediff (t1.maxtime, t1.mintime) as diffdate FROM
 (SELECT patient_id, DATE(min(time)) AS mintime, DATE(max(time)) AS maxtime FROM alive_events GROUP BY patient_id)t1 )a;



-- ***** your code below *****

select avg(a.diffdate), min(a.diffdate), max(a.diffdate) FROM
 (SELECT datediff (t1.maxtime, t1.mintime) as diffdate FROM
 (SELECT patient_id, DATE(min(time)) AS mintime, DATE(max(time)) AS maxtime FROM dead_events GROUP BY patient_id)t1 )a;





-- ******************************************* 
-- Task 5: Common diag/lab/med
-- Compute the 5 most frequently occurring diag/lab/med
-- for alive and dead patients respectively
-- *******************************************
-- alive patients
---- diag
--SELECT event_id, count(*) AS diag_count FROM alive_events
-- ***** your code below *****
SELECT a.event_id, a.diag_count from (SELECT event_id, count(*) AS diag_count
FROM alive_events WHERE event_id like "DIAG%" GROUP BY event_id)a ORDER BY a.diag_count DESC LIMIT 5;



---- lab
--SELECT event_id, count(*) AS lab_count FROM alive_events
-- ***** your code below *****
SELECT a.event_id, a.lab_count from (SELECT event_id, count(*) AS lab_count
FROM alive_events WHERE event_id like "LAB%" GROUP BY event_id)a ORDER BY a.lab_count DESC LIMIT 5;

---- med
-- SELECT event_id, count(*) AS med_count FROM alive_events
-- ***** your code below *****
SELECT a.event_id, a.med_count from (SELECT event_id, count(*) AS med_count
FROM alive_events WHERE event_id like "DRUG%" GROUP BY event_id)a ORDER BY a.med_count DESC LIMIT 5;



-- dead patients
---- diag
-- SELECT event_id, count(*) AS diag_count FROM dead_events
-- ***** your code below *****
SELECT a.event_id, a.diag_count from (SELECT event_id, count(*) AS diag_count
FROM dead_events WHERE event_id like "DIAG%" GROUP BY event_id)a ORDER BY a.diag_count DESC LIMIT 5;

---- lab
--SELECT event_id, count(*) AS lab_count FROM dead_events
-- ***** your code below *****
SELECT a.event_id, a.lab_count from (SELECT event_id, count(*) AS lab_count
FROM dead_events WHERE event_id like "LAB%" GROUP BY event_id)a ORDER BY a.lab_count DESC LIMIT 5;

---- med
--SELECT event_id, count(*) AS med_count FROM dead_events
-- ***** your code below *****
SELECT a.event_id, a.med_count from (SELECT event_id, count(*) AS med_count
FROM dead_events WHERE event_id like "DRUG%" GROUP BY event_id)a ORDER BY a.med_count DESC LIMIT 5;









