-- set runtime parameters
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.optimize.sort.dynamic.partition=true;
SET hive.auto.convert.join=false;

-- create database if not exists
CREATE DATABASE IF NOT EXISTS events;
-- the current database
SET hivevar:db=events;

-- macro to calculate the distance of user location and event location
CREATE TEMPORARY MACRO locationSimilar(user_location string, event_city string, event_province string, event_country string)
  CASE
    WHEN instr(user_location, event_city) > 0 OR instr(user_location, event_province) > 0 OR instr(user_location, event_country) > 0 THEN 1 ELSE 0
  END;

-- check if train table exists
DROP TABLE IF EXISTS ${db}.hb_train;
-- create train table
CREATE EXTERNAL TABLE ${db}.hb_train(row_key STRING, user_id STRING, event_id STRING, invited STRING, time_stamp STRING, interested STRING)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, eu:user, eu:event, eu:invited, eu:time_stamp, eu:interested')
    TBLPROPERTIES ('hbase.table.name' = 'events_db:train');
-- check if train table exists
DROP TABLE IF EXISTS ${db}.train;
-- create train table
CREATE TABLE ${db}.train
STORED AS ORC AS
    SELECT * FROM ${db}.hb_train;
-- check if train table exists
DROP TABLE IF EXISTS ${db}.hb_train;

-- check if events table exists
DROP TABLE IF EXISTS ${db}.hb_events;
-- create events table
CREATE EXTERNAL TABLE ${db}.hb_events(event_id STRING, start_time STRING, city STRING, state STRING, zip STRING, country STRING, latitude FLOAT, longitude FLOAT, user_id STRING, common_words STRING)
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key, schedule:start_time, location:city, location:state, location:zip, location:country, location:lat, location:lng, creator:user_id, remark:common_words')
    TBLPROPERTIES ('hbase.table.name' = 'events_db:events');
-- check if events table exists
DROP TABLE IF EXISTS ${db}.events;
-- create the events table
CREATE TABLE ${db}.events
STORED AS ORC AS
    SELECT * FROM ${db}.hb_events;
-- check if events table exists
DROP TABLE IF EXISTS ${db}.hb_events;
-- create event_cities table
DROP TABLE IF EXISTS ${db}.event_cities;
SET hivevar:num_cities = 32;
CREATE TABLE ${db}.event_cities 
STORED AS ORC AS
    SELECT
        t.city,
        ${num_cities} + 1 - row_number() over () AS level
    FROM (SELECT city, count(*) AS count FROM ${db}.events GROUP BY city ORDER BY count DESC LIMIT ${num_cities}) t;
-- create event_countries table
DROP TABLE IF EXISTS ${db}.event_countries;
SET hivevar:num_countries = 8;
CREATE TABLE ${db}.event_countries 
STORED AS ORC AS
    SELECT
        t.country,
        ${num_countries} + 1 - row_number() over () AS level
    FROM (SELECT country, count(*) AS count FROM ${db}.events GROUP BY country ORDER BY count DESC LIMIT ${num_countries}) t;

-- create user_event_count table
CREATE TABLE ${db}.user_event_count 
STORED AS ORC AS
    SELECT 
        user_id,
        COUNT(event_id) AS event_count
    FROM ${db}.train
    GROUP BY user_id;
-- create user_event table
CREATE TEMPORARY TABLE ${db}.user_event
STORED AS ORC AS
    SELECT
        t.user_id,
        t.event_id,
        t.invited AS user_invited,
        CASE WHEN t.time_stamp regexp '^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}.*' AND e.start_time regexp '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z' THEN datediff(from_unixtime(unix_timestamp(CONCAT(SUBSTR(e.start_time, 1, 10), ' ', SUBSTR(e.start_time, 12, 8)), 'yyyy-MM-dd hh:mm:ss')), from_unixtime(unix_timestamp(CONCAT(SUBSTR(t.time_stamp, 1, 10), ' ', SUBSTR(t.time_stamp, 12, 8)), 'yyyy-MM-dd hh:mm:ss'))) ELSE NULL END AS invite_ahead_days,
        t.interested AS user_interested,
        e.user_id AS event_creator,
        CASE WHEN e.start_time regexp '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z' THEN datediff(from_unixtime(unix_timestamp(CONCAT(SUBSTR(e.start_time, 1, 10), ' ', SUBSTR(e.start_time, 12, 8)), 'yyyy-MM-dd hh:mm:ss')), FROM_UNIXTIME(UNIX_TIMESTAMP())) ELSE NULL END AS start_ahead_days,
        CASE WHEN e.start_time regexp '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z' THEN MONTH(from_unixtime(unix_timestamp(CONCAT(SUBSTR(e.start_time, 1, 10), ' ', SUBSTR(e.start_time, 12, 8)), 'yyyy-MM-dd HH:mm:ss'))) ELSE NULL END AS event_start_month,
        CASE WHEN e.start_time regexp '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z' THEN from_unixtime(unix_timestamp(CONCAT(SUBSTR(e.start_time, 1, 10), ' ', SUBSTR(e.start_time, 12, 8)), 'yyyy-MM-dd hh:mm:ss'), 'u') ELSE NULL END AS event_start_dayofweek,
        CASE WHEN e.start_time regexp '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z' THEN HOUR(from_unixtime(unix_timestamp(CONCAT(SUBSTR(e.start_time, 1, 10), ' ', SUBSTR(e.start_time, 12, 8)), 'yyyy-MM-dd HH:mm:ss'))) ELSE NULL END AS event_start_hour,
        e.city as event_city,
        e.state as event_state,
        e.country as event_country,
        e.latitude,
        e.longitude
    FROM ${db}.train t INNER JOIN ${db}.events e ON t.event_id = e.event_id;
-- create event_creator_is_friend table
CREATE TEMPORARY TABLE ${db}.event_creator_is_friend
STORED AS ORC AS
    SELECT
        ue.user_id,
        ue.event_id,
        ue.user_invited,
        ue.invite_ahead_days,
        ue.user_interested,
        ue.event_creator,
        ue.start_ahead_days,
        ue.event_start_month,
        ue.event_start_dayofweek,
        ue.event_start_hour,
        ue.event_city,
        ue.event_state,
        ue.event_country,
        ue.latitude,
        ue.longitude,
        CASE WHEN uf.friend_id IS NOT NULL THEN 1 ELSE 0 END AS event_creator_is_friend,
        CASE WHEN ufc.friend_count IS NOT NULL THEN ufc.friend_count ELSE 0 END AS user_friend_count
    FROM ${db}.user_event ue
        LEFT JOIN ${db}.user_friend uf ON ue.user_id = uf.user_id AND ue.event_creator = uf.friend_id
        LEFT JOIN ${db}.user_friend_count ufc ON ue.user_id = ufc.user_id;
-- create friend_attend_percentage table
CREATE TEMPORARY TABLE ${db}.friend_attend_percentage
STORED AS ORC AS
    SELECT
        ecif.user_id,
        ecif.event_id,
        ecif.invite_ahead_days,
        ecif.start_ahead_days,
        ecif.event_start_month,
        ecif.event_start_dayofweek,
        ecif.event_start_hour,
        ecif.event_city,
        ecif.event_state,
        ecif.event_country,
        ecif.latitude,
        ecif.longitude,
        ecif.event_creator_is_friend,
        ecif.user_friend_count,
        ecif.user_invited,
        ecif.user_interested,
        CASE WHEN fas.invited_friends_count IS NOT NULL THEN fas.invited_friends_count ELSE 0 END AS invited_friends_count,
        CASE WHEN fas.attended_friends_count IS NOT NULL THEN fas.attended_friends_count ELSE 0 END AS attended_friends_count,
        CASE WHEN fas.not_attended_friends_count IS NOT NULL THEN fas.not_attended_friends_count ELSE 0 END AS not_attended_friends_count,
        CASE WHEN fas.maybe_attended_friends_count IS NOT NULL THEN fas.maybe_attended_friends_count ELSE 0 END AS maybe_attended_friends_count,
        CASE WHEN ecif.user_friend_count != 0 AND fas.invited_friends_count IS NOT NULL THEN fas.invited_friends_count * 100 / ecif.user_friend_count ELSE 0 END as invited_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.attended_friends_count IS NOT NULL THEN fas.attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as attended_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.not_attended_friends_count IS NOT NULL THEN fas.not_attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as not_attended_friends_percentage,
        CASE WHEN ecif.user_friend_count != 0 AND fas.maybe_attended_friends_count IS NOT NULL THEN fas.maybe_attended_friends_count * 100 / ecif.user_friend_count ELSE 0 END as maybe_attended_friends_percentage
    FROM ${db}.event_creator_is_friend ecif
        LEFT JOIN ${db}.friend_attend_summary fas ON ecif.user_id = fas.user_id AND ecif.event_id = fas.event_id;
-- create user_friend_event table
CREATE TEMPORARY TABLE ${db}.user_friend_event
STORED AS ORC AS
    SELECT
        CAST(fap.user_interested AS int) AS user_interested,
        fap.user_id,
        fap.event_id,
        CASE WHEN l.locale_id IS NOT NULL THEN l.locale_id ELSE 0 END AS user_locale,
        CASE WHEN u.gender = 'male' THEN 1 WHEN u.gender = 'female' THEN 0 ELSE -1 END AS user_gender,
        YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) - u.birth_year AS user_age,
        CAST(u.time_zone AS int) AS user_time_zone,
        YEAR(FROM_UNIXTIME(UNIX_TIMESTAMP())) - u.joined_at AS user_member_years,
        fap.user_friend_count,
        fap.invite_ahead_days AS user_invite_ahead_days,
        uec.event_count AS user_had_event_count,
        uaec.invited_count AS user_invited_event_count,
        uaec.attended_count AS user_attended_event_count,
        uaec.not_attended_count AS user_not_attended_event_count,
        uaec.maybe_attended_count AS user_maybe_attended_event_count,
        CAST(fap.user_invited AS int) AS user_invited,
        fap.invited_friends_count AS user_invited_friends_count,
        fap.attended_friends_count AS user_attended_friends_count,
        fap.not_attended_friends_count AS user_not_attended_friends_count,
        fap.maybe_attended_friends_count AS user_maybe_attended_friends_count,
        fap.invited_friends_percentage AS user_invited_friends_percentage,
        fap.attended_friends_percentage AS user_attended_friends_percentage,
        fap.not_attended_friends_percentage AS user_not_attended_friends_percentage,
        fap.maybe_attended_friends_percentage AS user_maybe_attended_friends_percentage,
        fap.start_ahead_days AS event_start_ahead_days,
        fap.event_start_month,
        CAST(fap.event_start_dayofweek AS int) AS event_start_dayofweek,
        fap.event_start_hour,
        iv.attend_count AS event_invited_user_count,
        yes.attend_count AS event_attended_user_count,
        no.attend_count AS event_not_attended_user_count,
        maybe.attend_count AS event_maybe_attended_user_count,
        ec.level AS event_city_level,
        et.level AS event_country_level,
        fap.event_creator_is_friend,
        locationSimilar(lower(u.location), lower(fap.event_city), lower(fap.event_state), lower(fap.event_country)) AS location_similar
    FROM ${db}.friend_attend_percentage fap
        INNER JOIN ${db}.users u ON fap.user_id = u.user_id
        INNER JOIN ${db}.user_event_count uec ON uec.user_id = fap.user_id
        INNER JOIN ${db}.event_attendee_count iv ON iv.event_id = fap.event_id AND iv.attend_type = 'invited'
        INNER JOIN ${db}.event_attendee_count yes ON yes.event_id = fap.event_id AND yes.attend_type = 'yes'
        INNER JOIN ${db}.event_attendee_count no ON no.event_id = fap.event_id AND no.attend_type = 'no'
        INNER JOIN ${db}.event_attendee_count maybe ON maybe.event_id = fap.event_id AND maybe.attend_type = 'maybe'
        LEFT JOIN ${db}.user_attend_event_count uaec ON uaec.user_id = fap.user_id
        LEFT JOIN ${db}.event_cities ec ON ec.city = fap.event_city
        LEFT JOIN ${db}.event_countries et ON et.country = fap.event_country
        LEFT JOIN ${db}.locale l ON u.locale = l.locale;
-- check if train_data table exists
DROP TABLE IF EXISTS ${db}.train_data;
-- create the train_data table
CREATE TABLE ${db}.train_data 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
AS
    SELECT
        CASE WHEN m.user_interested IS NULL THEN 0 ELSE m.user_interested END AS user_interested,
        m.user_id,
        m.event_id,
        CASE WHEN m.user_locale IS NULL THEN 0 ELSE m.user_locale END AS user_locale,
        CASE WHEN m.user_gender IS NULL THEN -1 ELSE m.user_gender END AS user_gender,
        CASE WHEN m.user_age IS NULL THEN s.average_age ELSE m.user_age END AS user_age,
        CASE WHEN m.user_time_zone IS NULL THEN 0 ELSE m.user_time_zone END AS user_time_zone,
        CASE WHEN m.user_member_years IS NULL THEN 0 ELSE m.user_member_years END AS user_member_years,
        CASE WHEN m.user_friend_count IS NULL THEN 0 ELSE m.user_friend_count END AS user_friend_count,
        CASE WHEN m.user_invite_ahead_days IS NULL THEN 0 ELSE m.user_invite_ahead_days END AS user_invite_ahead_days,
        CASE WHEN m.user_had_event_count IS NULL THEN 0 ELSE m.user_had_event_count END AS user_had_event_count,
        CASE WHEN m.user_invited_event_count IS NULL THEN 0 ELSE m.user_invited_event_count END AS user_invited_event_count,
        CASE WHEN m.user_attended_event_count IS NULL THEN 0 ELSE m.user_attended_event_count END AS user_attended_event_count,
        CASE WHEN m.user_not_attended_event_count IS NULL THEN 0 ELSE m.user_not_attended_event_count END AS user_not_attended_event_count,           
        CASE WHEN m.user_maybe_attended_event_count IS NULL THEN 0 ELSE m.user_maybe_attended_event_count END AS user_maybe_attended_event_count,         
        CASE WHEN m.user_invited IS NULL THEN 0 ELSE m.user_invited END AS user_invited,
        CASE WHEN m.user_invited_friends_count IS NULL THEN 0 ELSE m.user_invited_friends_count END AS user_invited_friends_count,              
        CASE WHEN m.user_attended_friends_count IS NULL THEN 0 ELSE m.user_attended_friends_count END AS user_attended_friends_count,
        CASE WHEN m.user_not_attended_friends_count IS NULL THEN 0 ELSE m.user_not_attended_friends_count END AS user_not_attended_friends_count,         
        CASE WHEN m.user_maybe_attended_friends_count IS NULL THEN 0 ELSE m.user_maybe_attended_friends_count END AS user_maybe_attended_friends_count,
        CASE WHEN m.user_invited_friends_percentage IS NULL THEN 0 ELSE m.user_invited_friends_percentage END AS user_invited_friends_percentage,         
        CASE WHEN m.user_attended_friends_percentage IS NULL THEN 0 ELSE m.user_attended_friends_percentage END AS user_attended_friends_percentage,        
        CASE WHEN m.user_not_attended_friends_percentage IS NULL THEN 0 ELSE m.user_not_attended_friends_percentage END AS user_not_attended_friends_percentage,    
        CASE WHEN m.user_maybe_attended_friends_percentage IS NULL THEN 0 ELSE m.user_maybe_attended_friends_percentage END AS user_maybe_attended_friends_percentage,
        CASE WHEN m.event_start_ahead_days IS NULL THEN 0 ELSE m.event_start_ahead_days END AS event_start_ahead_days,
        CASE WHEN m.event_start_month IS NULL THEN 0 ELSE m.event_start_month END AS event_start_month,
        CASE WHEN m.event_start_dayofweek IS NULL THEN 0 ELSE m.event_start_dayofweek END AS event_start_dayofweek,
        CASE WHEN m.event_start_hour IS NULL THEN 0 ELSE m.event_start_hour END AS event_start_hour,
        CASE WHEN m.event_invited_user_count IS NULL THEN 0 ELSE m.event_invited_user_count END AS event_invited_user_count,
        CASE WHEN m.event_attended_user_count IS NULL THEN 0 ELSE m.event_attended_user_count END AS event_attended_user_count,
        CASE WHEN m.event_not_attended_user_count IS NULL THEN 0 ELSE m.event_not_attended_user_count END AS event_not_attended_user_count,
        CASE WHEN m.event_maybe_attended_user_count IS NULL THEN 0 ELSE m.event_maybe_attended_user_count END AS event_maybe_attended_user_count,
        CASE WHEN m.event_city_level IS NULL THEN 0 ELSE m.event_city_level END AS event_city_level,                        
        CASE WHEN m.event_country_level IS NULL THEN 0 ELSE m.event_country_level END AS event_country_level,                     
        CASE WHEN m.event_creator_is_friend IS NULL THEN 0 ELSE m.event_creator_is_friend END AS event_creator_is_friend,
        CASE WHEN m.location_similar IS NULL THEN 0 ELSE m.location_similar END AS location_similar
    FROM ${db}.user_friend_event m
        CROSS JOIN (SELECT AVG(user_age) AS average_age FROM ${db}.user_friend_event) s;

-- create train_update table
CREATE TABLE IF NOT EXISTS ${db}.train_update
(
    time_stamp STRING,
    train_count bigint
) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;
-- insert data
INSERT INTO ${db}.train_update 
    SELECT FROM_UNIXTIME(UNIX_TIMESTAMP()) as time_stamp, t.train_count FROM (SELECT count(*) AS train_count FROM train_data) t;
