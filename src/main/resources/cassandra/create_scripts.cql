-- IMPORTANT CONFIGURATION CHANGES:
-- in cassandra.yaml
read_request_timeout_in_ms: 150000
range_request_timeout_in_ms: 300000
write_request_timeout_in_ms: 60000
counter_write_request_timeout_in_ms: 150000
cas_contention_timeout_in_ms: 30000
truncate_request_timeout_in_ms: 1800000
request_timeout_in_ms: 300000
slow_query_log_timeout_in_ms: 15000
-- start cassandra client as
cqlsh --request-time=300000


-- create the key-space
CREATE KEYSPACE IF NOT EXISTS events_db WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

-- create the users table
DROP TABLE  IF EXISTS events_db.users;
-- create
CREATE TABLE events_db.users(
    counter_value counter,
    user_id text,
    birth_year text,
    gender text,
    locale text,
    location text,
    time_zone text,
    joined_at text,
    PRIMARY KEY(user_id, birth_year, gender, locale, location, time_zone, joined_at)
);

-- create the events table
DROP TABLE IF EXISTS events_db.events;
-- create
CREATE TABLE events_db.events(
    counter_value counter,
    event_id text,
    start_time text,
    city text,
    state text,
    zip text,
    country text,
    latitude text,
    longitude text,
    user_id text,
    common_words text,
    PRIMARY KEY(event_id, start_time, city, state, zip, country, latitude, longitude, user_id, common_words)
);

-- create user-friend table
DROP TABLE IF EXISTS events_db.user_friend;
-- create
CREATE TABLE events_db.user_friend(
    counter_value counter,
    user_id text,
    friend_id text,
    PRIMARY KEY((user_id, friend_id))
);

-- create user-friend-count table
DROP TABLE IF EXISTS events_db.user_friend_count;
-- create
CREATE TABLE events_db.user_friend_count(
    counter_value counter,
    user_id text,
    friend_count int,
    PRIMARY KEY((user_id, friend_count))
);

-- create event-attendee table
DROP TABLE IF EXISTS events_db.event_attendee;
-- create
CREATE TABLE events_db.event_attendee(
    counter_value counter,
    event_id text,
    user_id text,
    attend_type text,
    PRIMARY KEY((event_id, user_id, attend_type))
);

-- create event-attendee table
DROP TABLE IF EXISTS events_db.event_attendee_count;
-- create
CREATE TABLE events_db.event_attendee_count(
    counter_value counter,
    event_id text,
    attend_type text,
    attend_count int,
    PRIMARY KEY((event_id, attend_type), attend_count)
);

-- create event-attendee table
DROP TABLE IF EXISTS events_db.user_attend_event_count;
-- create
CREATE TABLE events_db.user_attend_event_count(
    counter_value counter,
    user_id text,
    invited_count int,
    attended_count int,
    not_attended_count int,
    maybe_attended_count int,
    PRIMARY KEY(user_id, invited_count, attended_count, not_attended_count, maybe_attended_count)
);

-- create event-friend_attend_summary table
DROP TABLE IF EXISTS events_db.friend_attend_summary;
-- create
CREATE TABLE events_db.friend_attend_summary(
    counter_value counter,
    user_id text,
    event_id text,
    invited_friends_count int,
    attended_friends_count int,
    not_attended_friends_count int,
    maybe_attended_friends_count int,
    PRIMARY KEY((user_id, event_id), invited_friends_count, attended_friends_count, not_attended_friends_count, maybe_attended_friends_count)
);

-- create event-locale table
DROP TABLE IF EXISTS events_db.locale;
-- create
CREATE TABLE events_db.locale(
    counter_value counter,
    locale_id int,
    locale text,
    PRIMARY KEY(locale_id, locale)
);

-- create train table
DROP TABLE IF EXISTS events_db.train;
-- create
CREATE TABLE events_db.train(
    counter_value counter,
    user_id text,
    event_id text,
    invited text,
    time_stamp text,
    interested text,
    PRIMARY KEY((user_id, event_id), invited, time_stamp, interested)
);

-- create user_event_count table
DROP TABLE IF EXISTS events_db.user_event_count;
-- create
CREATE TABLE events_db.user_event_count(
    counter_value counter,
    user_id text,
    event_count int,
    PRIMARY KEY(user_id, event_count)
);

-- create event_cities table
DROP TABLE IF EXISTS events_db.event_cities;
-- create
CREATE TABLE events_db.event_cities(
    counter_value counter,
    city text,
    level int,
    PRIMARY KEY(city, level)
);
-- create event_countries table
DROP TABLE IF EXISTS events_db.event_countries;
-- create
CREATE TABLE events_db.event_countries(
    counter_value counter,
    country text,
    level int,
    PRIMARY KEY(country, level)
);

-- create train_update table
DROP TABLE IF EXISTS events_db.train_update;
-- create
CREATE TABLE events_db.train_update(
    counter_value counter,
    time_stamp text,
    done int,
    PRIMARY KEY(time_stamp, done)
);