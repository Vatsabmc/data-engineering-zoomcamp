-- Raw processed_events table
CREATE TABLE processed_events (
    PULocationID INTEGER,
    DOLocationID INTEGER,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    passenger_count INTEGER,
    pickup_datetime VARCHAR,
    dropoff_datetime VARCHAR
);


-- Raw events table
CREATE TABLE IF NOT EXISTS events (
    pulocationid INTEGER,
    dolocationid INTEGER,
    passenger_count INTEGER,
    trip_distance DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    lpep_pickup_datetime TIMESTAMP,
    lpep_dropoff_datetime TIMESTAMP
);

-- Aggregated sink table
CREATE TABLE IF NOT EXISTS processed_events_aggregated (
    window_start TIMESTAMP(3) NOT NULL,
    pulocationid INTEGER NOT NULL,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, pulocationid)
);

-- Session-window sink table (5-minute gap by PULocationID)
CREATE TABLE IF NOT EXISTS processed_events_session (
    window_start TIMESTAMP(3) NOT NULL,
    window_end TIMESTAMP(3) NOT NULL,
    pulocationid INTEGER NOT NULL,
    num_trips BIGINT,
    total_revenue DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end, pulocationid)
);

-- Hourly tip aggregation sink table (across all locations)
CREATE TABLE IF NOT EXISTS processed_tip_amount_hourly (
    window_start TIMESTAMP(3) NOT NULL,
    window_end TIMESTAMP(3) NOT NULL,
    total_tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start)
);

-- Trips with a trip_distance > 5
SELECT count(1) 
FROM processed_events 
WHERE trip_distance>5;

-- PULocationID with the most trips in a single 5-minute window
SELECT PULocationID, num_trips
FROM processed_events_aggregated
ORDER BY num_trips DESC
LIMIT 3;


-- PULocationID with the longest session (most trips in one session)
SELECT pulocationid, window_start, window_end, num_trips, total_revenue
FROM processed_events_session
ORDER BY num_trips DESC, window_start ASC, pulocationid ASC
LIMIT 1;


-- Hourly total tip amount across all locations
SELECT window_start, window_end, total_tip_amount
FROM processed_tip_amount_hourly
ORDER BY total_tip_amount DESC
LIMIT 5;