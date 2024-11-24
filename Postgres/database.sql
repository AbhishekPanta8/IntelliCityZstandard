-- Create the database
CREATE DATABASE smart_city_db;

-- Switch to the database
\c smart_city_db;

-- Create tables
CREATE TABLE IF NOT EXISTS average_counts_per_day (
    day_of_week VARCHAR(10),
    average_counts DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS status_counts (
    status VARCHAR(10),
    count INTEGER
);

CREATE TABLE IF NOT EXISTS user_type_metrics (
    user_type VARCHAR(10),
    active_user_count INTEGER
);
