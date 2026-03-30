CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS clean;

CREATE TABLE IF NOT EXISTS raw.load_log (
    id SERIAL PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    file_period TEXT NOT NULL,
    source_url TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL,
    UNIQUE (dataset_name, file_period)
);
