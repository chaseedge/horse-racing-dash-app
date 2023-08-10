CREATE TABLE races(
    race_id TEXT PRIMARY KEY,
    track_id TEXT NOT NULL,
    race_date DATE NOT NULL,
    post_time TIMESTAMP,
    race_number INT NOT NULL,
    distance TEXT NOT NULL,
    surface TEXT,
    race_class TEXT
);
CREATE INDEX race_date_idx ON races (race_date);
CREATE INDEX track_idx ON races (track_id);
