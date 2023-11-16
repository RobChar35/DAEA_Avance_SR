
CREATE TABLE movies (
    movieId SERIAL PRIMARY KEY,
    title TEXT,
    genres TEXT
);

9742

CREATE TABLE ratings (
    userId INTEGER,
    movieId INTEGER,
    rating NUMERIC,
    timestamp TIMESTAMP
);

100836