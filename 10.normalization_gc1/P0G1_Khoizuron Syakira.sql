/*
=================================================
Graded Challenge 1

Nama  : Khoizuron Syakira
Batch : CODA-RMT-023

Program ini dibuat untuk melakukan untuk melakukan normalisasi data
membangun skema relasional database, dan melakukan analisis dasar dalam penyajian data.
=================================================
*/

-- Query di bawah untuk membuat database baru 
CREATE DATABASE gc1;

-- Query di bawah untuk membuat tabel Staging
CREATE TABLE Staging (
    id VARCHAR(50) PRIMARY KEY,
    title TEXT,
    type VARCHAR(20),
    description TEXT,
    release_year INT,
    age_certification VARCHAR(20),
    runtime INT,
    genre TEXT[],
    production_countries TEXT[]
);

-- Query di bawah untuk mengimpor data dari file movies.csv ke dalam tabel Staging
COPY Staging (
    id,
    title,
    type,
    description,
    release_year,
    age_certification,
    runtime,
    genre,
    production_countries)
FROM '/private/tmp/movies.csv' DELIMITER ',' CSV HEADER
;

-- Query di bawah untuk checking tabel Staging apakah sudah benar
SELECT * FROM Staging;

-- Query di bawah untuk membuat tabel Movies
CREATE TABLE Movies (
    id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255),
    type VARCHAR(50),
    description TEXT,
    release_year INT,
    age_certification VARCHAR(50),
    runtime INT
);

-- Query di bawah untuk membuat tabel Movie_Genres
CREATE TABLE Movie_Genres (
    id SERIAL PRIMARY KEY,
    movie_id VARCHAR(50) REFERENCES Movies(id),
    genre VARCHAR(100)
);

-- Query di bawah untuk membuat tabel Movie_Countries
CREATE TABLE Movie_Countries (
    id SERIAL PRIMARY KEY,
    movie_id VARCHAR(50) REFERENCES Movies(id),
    country VARCHAR(100)
);

-- Query di bawah untuk import data ke tabel Movies dari tabel Staging
INSERT INTO Movies (id, title, type, description, release_year, age_certification, runtime)
SELECT id, title, type, description, release_year, age_certification, runtime FROM Staging;

-- Query di bawah untuk mengisi data ke tabel Movie_Genres dengan memecah array genre
INSERT INTO Movie_Genres (movie_id, genre)
SELECT id AS movie_id, UNNEST(genre) AS genre FROM Staging;

-- Query di bawah untuk mengisi data ke tabel Movie_Countries dengan memecah array production_countries
INSERT INTO Movie_Countries (movie_id, country)
SELECT id AS movie_id, UNNEST(production_countries) AS country FROM Staging;

-- Query di bawah untuk menampilkan semua film dengan genre dan negara produksinya
SELECT m.title, mg.genre, mc.country FROM Movies AS m
JOIN Movie_Genres AS mg 
ON m.id = mg.movie_id
JOIN Movie_Countries AS mc 
ON m.id = mc.movie_id
;

-- Query di bawah untuk menampilkan genere film yang paling populer
SELECT genre, COUNT(movie_id) AS total_movies
FROM Movie_Genres
GROUP BY genre
ORDER BY total_movies DESC
LIMIT 1
;

-- Query di bawah untuk menampilkan negara yang paling banyak membuat film
SELECT country, COUNT(movie_id) AS total_movies
FROM Movie_Countries
GROUP BY country
ORDER BY total_movies DESC
LIMIT 1
;

--  Query di bawah untuk menghapus table Staging
DROP TABLE IF EXISTS Staging;
