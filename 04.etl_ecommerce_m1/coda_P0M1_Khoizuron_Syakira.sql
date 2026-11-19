/*
=================================================
Milestone 1

Nama  : Khoizuron Syakira
Batch : CODA-RMT-023

Load Stage dari proses ETL
=================================================
*/

-- 1. Membuat database baru untuk proyek Milestone 1
CREATE DATABASE coda_p0m1;

-- 2. Buat tabel produk_sephora
CREATE TABLE produk_sephora (
    id SERIAL PRIMARY KEY,
    brand VARCHAR(100),
    nama_produk VARCHAR(255),
    harga INT,
    rating FLOAT
);

-- 3. Import data CSV menggunakan path absolut Mac kamu
COPY produk_sephora(brand, nama_produk, harga, rating)
FROM '/private/tmp/sephora_fragrance_bersih.csv'
DELIMITER ','
CSV HEADER;

-- 4. Tampilkan data untuk verifikasi hasil import
SELECT * FROM produk_sephora;
