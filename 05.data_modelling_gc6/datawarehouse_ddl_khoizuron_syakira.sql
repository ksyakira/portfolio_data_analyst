-- CREATE DATABASE thelook_dwh;

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_orders;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_users;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_users (
    user_id          INTEGER      PRIMARY KEY,
    first_name       VARCHAR(100),
    last_name        VARCHAR(100),
    age              INTEGER,
    gender           VARCHAR(20),
    city             VARCHAR(100),
    state            VARCHAR(100),
    country          VARCHAR(100),
    traffic_source   VARCHAR(50),
    user_created_at  TIMESTAMPTZ
);

CREATE TABLE dim_products (
    product_id    INTEGER       PRIMARY KEY,
    product_name  TEXT,
    brand         VARCHAR(255),
    category      VARCHAR(100),
    department    VARCHAR(50),
    sku           VARCHAR(50),
    retail_price  NUMERIC(10,2)
);

CREATE TABLE dim_orders (
    order_id      INTEGER      PRIMARY KEY,
    order_status  VARCHAR(30),
    num_of_item   INTEGER
);

CREATE TABLE dim_date (
    date_key    INTEGER      PRIMARY KEY,
    full_date   DATE         NOT NULL UNIQUE,
    year        INTEGER      NOT NULL,
    quarter     INTEGER      NOT NULL,
    month       INTEGER      NOT NULL,
    month_name  VARCHAR(20)  NOT NULL,
    day_name    VARCHAR(20)  NOT NULL
);

CREATE TABLE fact_sales (
    order_item_id           INTEGER        PRIMARY KEY,
    order_id                INTEGER        NOT NULL REFERENCES dim_orders (order_id),
    user_id                 INTEGER        NOT NULL REFERENCES dim_users (user_id),
    product_id              INTEGER        NOT NULL REFERENCES dim_products (product_id),
    date_key                INTEGER        NOT NULL REFERENCES dim_date (date_key),
    item_status             VARCHAR(30),
    order_created_at        TIMESTAMPTZ    NOT NULL,
    shipped_at              TIMESTAMPTZ,
    delivered_at            TIMESTAMPTZ,
    returned_at             TIMESTAMPTZ,
    days_in_stock           INTEGER,
    sale_price              NUMERIC(10,2)  NOT NULL,
    product_cost            NUMERIC(10,2)  NOT NULL,
    profit                  NUMERIC(10,2)  NOT NULL
);

SELECT 'dim_users' AS tabel, COUNT(*) FROM dim_users
UNION ALL SELECT 'dim_products',            COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_orders',              COUNT(*) FROM dim_orders
UNION ALL SELECT 'dim_date',                COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_sales',              COUNT(*) FROM fact_sales

-- Problem 1
SELECT
    d.year,
    ROUND(SUM(f.sale_price), 2)::float AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.item_status NOT IN('Cancelled', 'Returned')
GROUP BY 1
ORDER BY 1

-- Problem 2
SELECT
    d.year,
    ROUND(SUM(f.sale_price), 2)::float AS revenue
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.item_status NOT IN('Cancelled', 'Returned')
GROUP BY 1
ORDER BY 1