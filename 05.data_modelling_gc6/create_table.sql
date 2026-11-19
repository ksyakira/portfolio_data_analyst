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
