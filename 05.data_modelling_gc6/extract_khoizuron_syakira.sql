SELECT
    id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status,
    created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price
FROM `bigquery-public-data.thelook_ecommerce.order_items`;

SELECT
    order_id,
    user_id,
    status,
    num_of_item,
    created_at
FROM `bigquery-public-data.thelook_ecommerce.orders`;

SELECT
    id,
    name,
    brand,
    category,
    department,
    sku,
    retail_price,
    cost
FROM `bigquery-public-data.thelook_ecommerce.products`;

SELECT
    id,
    first_name,
    last_name,
    age,
    gender,
    city,
    state,
    country,
    traffic_source,
    created_at
FROM `bigquery-public-data.thelook_ecommerce.users`;