USE database_mohammedhyder;

-- SURROGATE KEY AND BUSINESS KEY
SELECT
    customer_key AS surrogate_key,
    customer_id AS source_business_key,
    customer_name,
    city,
    email
FROM dim_customer
ORDER BY customer_key;

-- SURROGATE KEY VALIDATION
SELECT
    COUNT(*) AS total_customers,
    COUNT(customer_key) AS customers_with_surrogate_keys,
    COUNT(DISTINCT customer_key) AS unique_surrogate_keys
FROM dim_customer;


USE database_mohammedhyder;

-- VIEW TRANSFORMED CUSTOMERS AND ORDERS
SELECT *
FROM task1_transformed_customers
LIMIT 10;
-- VIEW TRANSFORMED ORDERS
SELECT *
FROM task1_transformed_orders
LIMIT 10;


-- MONTHLY SALES
USE database_mohammedhyder;

SELECT
    DATE_FORMAT(order_date, '%Y-%m') AS sales_month,
    COUNT(*) AS total_orders,
    SUM(total_amount) AS total_sales
FROM fact_order
GROUP BY DATE_FORMAT(order_date, '%Y-%m')
ORDER BY sales_month;


-- SALES BY CITY
SELECT
    d.city,
    COUNT(f.order_id) AS total_orders,
    SUM(f.total_amount) AS total_sales
FROM fact_order f
JOIN dim_customer d
    ON f.customer_id = d.customer_id
GROUP BY d.city
ORDER BY total_sales DESC;


-- SALES BY CUSTOMER
SELECT
    d.customer_id,
    d.customer_name,
    COUNT(f.order_id) AS total_orders,
    SUM(f.total_amount) AS total_sales
FROM fact_order f
JOIN dim_customer d
    ON f.customer_id = d.customer_id
GROUP BY
    d.customer_id,
    d.customer_name
ORDER BY total_sales DESC;

-- DESCRIBE THE DIM & FACT TABLES
DESCRIBE dim_customer;
DESCRIBE fact_order;