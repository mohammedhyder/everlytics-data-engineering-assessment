USE database_mohammedhyder;

-- STAGING CUSTOMERS
CREATE TABLE IF NOT EXISTS stg_customers (
    customer_id VARCHAR(50),
    customer_name VARCHAR(150),
    city VARCHAR(100),
    email VARCHAR(200),
    created_date DATE
)
DUPLICATE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- STAGING ORDERS
CREATE TABLE IF NOT EXISTS stg_orders (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date DATE,
    quantity INT,
    unit_price DECIMAL(12,2)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

USE database_mohammedhyder;

-- CUSTOMER TARGET
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key BIGINT NOT NULL AUTO_INCREMENT,
    customer_id VARCHAR(50),
    customer_name VARCHAR(150),
    city VARCHAR(100),
    email VARCHAR(200),
    created_date DATE
)
DUPLICATE KEY(customer_key)
DISTRIBUTED BY HASH(customer_key) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);

-- ORDER TARGET
CREATE TABLE IF NOT EXISTS fact_order (
    order_id VARCHAR(50),
    customer_id VARCHAR(50),
    order_date DATE,
    quantity INT,
    unit_price DECIMAL(12,2),
    total_amount DECIMAL(14,2)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);


SHOW TABLES;

SHOW BACKENDS;

USE database_mohammedhyder;

SELECT COUNT(*) AS customer_staging_count
FROM stg_customers;

SELECT COUNT(*) AS order_staging_count
FROM stg_orders;

SELECT COUNT(*) AS customer_target_count
FROM dim_customer;

SELECT COUNT(*) AS order_target_count
FROM fact_order;

-- Checking cleaned customer names, cities, and emails
SELECT customer_id, customer_name, city, email
FROM dim_customer
LIMIT 10;

-- Checking calculated order amounts
SELECT order_id, customer_id, quantity, unit_price, total_amount
FROM fact_order
LIMIT 10;

-- Checking every order amount is correct
SELECT COUNT(*) AS incorrect_amounts
FROM fact_order
WHERE total_amount <> quantity * unit_price;


