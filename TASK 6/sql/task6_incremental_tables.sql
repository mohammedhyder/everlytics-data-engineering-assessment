USE database_mohammedhyder;

CREATE TABLE IF NOT EXISTS task6_ingestion_tracker (
    file_name VARCHAR(500) NOT NULL,
    status VARCHAR(20),
    processed_at DATETIME,
    record_count BIGINT,
    error_message VARCHAR(1000)
)
UNIQUE KEY(file_name)
DISTRIBUTED BY HASH(file_name) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS task6_customers (
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(150),
    city VARCHAR(100),
    email VARCHAR(200),
    created_date DATE
)
UNIQUE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS task6_orders (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    order_date DATE,
    quantity INT,
    unit_price DECIMAL(12,2),
    total_amount DECIMAL(14,2)
)
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
