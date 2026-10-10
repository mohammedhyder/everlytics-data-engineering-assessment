USE database_mohammedhyder;

SELECT file_name, status, processed_at, record_count, error_message
FROM task6_ingestion_tracker
ORDER BY processed_at DESC;

SELECT status, COUNT(*) AS file_count
FROM task6_ingestion_tracker
GROUP BY status;

SELECT COUNT(*) AS customer_count FROM task6_customers;
SELECT COUNT(*) AS order_count FROM task6_orders;

SELECT order_id, quantity, unit_price, total_amount
FROM task6_orders
ORDER BY order_id;