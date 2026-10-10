from mohammedhyder.assessment.task1_config import (
    MINIO_CONNECTION_ID,
    DORIS_CONNECTION_ID,
    MINIO_BUCKET,
)

CUSTOMERS_PREFIX = "mohammedhyder/raw/customers/"
ORDERS_PREFIX = "mohammedhyder/raw/orders/"

TRACKER_TABLE = "task6_ingestion_tracker"
CUSTOMERS_TABLE = "task6_customers"
ORDERS_TABLE = "task6_orders"

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "city",
    "email",
    "created_date",
]

ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "quantity",
    "unit_price",
]
