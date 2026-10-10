# --------------------------------------------------
# TASK 1 CONFIGURATION
# --------------------------------------------------

MINIO_CONNECTION_ID = "minio_mohammedhyder"

DORIS_CONNECTION_ID = "doris_mohammedhyder"


# --------------------------------------------------
# MINIO
# --------------------------------------------------

MINIO_BUCKET = "test"

CUSTOMERS_INPUT_KEY = (
    "mohammedhyder/raw/customers/customers.csv"
)

ORDERS_INPUT_KEY = (
    "mohammedhyder/raw/orders/orders.csv"
)


# --------------------------------------------------
# DORIS STAGING TABLES
# --------------------------------------------------

CUSTOMERS_STAGING_TABLE = "stg_customers"

ORDERS_STAGING_TABLE = "stg_orders"


# --------------------------------------------------
# DORIS TRANSFORMED TABLES
# --------------------------------------------------

TRANSFORMED_CUSTOMERS_TABLE = (
    "task1_transformed_customers"
)

TRANSFORMED_ORDERS_TABLE = (
    "task1_transformed_orders"
)


# --------------------------------------------------
# DORIS TARGET TABLES
# --------------------------------------------------

CUSTOMERS_TARGET_TABLE = "dim_customer"

ORDERS_TARGET_TABLE = "fact_order"