import csv
from decimal import Decimal
from io import StringIO
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook

from mohammedhyder.assessment.task1_config import (
    MINIO_CONNECTION_ID,
    DORIS_CONNECTION_ID,
    MINIO_BUCKET,
    CUSTOMERS_INPUT_KEY,
    ORDERS_INPUT_KEY,
    CUSTOMERS_STAGING_TABLE,
    ORDERS_STAGING_TABLE,
    TRANSFORMED_CUSTOMERS_TABLE,
    TRANSFORMED_ORDERS_TABLE,
    CUSTOMERS_TARGET_TABLE,
    ORDERS_TARGET_TABLE,
)


# --------------------------------------------------
# CONNECTION HELPERS
# --------------------------------------------------

def get_minio_hook():
    return S3Hook(
        aws_conn_id=MINIO_CONNECTION_ID
    )


def get_doris_hook():
    return MySqlHook(
        mysql_conn_id=DORIS_CONNECTION_ID
    )


# --------------------------------------------------
# 1. CHECK INPUT FILES
# --------------------------------------------------

def check_input_files():

    hook = get_minio_hook()

    customer_exists = hook.check_for_key(
        key=CUSTOMERS_INPUT_KEY,
        bucket_name=MINIO_BUCKET,
    )

    order_exists = hook.check_for_key(
        key=ORDERS_INPUT_KEY,
        bucket_name=MINIO_BUCKET,
    )

    if not customer_exists:
        raise FileNotFoundError(
            f"Customer file not found: "
            f"{CUSTOMERS_INPUT_KEY}"
        )

    if not order_exists:
        raise FileNotFoundError(
            f"Order file not found: "
            f"{ORDERS_INPUT_KEY}"
        )

    print("Input validation successful.")
    print("customers.csv found.")
    print("orders.csv found.")


# --------------------------------------------------
# 2. READ CSV FROM MINIO
# --------------------------------------------------

def read_csv_from_minio(key):

    hook = get_minio_hook()

    csv_data = hook.read_key(
        key=key,
        bucket_name=MINIO_BUCKET,
    )

    reader = csv.DictReader(
        StringIO(csv_data)
    )

    return list(reader)


# --------------------------------------------------
# 3. LOAD CUSTOMERS INTO STAGING
# --------------------------------------------------

def load_customers_to_staging():

    rows = read_csv_from_minio(
        CUSTOMERS_INPUT_KEY
    )

    hook = get_doris_hook()

    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute(
        f"TRUNCATE TABLE {CUSTOMERS_STAGING_TABLE}"
    )

    sql = f"""
        INSERT INTO {CUSTOMERS_STAGING_TABLE}
        (
            customer_id,
            customer_name,
            city,
            email,
            created_date
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    values = [
        (
            row["customer_id"],
            row["customer_name"],
            row["city"],
            row["email"],
            row["created_date"],
        )
        for row in rows
    ]

    cursor.executemany(sql, values)

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"Loaded {len(values)} customer records "
        f"into {CUSTOMERS_STAGING_TABLE}."
    )


# --------------------------------------------------
# 4. LOAD ORDERS INTO STAGING
# --------------------------------------------------

def load_orders_to_staging():

    rows = read_csv_from_minio(
        ORDERS_INPUT_KEY
    )

    hook = get_doris_hook()

    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute(
        f"TRUNCATE TABLE {ORDERS_STAGING_TABLE}"
    )

    sql = f"""
        INSERT INTO {ORDERS_STAGING_TABLE}
        (
            order_id,
            customer_id,
            order_date,
            quantity,
            unit_price
        )
        VALUES (%s, %s, %s, %s, %s)
    """

    values = [
        (
            row["order_id"],
            row["customer_id"],
            row["order_date"],
            int(row["quantity"]),
            Decimal(row["unit_price"]),
        )
        for row in rows
    ]

    cursor.executemany(sql, values)

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"Loaded {len(values)} order records "
        f"into {ORDERS_STAGING_TABLE}."
    )


# --------------------------------------------------
# 5. TRANSFORM CUSTOMERS
# --------------------------------------------------

def transform_customers():

    hook = get_doris_hook()

    connection = hook.get_conn()
    cursor = connection.cursor()

    # Re-create transformed customer table
    cursor.execute(
        f"DROP TABLE IF EXISTS "
        f"{TRANSFORMED_CUSTOMERS_TABLE}"
    )

    create_sql = f"""
        CREATE TABLE {TRANSFORMED_CUSTOMERS_TABLE}
        (
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
        )
    """

    cursor.execute(create_sql)

    # Basic customer transformations:
    # 1. TRIM leading/trailing spaces
    # 2. Remove multiple spaces between words
    # 3. Convert names to proper case
    # 4. Convert cities to proper case
    # 5. Standardize email to lowercase
    transform_sql = f"""
        INSERT INTO {TRANSFORMED_CUSTOMERS_TABLE}
        (
            customer_id,
            customer_name,
            city,
            email,
            created_date
        )
        SELECT
            TRIM(customer_id),

            INITCAP(
                REGEXP_REPLACE(
                    TRIM(customer_name),
                    '[[:space:]]+',
                    ' '
                )
            ),

            INITCAP(
                REGEXP_REPLACE(
                    TRIM(city),
                    '[[:space:]]+',
                    ' '
                )
            ),

            LOWER(TRIM(email)),

            created_date

        FROM {CUSTOMERS_STAGING_TABLE}
    """

    cursor.execute(transform_sql)

    connection.commit()

    # --------------------------------------------------
    # EMAIL FORMAT VALIDATION
    # --------------------------------------------------

    cursor.execute(
        f"""
        SELECT COUNT(*)
        FROM {TRANSFORMED_CUSTOMERS_TABLE}
        WHERE email IS NULL
           OR NOT REGEXP(
               email,
               '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{{2,}}$'
           )
        """
    )

    invalid_email_count = cursor.fetchone()[0]

    if invalid_email_count > 0:

        cursor.close()
        connection.close()

        raise ValueError(
            f"Found {invalid_email_count} customers "
            f"with invalid email format."
        )

    cursor.close()
    connection.close()

    print(
        "Customer transformation completed."
    )

    print(
        "Applied customer transformations:"
    )

    print(
        "- Removed leading/trailing spaces"
    )

    print(
        "- Removed multiple spaces between words"
    )

    print(
        "- Converted customer names to proper case"
    )

    print(
        "- Converted city names to proper case"
    )

    print(
        "- Standardized email to lowercase"
    )

    print(
        "- Validated email format"
    )


# --------------------------------------------------
# 6. TRANSFORM ORDERS
# --------------------------------------------------

def transform_orders():

    hook = get_doris_hook()

    connection = hook.get_conn()
    cursor = connection.cursor()

    # Re-create transformed order table
    cursor.execute(
        f"DROP TABLE IF EXISTS "
        f"{TRANSFORMED_ORDERS_TABLE}"
    )

    create_sql = f"""
        CREATE TABLE {TRANSFORMED_ORDERS_TABLE}
        (
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
        )
    """

    cursor.execute(create_sql)

    # Basic order transformation:
    # total_amount = quantity * unit_price

    transform_sql = f"""
        INSERT INTO {TRANSFORMED_ORDERS_TABLE}
        (
            order_id,
            customer_id,
            order_date,
            quantity,
            unit_price,
            total_amount
        )
        SELECT
            order_id,
            customer_id,
            order_date,
            quantity,
            unit_price,
            quantity * unit_price AS total_amount
        FROM {ORDERS_STAGING_TABLE}
    """

    cursor.execute(transform_sql)

    connection.commit()

    cursor.close()
    connection.close()

    print(
        "Order transformation completed."
    )

    print(
        "Applied transformation:"
    )

    print(
        "total_amount = quantity * unit_price"
    )


# --------------------------------------------------
# 7. LOAD CUSTOMER TARGET
# --------------------------------------------------

def load_customer_target():

    hook = get_doris_hook()

    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute(
        f"TRUNCATE TABLE "
        f"{CUSTOMERS_TARGET_TABLE}"
    )

    sql = f"""
        INSERT INTO {CUSTOMERS_TARGET_TABLE}
        (
            customer_key,
            customer_id,
            customer_name,
            city,
            email,
            created_date
        )
        SELECT
            ROW_NUMBER() OVER (
                ORDER BY customer_id
            ),
            customer_id,
            customer_name,
            city,
            email,
            created_date
        FROM {TRANSFORMED_CUSTOMERS_TABLE}
    """

    cursor.execute(sql)

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"Customer target table "
        f"{CUSTOMERS_TARGET_TABLE} loaded."
    )


# --------------------------------------------------
# 8. LOAD ORDER TARGET
# --------------------------------------------------

def load_order_target():

    hook = get_doris_hook()

    connection = hook.get_conn()
    cursor = connection.cursor()

    cursor.execute(
        f"TRUNCATE TABLE "
        f"{ORDERS_TARGET_TABLE}"
    )

    sql = f"""
        INSERT INTO {ORDERS_TARGET_TABLE}
        (
            order_id,
            customer_id,
            order_date,
            quantity,
            unit_price,
            total_amount
        )
        SELECT
            order_id,
            customer_id,
            order_date,
            quantity,
            unit_price,
            total_amount
        FROM {TRANSFORMED_ORDERS_TABLE}
    """

    cursor.execute(sql)

    connection.commit()

    cursor.close()
    connection.close()

    print(
        f"Order target table "
        f"{ORDERS_TARGET_TABLE} loaded."
    )

#Date Validation check

def validate_date_columns(rows, date_column, file_name):
    """
    Validate that date values exist and use YYYY-MM-DD format.
    Reject impossible dates such as 2026-02-30.
    """
    errors = []

    for row_number, row in enumerate(rows, start=2):
        value = (row.get(date_column) or "").strip()

        if not value:
            errors.append(
                f"{file_name}, row {row_number}: "
                f"{date_column} is missing."
            )
            continue

        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            errors.append(
                f"{file_name}, row {row_number}: "
                f"Invalid {date_column} '{value}'. "
                f"Expected YYYY-MM-DD."
            )

    if errors:
        raise ValueError(
            "Date validation failed:\n" + "\n".join(errors)
        )

    print(
        f"Date validation passed for {file_name}: "
        f"{len(rows)} rows checked."
    )

# --------------------------------------------------
# 9. VALIDATE FINAL LOAD
# --------------------------------------------------

def validate_final_load():

    hook = get_doris_hook()

    connection = hook.get_conn()
    cursor = connection.cursor()

    validation_errors = []

    def get_count(sql):
        cursor.execute(sql)
        return cursor.fetchone()[0]

    # --------------------------------------------------
    # 1. FINAL LOAD COUNTS
    # --------------------------------------------------

    customer_count = get_count(
        f"SELECT COUNT(*) FROM {CUSTOMERS_TARGET_TABLE}"
    )
    order_count = get_count(
        f"SELECT COUNT(*) FROM {ORDERS_TARGET_TABLE}"
    )

    print(f"Customer target records: {customer_count}")
    print(f"Order target records: {order_count}")

    if customer_count == 0:
        validation_errors.append("Customer target table contains no records.")

    if order_count == 0:
        validation_errors.append("Order target table contains no records.")

    # --------------------------------------------------
    # 2. MISSING / NULL MANDATORY IDS AND VALUES
    # --------------------------------------------------

    missing_customer_values = get_count(
        f"""
        SELECT COUNT(*)
        FROM {CUSTOMERS_TARGET_TABLE}
        WHERE customer_id IS NULL OR TRIM(customer_id) = ''
           OR customer_name IS NULL OR TRIM(customer_name) = ''
           OR city IS NULL OR TRIM(city) = ''
           OR email IS NULL OR TRIM(email) = ''
           OR created_date IS NULL
        """
    )

    missing_order_values = get_count(
        f"""
        SELECT COUNT(*)
        FROM {ORDERS_TARGET_TABLE}
        WHERE order_id IS NULL OR TRIM(order_id) = ''
           OR customer_id IS NULL OR TRIM(customer_id) = ''
           OR order_date IS NULL
           OR quantity IS NULL
           OR unit_price IS NULL
           OR total_amount IS NULL
        """
    )

    print(f"Customers with missing mandatory values: {missing_customer_values}")
    print(f"Orders with missing mandatory values: {missing_order_values}")

    if missing_customer_values > 0:
        validation_errors.append(
            f"{missing_customer_values} customer records have missing mandatory values."
        )

    if missing_order_values > 0:
        validation_errors.append(
            f"{missing_order_values} order records have missing mandatory values."
        )

    # --------------------------------------------------
    # 3. DUPLICATE CUSTOMER / ORDER BUSINESS IDS
    # --------------------------------------------------

    duplicate_customer_ids = get_count(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT customer_id
            FROM {CUSTOMERS_TARGET_TABLE}
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        ) duplicate_customers
        """
    )

    duplicate_order_ids = get_count(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT order_id
            FROM {ORDERS_TARGET_TABLE}
            GROUP BY order_id
            HAVING COUNT(*) > 1
        ) duplicate_orders
        """
    )

    print(f"Duplicate customer IDs: {duplicate_customer_ids}")
    print(f"Duplicate order IDs: {duplicate_order_ids}")

    if duplicate_customer_ids > 0:
        validation_errors.append(
            f"Found {duplicate_customer_ids} duplicate customer IDs."
        )

    if duplicate_order_ids > 0:
        validation_errors.append(
            f"Found {duplicate_order_ids} duplicate order IDs."
        )

    # --------------------------------------------------
    # 4. QUANTITY AND UNIT PRICE
    # --------------------------------------------------

    invalid_quantity_count = get_count(
        f"""
        SELECT COUNT(*)
        FROM {ORDERS_TARGET_TABLE}
        WHERE quantity <= 0
        """
    )

    invalid_price_count = get_count(
        f"""
        SELECT COUNT(*)
        FROM {ORDERS_TARGET_TABLE}
        WHERE unit_price < 0
        """
    )

    print(f"Invalid quantity records (quantity <= 0): {invalid_quantity_count}")
    print(f"Invalid unit price records (unit_price < 0): {invalid_price_count}")

    if invalid_quantity_count > 0:
        validation_errors.append(
            f"Found {invalid_quantity_count} orders with quantity <= 0."
        )

    if invalid_price_count > 0:
        validation_errors.append(
            f"Found {invalid_price_count} orders with negative unit price."
        )

    # --------------------------------------------------
    # 5. DATE VALIDATION
    # Doris DATE columns reject malformed date strings on insert.
    # This check catches missing dates that reached the tables.
    # --------------------------------------------------

    invalid_customer_dates = get_count(
        f"""
        SELECT COUNT(*)
        FROM {CUSTOMERS_TARGET_TABLE}
        WHERE created_date IS NULL
        """
    )

    invalid_order_dates = get_count(
        f"""
        SELECT COUNT(*)
        FROM {ORDERS_TARGET_TABLE}
        WHERE order_date IS NULL
        """
    )

    print(f"Customers with missing/invalid created_date: {invalid_customer_dates}")
    print(f"Orders with missing/invalid order_date: {invalid_order_dates}")

    if invalid_customer_dates > 0:
        validation_errors.append(
            f"Found {invalid_customer_dates} customers with missing created_date."
        )

    if invalid_order_dates > 0:
        validation_errors.append(
            f"Found {invalid_order_dates} orders with missing order_date."
        )

    # --------------------------------------------------
    # 6. REFERENTIAL INTEGRITY
    # Every order must reference an existing customer.
    # --------------------------------------------------

    orphan_order_count = get_count(
        f"""
        SELECT COUNT(*)
        FROM {ORDERS_TARGET_TABLE} o
        LEFT JOIN {CUSTOMERS_TARGET_TABLE} c
            ON TRIM(o.customer_id) = TRIM(c.customer_id)
        WHERE c.customer_id IS NULL
        """
    )

    print(f"Orders with invalid customer reference: {orphan_order_count}")

    if orphan_order_count > 0:
        validation_errors.append(
            f"Found {orphan_order_count} orders referencing missing customers."
        )

    # --------------------------------------------------
    # 7. TOTAL AMOUNT CALCULATION
    # --------------------------------------------------

    invalid_amount_count = get_count(
        f"""
        SELECT COUNT(*)
        FROM {ORDERS_TARGET_TABLE}
        WHERE total_amount <> quantity * unit_price
        """
    )

    print(f"Invalid total_amount records: {invalid_amount_count}")

    if invalid_amount_count > 0:
        validation_errors.append(
            f"Found {invalid_amount_count} incorrect total_amount values."
        )

    cursor.close()
    connection.close()

    # --------------------------------------------------
    # FAIL AIRFLOW TASK IF ANY CRITICAL CHECK FAILS
    # --------------------------------------------------

    if validation_errors:
        error_message = "Task 1 validation failed:\\n- " + "\\n- ".join(validation_errors)
        print(error_message)
        raise ValueError(error_message)

    print("Task 1 validation completed successfully.")
