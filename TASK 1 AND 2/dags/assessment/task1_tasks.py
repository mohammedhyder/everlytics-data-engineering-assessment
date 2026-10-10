from mohammedhyder.assessment.task1_functions import (
    check_input_files,
    load_customers_to_staging,
    load_orders_to_staging,
    transform_customers,
    transform_orders,
    load_customer_target,
    load_order_target,
    validate_final_load,
)


# --------------------------------------------------
# 1. CHECK INPUT FILES
# --------------------------------------------------

def check_input_task():

    check_input_files()


# --------------------------------------------------
# 2. LOAD STAGING
# --------------------------------------------------

def load_staging_task():

    load_customers_to_staging()

    load_orders_to_staging()

    print(
        "Both input files loaded into "
        "Doris staging tables."
    )


# --------------------------------------------------
# 3. TRANSFORM
# --------------------------------------------------

def transform_task():

    # Customer transformations
    transform_customers()

    # Order transformations
    transform_orders()

    print(
        "Customer and order transformations "
        "completed successfully."
    )


# --------------------------------------------------
# 4. LOAD TARGET
# --------------------------------------------------

def load_target_task():

    load_customer_target()

    load_order_target()

    print(
        "Doris target tables loaded successfully."
    )


# --------------------------------------------------
# 5. VALIDATE
# --------------------------------------------------

def validate_task():

    validate_final_load()