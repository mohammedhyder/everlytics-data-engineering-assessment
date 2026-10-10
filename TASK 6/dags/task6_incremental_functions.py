import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from io import StringIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook

from mohammedhyder.assessment.task6_incremental_config import (
    MINIO_CONNECTION_ID,
    DORIS_CONNECTION_ID,
    MINIO_BUCKET,
    CUSTOMERS_PREFIX,
    ORDERS_PREFIX,
    TRACKER_TABLE,
    CUSTOMERS_TABLE,
    ORDERS_TABLE,
    CUSTOMER_COLUMNS,
    ORDER_COLUMNS,
)


def _minio_hook():
    return S3Hook(aws_conn_id=MINIO_CONNECTION_ID)


def _doris_hook():
    return MySqlHook(mysql_conn_id=DORIS_CONNECTION_ID)


def _list_csv_keys(prefix):
    """Return sorted CSV object keys under one MinIO prefix."""
    keys = _minio_hook().list_keys(
        bucket_name=MINIO_BUCKET,
        prefix=prefix,
    ) or []
    return sorted(
        key for key in keys
        if key.lower().endswith(".csv") and not key.endswith("/")
    )


def _tracker_status(file_name):
    result = _doris_hook().get_first(
        f"SELECT status FROM {TRACKER_TABLE} WHERE file_name = %s",
        parameters=(file_name,),
    )
    return result[0] if result else None


def _set_tracker(file_name, status, record_count=0, error_message=None):
    """Upsert tracker state. file_name is the unique key."""
    _doris_hook().run(
        f"""
        INSERT INTO {TRACKER_TABLE}
            (file_name, status, processed_at, record_count, error_message)
        VALUES (%s, %s, NOW(), %s, %s)
        """,
        parameters=(
            file_name,
            status,
            record_count,
            (str(error_message)[:1000] if error_message else None),
        ),
    )


def _read_and_validate_csv(file_name, expected_columns, date_columns):
    raw = _minio_hook().read_key(
        key=file_name,
        bucket_name=MINIO_BUCKET,
    )
    rows = list(csv.DictReader(StringIO(raw)))

    # Validate exact required headers before writing to Doris.
    if not rows:
        # Header-only files are valid CSV but are not useful incremental batches.
        raise ValueError(f"{file_name} contains no data rows.")

    reader = csv.DictReader(StringIO(raw))
    if reader.fieldnames is None:
        raise ValueError(f"{file_name} has no CSV header.")

    missing_columns = sorted(set(expected_columns) - set(reader.fieldnames))
    if missing_columns:
        raise ValueError(
            f"{file_name} is missing required columns: {missing_columns}"
        )

    for row_number, row in enumerate(rows, start=2):
        for column in expected_columns:
            value = (row.get(column) or "").strip()
            if not value:
                raise ValueError(
                    f"{file_name}, row {row_number}: {column} is missing."
                )

        for column in date_columns:
            value = row[column].strip()
            try:
                parsed = datetime.strptime(value, "%Y-%m-%d")
                if parsed.strftime("%Y-%m-%d") != value:
                    raise ValueError()
            except ValueError:
                raise ValueError(
                    f"{file_name}, row {row_number}: invalid {column} "
                    f"'{value}'; expected YYYY-MM-DD."
                )

    return rows


def _load_customer_file(file_name):
    rows = _read_and_validate_csv(
        file_name,
        CUSTOMER_COLUMNS,
        date_columns=["created_date"],
    )

    values = [
        (
            row["customer_id"].strip(),
            row["customer_name"].strip(),
            row["city"].strip(),
            row["email"].strip().lower(),
            row["created_date"].strip(),
        )
        for row in rows
    ]

    _doris_hook().insert_rows(
        table=CUSTOMERS_TABLE,
        rows=values,
        target_fields=CUSTOMER_COLUMNS,
        commit_every=1000,
    )
    return len(values)


def _load_order_file(file_name):
    rows = _read_and_validate_csv(
        file_name,
        ORDER_COLUMNS,
        date_columns=["order_date"],
    )

    values = []
    for row_number, row in enumerate(rows, start=2):
        try:
            quantity = int(row["quantity"])
            unit_price = Decimal(row["unit_price"])
        except (ValueError, InvalidOperation):
            raise ValueError(
                f"{file_name}, row {row_number}: quantity/unit_price "
                "must be numeric."
            )

        if quantity <= 0:
            raise ValueError(
                f"{file_name}, row {row_number}: quantity must be greater than 0."
            )
        if unit_price < 0:
            raise ValueError(
                f"{file_name}, row {row_number}: unit_price cannot be negative."
            )

        total_amount = (Decimal(quantity) * unit_price).quantize(Decimal("0.01"))
        values.append((
            row["order_id"].strip(),
            row["customer_id"].strip(),
            row["order_date"].strip(),
            quantity,
            str(unit_price),
            str(total_amount),
        ))

    _doris_hook().insert_rows(
        table=ORDERS_TABLE,
        rows=values,
        target_fields=[
            "order_id",
            "customer_id",
            "order_date",
            "quantity",
            "unit_price",
            "total_amount",
        ],
        commit_every=1000,
    )
    return len(values)


def _process_one_file(file_name, loader):
    """Skip successful files; record PROCESSING, SUCCESS, or FAILED."""
    if _tracker_status(file_name) == "SUCCESS":
        print(f"SKIP: already processed successfully: {file_name}")
        return {"file_name": file_name, "status": "SKIPPED", "record_count": 0}

    _set_tracker(file_name, "PROCESSING")
    try:
        record_count = loader(file_name)
        _set_tracker(file_name, "SUCCESS", record_count=record_count)
        print(f"SUCCESS: {file_name}; rows={record_count}")
        return {
            "file_name": file_name,
            "status": "SUCCESS",
            "record_count": record_count,
        }
    except Exception as exc:
        try:
            _set_tracker(file_name, "FAILED", error_message=str(exc))
        except Exception as tracker_exc:
            print(f"Could not record FAILED status: {tracker_exc}")
        print(f"FAILED: {file_name}: {exc}")
        raise


def run_incremental_ingestion():
    """Process all currently available new customer and order CSV objects."""
    customer_files = _list_csv_keys(CUSTOMERS_PREFIX)
    order_files = _list_csv_keys(ORDERS_PREFIX)

    if not customer_files and not order_files:
        raise FileNotFoundError(
            "No CSV files found under the configured customer/order MinIO prefixes."
        )

    results = []
    # Load customer batches before order batches.
    for file_name in customer_files:
        results.append(_process_one_file(file_name, _load_customer_file))

    for file_name in order_files:
        results.append(_process_one_file(file_name, _load_order_file))

    processed = sum(item["status"] == "SUCCESS" for item in results)
    skipped = sum(item["status"] == "SKIPPED" for item in results)
    print(
        f"Incremental run complete. Newly processed files={processed}; "
        f"already-successful files skipped={skipped}."
    )
