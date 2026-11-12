
from datetime import datetime, timedelta, timezone

import requests
from pymongo import MongoClient, ReplaceOne

from airflow import DAG
from airflow.operators.python import PythonOperator


# ------------------------------------------
# CONFIGURATION
# ------------------------------------------

API_URL = "https://api.frankfurter.dev/v2/rates"

BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = "USD,GBP,JPY"

MONGO_URI = (
    "mongodb://admin:password123@mongo:27017/"
    "?authSource=admin"
)

DATABASE_NAME = "currency_db"
COLLECTION_NAME = "exchange_rates"


# ------------------------------------------
# TASK 1: EXTRACT
# ------------------------------------------

def extract_currency_rates(ti):

    print("Extracting currency rates...")

    params = {
        "base": BASE_CURRENCY,
        "quotes": TARGET_CURRENCIES
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    rates = response.json()

    print(f"Extracted {len(rates)} exchange rates")

    # Named XCom push
    ti.xcom_push(
        key="extracted_rates",
        value=rates
    )


# ------------------------------------------
# TASK 2: TRANSFORM
# ------------------------------------------

def transform_currency_rates(ti):

    print("Transforming currency data...")

    # Named XCom pull from Task 1
    extracted_data = ti.xcom_pull(
        task_ids="extract_currency_rates",
        key="extracted_rates"
    )

    retrieved_at = datetime.now(
        timezone.utc
    ).isoformat(timespec="seconds")

    transformed_data = []

    for item in extracted_data:

        record = {
            "base_currency": item["base"],
            "target_currency": item["quote"],
            "exchange_rate": item["rate"],
            "rate_date": item["date"],
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at
        }

        transformed_data.append(record)

    print(f"Transformed {len(transformed_data)} records")

    # Named XCom push
    ti.xcom_push(
        key="transformed_rates",
        value=transformed_data
    )


# ------------------------------------------
# TASK 3: LOAD
# ------------------------------------------

def load_currency_rates(ti):

    print("Loading data into MongoDB...")

    # Named XCom pull from Task 2
    transformed_data = ti.xcom_pull(
        task_ids="transform_currency_rates",
        key="transformed_rates"
    )

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=10000
    )

    try:
        # Check database connection
        client.admin.command("ping")

        db = client[DATABASE_NAME]

        collection = db[COLLECTION_NAME]

        # Prevent duplicate records for the same date
        collection.create_index(
            [
                ("base_currency", 1),
                ("target_currency", 1),
                ("rate_date", 1)
            ],
            unique=True
        )

        operations = []

        for record in transformed_data:

            operations.append(
                ReplaceOne(
                    {
                        "base_currency": record["base_currency"],
                        "target_currency": record["target_currency"],
                        "rate_date": record["rate_date"]
                    },
                    record,
                    upsert=True
                )
            )

        if operations:
            result = collection.bulk_write(
                operations,
                ordered=True
            )

            print(
                f"Matched: {result.matched_count}, "
                f"Inserted: {result.upserted_count}, "
                f"Modified: {result.modified_count}"
            )

        print("Data successfully loaded into MongoDB!")

    finally:
        client.close()


# ------------------------------------------
# DAG CONFIGURATION
# ------------------------------------------

default_args = {
    "owner": "currency_etl",
    "retries": 2,
    "retry_delay": timedelta(minutes=2)
}


with DAG(
    dag_id="daily_currency_etl",
    default_args=default_args,
    description="Daily currency exchange rate ETL pipeline",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    tags=["ETL", "Currency", "MongoDB"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates
    )

    transform_task = PythonOperator(
        task_id="transform_currency_rates",
        python_callable=transform_currency_rates
    )

    load_task = PythonOperator(
        task_id="load_currency_rates",
        python_callable=load_currency_rates
    )

    # Task dependencies
    extract_task >> transform_task >> load_task
