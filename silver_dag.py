from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

BUCKET_NAME = "s3://business-insights-assessment-bucket"
DATABASE_NAME = "bia_db"
ATHENA_RESULTS = f"{BUCKET_NAME}/athena-results/"

with DAG(
    dag_id='silver_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={'retries': 1},
    tags=['silver', 'athena'],
) as dag:

    create_silver_order_items = AthenaOperator(
        task_id='create_silver_order_items',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.silver_order_items 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/silver/order_items/'
            ) AS 
            SELECT 
                TRIM(app_name) AS app_name,
                TRIM(restaurant_id) AS restaurant_id,
                CAST(creation_time_utc AS timestamp) AS creation_time,
                TRIM(order_id) AS order_id,
                TRIM(user_id) AS user_id,
                TRIM(printed_card_number) AS printed_card_number,
                is_loyalty,
                TRIM(currency) AS currency,
                TRIM(lineitem_id) AS lineitem_id,
                TRIM(item_category) AS item_category,
                TRIM(item_name) AS item_name,
                item_price,
                item_quantity
            FROM {DATABASE_NAME}.bronze_order_items
            WHERE order_id IS NOT NULL;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    create_silver_order_item_options = AthenaOperator(
        task_id='create_silver_order_item_options',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.silver_order_item_options 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/silver/order_item_options/'
            ) AS 
            SELECT 
                TRIM(order_id) AS order_id,
                TRIM(lineitem_id) AS lineitem_id,
                TRIM(option_group_name) AS option_group_name,
                TRIM(option_name) AS option_name,
                option_price
            FROM {DATABASE_NAME}.bronze_order_item_options
            WHERE order_id IS NOT NULL;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    create_silver_date_dim = AthenaOperator(
        task_id='create_silver_date_dim',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.silver_date_dim 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/silver/date_dim/'
            ) AS 
            SELECT 
                TRIM(date_key) AS date_key,
                CAST(date_value AS date) AS calendar_date,
                year,
                quarter,
                month,
                day,
                TRIM(day_of_week) AS day_of_week
            FROM {DATABASE_NAME}.bronze_date_dim;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    [
        create_silver_order_items,
        create_silver_order_item_options,
        create_silver_date_dim,
    ]
