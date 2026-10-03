from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# Single bucket setup
BUCKET_NAME = "s3://business-insights-assessment-raw-bucket"
ATHENA_RESULTS = f"{BUCKET_NAME}/athena-results/"
DATABASE_NAME = "bia_db"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='bronze_dag',
    default_args=default_args,
    description='Medallion Bronze Layer: Registers raw S3 CSVs into Athena',
    schedule_interval=None,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['medallion', 'bronze', 'athena'],
) as dag:

    # 1. Create Athena Database
    create_database = AthenaOperator(
        task_id='create_database',
        query=f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};",
        database='default',
        output_location=ATHENA_RESULTS,
    )

    # 2. Register Bronze Order Items Table
    create_bronze_order_items = AthenaOperator(
        task_id='create_bronze_order_items',
        query=f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_order_items (
                app_name string,
                restaurant_id string,
                creation_time_utc string,
                order_id string,
                user_id string,
                printed_card_number string,
                is_loyalty boolean,
                currency string,
                lineitem_id string,
                item_category string,
                item_name string,
                item_price decimal(10,2),
                item_quantity int
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            LOCATION '{BUCKET_NAME}/bronze/order_items/'
            TBLPROPERTIES ('skip.header.line.count'='1');
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 3. Register Bronze Order Item Options Table
    create_bronze_order_item_options = AthenaOperator(
        task_id='create_bronze_order_item_options',
        query=f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_order_item_options (
                order_id string,
                lineitem_id string,
                option_group_name string,
                option_name string,
                option_price decimal(10,2),
                option_quantity int
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            LOCATION '{BUCKET_NAME}/bronze/order_item_options/'
            TBLPROPERTIES ('skip.header.line.count'='1');
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 4. Register Bronze Date Dimension Table
    create_bronze_date_dim = AthenaOperator(
        task_id='create_bronze_date_dim',
        query=f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_date_dim (
                date_key string,
                day_of_week string,
                week int,
                month string,
                year int,
                is_weekend boolean,
                is_holiday boolean,
                holiday_name string
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            LOCATION '{BUCKET_NAME}/bronze/date_dim/'
            TBLPROPERTIES ('skip.header.line.count'='1');
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # Execution Flow: Database is created first, then all 3 tables register in parallel
    create_database >> [
        create_bronze_order_items,
        create_bronze_order_item_options,
        create_bronze_date_dim
    ]
