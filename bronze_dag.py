from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# Define your configuration variables here
BUCKET_NAME = "s3://business-insights-assessment-bucket"
DATABASE_NAME = "bia_db"
ATHENA_RESULTS = f"{BUCKET_NAME}/athena-results/"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='bronze_dag',
    default_args=default_args,
    description='A DAG to create the bronze layer database and external tables in Athena',
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bronze', 'athena'],
) as dag:

    # 1. Create the Database
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
            LOCATION '{BUCKET_NAME}/bronze/'
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
                option_price decimal(10,2)
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            LOCATION '{BUCKET_NAME}/bronze/'
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
                date_value string,
                year int,
                quarter int,
                month int,
                day int,
                day_of_week string
            )
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            LOCATION '{BUCKET_NAME}/bronze/'
            TBLPROPERTIES ('skip.header.line.count'='1');
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # Define task dependencies
    create_database >> [
        create_bronze_order_items,
        create_bronze_order_item_options,
        create_bronze_date_dim,
    ]
