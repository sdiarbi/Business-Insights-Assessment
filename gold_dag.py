from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

BUCKET_NAME = "s3://business-insights-assessment-bucket"
DATABASE_NAME = "bia_db"
ATHENA_RESULTS = f"{BUCKET_NAME}/athena-results/"

with DAG(
    dag_id='gold_dag',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={'retries': 1},
    tags=['gold', 'athena'],
) as dag:

    # 1. Customer Lifetime Value (CLV)
    create_gold_clv = AthenaOperator(
        task_id='create_gold_customer_lifetime_value',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_customer_lifetime_value 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/gold/customer_lifetime_value/'
            ) AS 
            WITH customer_spend AS (
                SELECT 
                    user_id,
                    SUM(item_price * item_quantity) AS total_monetary,
                    NTILE(5) OVER (ORDER BY SUM(item_price * item_quantity) DESC) AS clv_ntile
                FROM {DATABASE_NAME}.silver_order_items
                WHERE user_id IS NOT NULL
                GROUP BY user_id
            )
            SELECT 
                user_id,
                total_monetary AS estimated_clv,
                CASE 
                    WHEN clv_ntile = 1 THEN 'High CLV'
                    WHEN clv_ntile IN (2, 3, 4) THEN 'Medium CLV'
                    ELSE 'Low CLV'
                END AS clv_tier
            FROM customer_spend;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 2. Customer Segmentation & Behavior (RFM)
    create_gold_segmentation = AthenaOperator(
        task_id='create_gold_customer_segmentation',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_customer_segmentation 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/gold/customer_segmentation/'
            ) AS 
            SELECT 
                user_id,
                DATE_DIFF('day', MAX(CAST(creation_time AS date)), CURRENT_DATE) AS recency_days,
                COUNT(DISTINCT order_id) AS frequency_purchases,
                SUM(item_price * item_quantity) AS monetary_spend,
                CASE 
                    WHEN COUNT(DISTINCT order_id) >= 5 AND DATE_DIFF('day', MAX(CAST(creation_time AS date)), CURRENT_DATE) <= 30 THEN 'VIP'
                    WHEN COUNT(DISTINCT order_id) <= 2 AND DATE_DIFF('day', MAX(CAST(creation_time AS date)), CURRENT_DATE) <= 14 THEN 'New Customer'
                    WHEN DATE_DIFF('day', MAX(CAST(creation_time AS date)), CURRENT_DATE) > 45 THEN 'Churn Risk'
                    ELSE 'Regular'
                END AS customer_segment
            FROM {DATABASE_NAME}.silver_order_items
            WHERE user_id IS NOT NULL
            GROUP BY user_id;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 3. Churn Indicators
    create_gold_churn = AthenaOperator(
        task_id='create_gold_churn_indicators',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_churn_indicators 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/gold/churn_indicators/'
            ) AS 
            SELECT 
                user_id,
                MAX(creation_time) AS last_order_timestamp,
                DATE_DIFF('day', MAX(CAST(creation_time AS date)), CURRENT_DATE) AS days_since_last_order,
                CASE 
                    WHEN DATE_DIFF('day', MAX(CAST(creation_time AS date)), CURRENT_DATE) > 45 THEN 'At Risk'
                    ELSE 'Active'
                END AS churn_status
            FROM {DATABASE_NAME}.silver_order_items
            WHERE user_id IS NOT NULL
            GROUP BY user_id;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 4. Sales Trends Monitoring
    create_gold_trends = AthenaOperator(
        task_id='create_gold_sales_trends',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_sales_trends 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/gold/sales_trends/'
            ) AS 
            SELECT 
                SUBSTRING(CAST(creation_time AS VARCHAR), 1, 10) AS order_date,
                restaurant_id,
                item_category,
                COUNT(DISTINCT order_id) AS total_orders,
                SUM(item_quantity) AS total_items_sold,
                SUM(item_price * item_quantity) AS total_revenue
            FROM {DATABASE_NAME}.silver_order_items
            GROUP BY 1, 2, 3;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 5. Loyalty Program Impact
    create_gold_loyalty = AthenaOperator(
        task_id='create_gold_loyalty_program_impact',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_loyalty_program_impact 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/gold/loyalty_program_impact/'
            ) AS 
            SELECT 
                COALESCE(CAST(is_loyalty AS VARCHAR), 'false') AS is_loyalty_member,
                COUNT(DISTINCT user_id) AS unique_customers,
                COUNT(DISTINCT order_id) AS total_orders,
                SUM(item_price * item_quantity) AS total_revenue,
                AVG(item_price * item_quantity) AS average_item_spend,
                SUM(item_price * item_quantity) / NULLIF(COUNT(DISTINCT user_id), 0) AS avg_spend_per_customer
            FROM {DATABASE_NAME}.silver_order_items
            GROUP BY is_loyalty;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 6. Top-Performing Locations
    create_gold_locations = AthenaOperator(
        task_id='create_gold_top_locations',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_top_locations 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/gold/top_locations/'
            ) AS 
            SELECT 
                restaurant_id AS location_id,
                COUNT(DISTINCT order_id) AS total_orders,
                SUM(item_price * item_quantity) AS total_revenue,
                AVG(item_price * item_quantity) AS average_order_value,
                RANK() OVER (ORDER BY SUM(item_price * item_quantity) DESC) AS revenue_rank
            FROM {DATABASE_NAME}.silver_order_items
            GROUP BY restaurant_id;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # 7. Pricing & Discount Effectiveness
    create_gold_discounts = AthenaOperator(
        task_id='create_gold_pricing_discount_analysis',
        query=f"""
            CREATE TABLE IF NOT EXISTS {DATABASE_NAME}.gold_pricing_discount_analysis 
            WITH (
                format = 'PARQUET',
                external_location = '{BUCKET_NAME}/gold/pricing_discount_analysis/'
            ) AS 
            WITH order_discounts AS (
                SELECT DISTINCT 
                    order_id,
                    1 AS has_discount
                FROM {DATABASE_NAME}.silver_order_item_options
                WHERE option_price < 0
            ),
            order_totals AS (
                SELECT 
                    order_id,
                    SUM(item_price * item_quantity) AS order_revenue
                FROM {DATABASE_NAME}.silver_order_items
                GROUP BY order_id
            )
            SELECT 
                CASE WHEN d.has_discount = 1 THEN 'Discounted Order' ELSE 'Non-Discounted Order' END AS order_type,
                COUNT(DISTINCT t.order_id) AS total_orders,
                SUM(t.order_revenue) AS total_revenue,
                AVG(t.order_revenue) AS avg_order_revenue
            FROM order_totals t
            LEFT JOIN order_discounts d ON t.order_id = d.order_id
            GROUP BY 1;
        """,
        database=DATABASE_NAME,
        output_location=ATHENA_RESULTS,
    )

    # Run all 7 gold table creation tasks in parallel
    [
        create_gold_clv,
        create_gold_segmentation,
        create_gold_churn,
        create_gold_trends,
        create_gold_loyalty,
        create_gold_locations,
        create_gold_discounts,
    ]
