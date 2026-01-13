"""
QuickChat Spark ETL Airflow DAG

Orchestrates the Bronze -> Silver -> Gold ETL pipeline
Runs daily batch jobs and manages dependencies
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'quickchat-data-team',
    'depends_on_past': False,
    'email': ['data-alerts@quickchat.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Spark submit command template
SPARK_SUBMIT_CMD = """
spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 2 \
    --num-executors 2 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --class {main_class} \
    /opt/spark-etl/lib/quickchat-spark-etl.jar
"""

# DAG definition
with DAG(
    dag_id='quickchat_daily_etl',
    default_args=default_args,
    description='Daily ETL pipeline for QuickChat data lake',
    schedule_interval='0 2 * * *',  # Run at 2 AM daily
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['quickchat', 'etl', 'spark', 'daily'],
) as dag:

    # Start marker
    start = EmptyOperator(task_id='start')

    # ==========================================
    # BRONZE LAYER - Raw Data Extraction
    # ==========================================
    with TaskGroup(group_id='bronze_layer') as bronze_layer:

        bronze_messages = BashOperator(
            task_id='bronze_messages',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.bronze.BronzeMessagesJob'
            ),
        )

        bronze_users = BashOperator(
            task_id='bronze_users',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.bronze.BronzeUsersJob'
            ),
        )

        bronze_conversations = BashOperator(
            task_id='bronze_conversations',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.bronze.BronzeConversationsJob'
            ),
        )

        bronze_calls = BashOperator(
            task_id='bronze_calls',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.bronze.BronzeCallsJob'
            ),
        )

        # Bronze jobs can run in parallel
        [bronze_messages, bronze_users, bronze_conversations, bronze_calls]

    # ==========================================
    # SILVER LAYER - Cleaned & Enriched Data
    # ==========================================
    with TaskGroup(group_id='silver_layer') as silver_layer:

        silver_users = BashOperator(
            task_id='silver_users',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.silver.SilverUsersJob'
            ),
        )

        silver_messages = BashOperator(
            task_id='silver_messages',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.silver.SilverMessagesJob'
            ),
        )

        silver_user_activity = BashOperator(
            task_id='silver_user_activity',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.silver.SilverUserActivityJob'
            ),
        )

        # Silver jobs dependency: users first, then messages, then activity
        silver_users >> silver_messages >> silver_user_activity

    # ==========================================
    # GOLD LAYER - Aggregated Analytics
    # ==========================================
    with TaskGroup(group_id='gold_layer') as gold_layer:

        daily_metrics = BashOperator(
            task_id='daily_metrics',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.gold.DailyMetricsJob'
            ),
        )

        user_engagement = BashOperator(
            task_id='user_engagement',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.gold.UserEngagementJob'
            ),
        )

        ml_features = BashOperator(
            task_id='ml_features',
            bash_command=SPARK_SUBMIT_CMD.format(
                main_class='com.quickchat.etl.jobs.gold.MLFeaturesJob'
            ),
        )

        # Gold jobs can run in parallel after silver
        [daily_metrics, user_engagement, ml_features]

    # ==========================================
    # DATA QUALITY CHECKS
    # ==========================================
    with TaskGroup(group_id='quality_checks') as quality_checks:

        def check_data_quality(**context):
            """Run data quality checks on the processed data"""
            # Placeholder for data quality checks
            # Could use Great Expectations or custom checks
            print("Running data quality checks...")
            return True

        quality_check = PythonOperator(
            task_id='run_quality_checks',
            python_callable=check_data_quality,
        )

    # ==========================================
    # OPTIMIZATION TASKS
    # ==========================================
    with TaskGroup(group_id='optimization') as optimization:

        optimize_tables = BashOperator(
            task_id='optimize_delta_tables',
            bash_command="""
            spark-submit \
                --master spark://spark-master:7077 \
                --class com.quickchat.etl.utils.DeltaTableOptimizer \
                /opt/spark-etl/lib/quickchat-spark-etl.jar
            """,
            trigger_rule='all_success',
        )

    # End marker
    end = EmptyOperator(task_id='end')

    # ==========================================
    # DAG DEPENDENCIES
    # ==========================================
    start >> bronze_layer >> silver_layer >> gold_layer >> quality_checks >> optimization >> end


# ==========================================
# STREAMING DAG (Separate)
# ==========================================
with DAG(
    dag_id='quickchat_streaming_etl',
    default_args=default_args,
    description='Streaming ETL for real-time data ingestion',
    schedule_interval=None,  # Manually triggered or always-on
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['quickchat', 'etl', 'spark', 'streaming'],
) as streaming_dag:

    start_streaming = BashOperator(
        task_id='start_kafka_streaming',
        bash_command=SPARK_SUBMIT_CMD.format(
            main_class='com.quickchat.etl.jobs.streaming.KafkaStreamingJob'
        ),
    )


# ==========================================
# WEEKLY AGGREGATIONS DAG
# ==========================================
with DAG(
    dag_id='quickchat_weekly_etl',
    default_args=default_args,
    description='Weekly aggregations and reports',
    schedule_interval='0 4 * * 0',  # Run at 4 AM every Sunday
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['quickchat', 'etl', 'spark', 'weekly'],
) as weekly_dag:

    weekly_metrics = BashOperator(
        task_id='weekly_metrics',
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --class com.quickchat.etl.jobs.gold.WeeklyMetricsJob \
            /opt/spark-etl/lib/quickchat-spark-etl.jar
        """,
    )

    vacuum_tables = BashOperator(
        task_id='vacuum_delta_tables',
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --class com.quickchat.etl.utils.DeltaTableVacuum \
            /opt/spark-etl/lib/quickchat-spark-etl.jar
        """,
    )

    weekly_metrics >> vacuum_tables
