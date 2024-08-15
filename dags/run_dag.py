from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id="glue_job_dag",
    default_args=default_args,
    description="Run Glue job with execution date set to 3 days ago",
    schedule_interval="@daily",  # 매일 실행
    start_date=datetime.now() - timedelta(days=3),  # 3일 전으로 설정
    catchup=True,  # 과거 날짜의 작업을 실행하도록 설정
    tags=["example"],
) as dag:

    # Glue 작업 정의
    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="jun-1-s3-to-rds",
        region_name="ap-northeast-2",
        iam_role_name="jun-1-glue-role",
        aws_conn_id="aws_s3_connection",
    )

    run_glue_job
