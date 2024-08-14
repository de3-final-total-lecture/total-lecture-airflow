from test_send_email import TestEmailOperator

from airflow import DAG
from airflow.utils.dates import days_ago

import pendulum

kst = pendulum.timezone("Asia/Seoul")

# 기본 설정
default_args = {
    "owner": "zjacom",
    "depends_on_past": False,
    "start_date": kst.convert(days_ago(1)),
}

with DAG(
    "test_send_email_dag",
    default_args=default_args,
    schedule_interval=None,
) as dag:

    test_send_email = TestEmailOperator(
        task_id="test",
    )
