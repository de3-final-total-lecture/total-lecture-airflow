from airflow import DAG
from airflow.utils.dates import days_ago
from dags.custom.inflearn_to_s3_operator import InflearnToS3Operator
from dags.custom.data_parsing_functions import (
    parsing_lecture_details,
    parsing_lecture_reviews,
)

import pendulum


kst = pendulum.timezone("Asia/Seoul")
default_args = {
    "owner": "zjacom",
    "start_date": kst.convert(days_ago(1)),
    "retries": 1,
}

with DAG(
    "inflearn_get_lecture_info_reviews",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    lecture_info = InflearnToS3Operator(
        task_id="lecture_info",
        bucket_name="team-jun-1-bucket",
        pull_prefix="raw_data/URL",
        push_prefix="product",
        process_func=parsing_lecture_details,
    )
    review = InflearnToS3Operator(
        task_id="review",
        bucket_name="team-jun-1-bucket",
        pull_prefix="raw_data/URL",
        push_prefix="analytics/reviews",
        process_func=parsing_lecture_reviews,
    )

    [lecture_info, review]
