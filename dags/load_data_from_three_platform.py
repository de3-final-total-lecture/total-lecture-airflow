from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum
from dags.coursera.operator import CourseraPreInfoToS3Operator, CourseraInfoToS3Operator
from dags.inflearn.operator import (
    InflearnPreInfoToS3Operator,
    InflearnInfoToS3Operator,
    InflearnCategoryConnectionOperator,
)
from dags.udemy.operator import UdemyInfoToS3Operator
from dags.custom.s3_to_rds_operator import S3ToRDSOperator
from dags.openai.operator import (
    OpenAICategoryConnectionOperator,
    OpenAIReviewAnalysisOperator,
)
from datetime import timedelta
from plugins.my_slack import on_failure_callback, on_success_callback

kst = pendulum.timezone("Asia/Seoul")
from airflow.utils.dates import days_ago

with DAG(
    dag_id="load_data_from_three_platform",
    start_date=kst.convert(days_ago(1)),
    schedule_interval=None,  # 매주 수요일 오후 12시에 실행
    catchup=False,
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(
        "get_lecture_pre_informations",
        tooltip="Tasks for load lecture url and id to s3",
    ) as section_1:
        load_coursera_pre_info = CourseraPreInfoToS3Operator(
            task_id="load_coursera_pre_info",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            push_prefix="raw_data/URL",
            retries=3,
            retry_delay=timedelta(minutes=5),
        )
        load_inlearn_pre_info = InflearnPreInfoToS3Operator(
            task_id="load_inlearn_pre_info",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            push_prefix="raw_data/URL",
        )

    with TaskGroup(
        "get_lecture_informations", tooltip="Tasks for load lecture informations to s3"
    ) as section_2:
        load_coursera_lecture_info = CourseraInfoToS3Operator(
            task_id="load_coursera_lecture_info",
            bucket_name="team-jun-1-bucket",
            pull_prefix="raw_data/URL",
            push_prefix="product",
        )
        load_coursera_reviews = CourseraInfoToS3Operator(
            task_id="load_coursera_reviews",
            bucket_name="team-jun-1-bucket",
            pull_prefix="raw_data/URL",
            push_prefix="analytics/reviews",
        )
        load_inflearn_lecture_info = InflearnInfoToS3Operator(
            task_id="load_inflearn_lecture_info",
            bucket_name="team-jun-1-bucket",
            pull_prefix="raw_data/URL",
            push_prefix="product",
        )
        load_inflearn_reviews = InflearnInfoToS3Operator(
            task_id="load_inflearn_reviews",
            bucket_name="team-jun-1-bucket",
            pull_prefix="raw_data/URL",
            push_prefix="analytics/reviews",
        )
        load_udemy_info_by_recommend = UdemyInfoToS3Operator(
            task_id="load_udemy_info_by_recommend",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            sort_type="most-num_reviews",
        )
        load_udemy_info_by_recent = UdemyInfoToS3Operator(
            task_id="load_udemy_info_by_recent",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            sort_type="newest",
        )
        [
            load_coursera_lecture_info,
            load_coursera_reviews,
            load_inflearn_lecture_info,
            load_inflearn_reviews,
            load_udemy_info_by_recommend,
            load_udemy_info_by_recent,
        ]

    with TaskGroup(
        "load_lecture_data_into_rds",
        tooltip="Tasks for transform json file to csv. And load data in RDS table.",
    ) as section_3:
        run_glue_job = GlueJobOperator(
            task_id="run_glue_job",
            job_name="jun-1-s3-to-rds",
            region_name="ap-northeast-2",
            iam_role_name="jun-1-glue-role",
            aws_conn_id="aws_s3_connection",
        )
        load_lecture_data_from_s3_to_rds = S3ToRDSOperator(
            task_id="load_lecture_data_from_csv_to_rds",
            bucket_name="team-jun-1-bucket",
            pull_prefix="product",
            push_table="Lecture_info",
        )
        run_glue_job >> load_lecture_data_from_s3_to_rds

    with TaskGroup(
        "create_lecture_category_connections",
        tooltip="Tasks for create lecture category connections from three platform",
    ) as section_4:
        create_inflearn_category_conn = InflearnCategoryConnectionOperator(
            task_id="create_inflearn_category_conn",
            bucket_name="team-jun-1-bucket",
            pull_prefix="product",
            push_table="Category_conn",
        )

        create_udemy_coursera_category_conn_by_open_ai = (
            OpenAICategoryConnectionOperator(
                task_id="create_udemy_coursera_category_conn_by_open_ai",
                bucket_name="team-jun-1-bucket",
                pull_prefix="product",
                push_table="Category_conn",
            )
        )

    create_review_analysis_by_open_ai = OpenAIReviewAnalysisOperator(
        task_id="create_review_analysis_by_open_ai",
        bucket_name="team-jun-1-bucket",
        pull_prefix="analytics/reviews",
        push_table="Review_analysis",
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> section_1
        >> section_2
        >> section_3
        >> section_4
        >> create_review_analysis_by_open_ai
        >> end
    )
