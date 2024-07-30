from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import pendulum
from dags.coursera.operator import CourseraPreInfoToS3Operator, CourseraInfoToS3Operator
from dags.inflearn.operator import InflearnPreInfoToS3Operator, InflearnInfoToS3Operator
from dags.udemy.operator import UdemyInfoToS3Operator

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="test_task_group",
    start_date=pendulum.today(tz=kst).subtract(days=1),
    tags=["example"],
) as dag:
    start = EmptyOperator(task_id="start")

    # Task Group #1
    with TaskGroup(
        "Get_lecture_pre_informations", tooltip="Tasks for downloading data"
    ) as section_1:
        load_coursera_pre_info = CourseraPreInfoToS3Operator(
            task_id="load_coursera_pre_info",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            push_prefix="raw_data/URL",
        )
        load_inlearn_pre_info = InflearnPreInfoToS3Operator(
            task_id="load_inlearn_pre_info",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            push_prefix="raw_data/URL",
        )

    # Task Group #2
    with TaskGroup("Process", tooltip="Tasks for processing data") as section_2:
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
            # load_coursera_lecture_info,
            # load_coursera_reviews,
            # load_inflearn_lecture_info,
            # load_inflearn_reviews,
            load_udemy_info_by_recommend,
            load_udemy_info_by_recent,
        ]

    end = EmptyOperator(task_id="end")

    start >> section_2 >> end
