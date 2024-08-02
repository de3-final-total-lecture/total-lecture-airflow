from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
import pendulum
from dags.udemy.operator import UdemyInfoToS3Operator

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="test_udemy_etl_process",
    start_date=pendulum.today(tz=kst).subtract(days=1),
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup(
        "get_lecture_informations", tooltip="Tasks for load lecture informations to s3"
    ) as section_2:
        load_udemy_info_by_recommend = UdemyInfoToS3Operator(
            task_id="load_udemy_info_by_recommend",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            sort_type="most-num_reviews",
            client_id=Variable.get("Udemy_CLIENT_ID"),
            client_secret=Variable.get("Udemy_CLIENT_SECRET"),
            base_url=Variable.get("BASE_URL"),
        )
        load_udemy_info_by_recent = UdemyInfoToS3Operator(
            task_id="load_udemy_info_by_recent",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            sort_type="newest",
            client_id=Variable.get("Udemy_CLIENT_ID"),
            client_secret=Variable.get("Udemy_CLIENT_SECRET"),
            base_url=Variable.get("BASE_URL"),
        )
        [
            load_udemy_info_by_recommend,
            load_udemy_info_by_recent,
        ]

    end = EmptyOperator(task_id="end")

    start >> section_2 >> end
