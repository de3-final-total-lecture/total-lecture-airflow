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

    # Task Group #2
    with TaskGroup("Process", tooltip="Tasks for processing data") as section_2:
        load_udemy_info_by_recent = UdemyInfoToS3Operator(
            task_id="load_udemy_info_by_recent",
            bucket_name="team-jun-1-bucket",
            pull_prefix="crawling_keyword/encoded_keyword.json",
            sort_type="newest",
        )
        load_udemy_info_by_recent,

    end = EmptyOperator(task_id="end")

    start >> section_2 >> end
