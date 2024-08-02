from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
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
from dags.openai.operator import OpenAICategoryConnectionOperator
from datetime import timedelta

kst = pendulum.timezone("Asia/Seoul")

with DAG(
    dag_id="test_s3_to_rds",
    start_date=pendulum.today(tz=kst).subtract(days=1),
) as dag:
    start = EmptyOperator(task_id="start")

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

    end = EmptyOperator(task_id="end")

    start >> section_3 >> end
