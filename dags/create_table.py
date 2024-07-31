from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago

import pendulum

kst = pendulum.timezone("Asia/Seoul")
default_args = {
    "owner": "zjacom",
    "start_date": kst.convert(days_ago(1)),
}

with DAG(
    "create_tables",
    default_args=default_args,
    description="DAG to create tables",
    schedule_interval=None,
) as dag:
    create_users_table = MySqlOperator(
        task_id="create_users_table",
        sql="""CREATE TABLE IF NOT EXISTS Users (
            user_id INT AUTO_INCREMENT PRIMARY KEY,
            user_name VARCHAR(16),
            user_email VARCHAR(255) NOT NULL UNIQUE,
            user_password VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );""",
        mysql_conn_id="mysql_conn",
    )

    create_wish_list_table = MySqlOperator(
        task_id="create_wish_list_table",
        sql="""CREATE TABLE IF NOT EXISTS Wish_list (
            lecture_id VARCHAR(255) PRIMARY KEY,
            user_id INT,
            lecture_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        mysql_conn_id="mysql_conn",
    )

    create_category_table = MySqlOperator(
        task_id="create_category_table",
        sql="""CREATE TABLE IF NOT EXISTS Category (
            category_id INT AUTO_INCREMENT PRIMARY KEY,
            main_category_name VARCHAR(255),
            mid_category_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );""",
        mysql_conn_id="mysql_conn",
    )

    ################################################
    create_lecture_info_table = MySqlOperator(
        task_id="create_lecture_info_table",
        sql="""
        CREATE TABLE IF NOT EXISTS Lecture_info (
            lecture_id VARCHAR(255) PRIMARY KEY,
            lecture_name VARCHAR(255),
            price INT,
            description VARCHAR(5000),
            what_do_i_learn VARCHAR(8191),
            tag VARCHAR(255),
            level VARCHAR(255),
            teacher VARCHAR(255),
            scope FLOAT,
            review_count INT,
            lecture_time VARCHAR(255),
            thumbnail_url VARCHAR(511),
            is_new BOOLEAN,
            is_recommend BOOLEAN,
            platform_name VARCHAR(32),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );""",
        mysql_conn_id="mysql_conn",
    )

    create_category_conn_table = MySqlOperator(
        task_id="create_category_conn_table",
        sql="""
        CREATE TABLE IF NOT EXISTS Category_conn (
            lecture_id VARCHAR(255),
            category_id INT
        );""",
        mysql_conn_id="mysql_conn",
    )

    create_lecture_price_history_table = MySqlOperator(
        task_id="create_lecture_price_history_table",
        sql="""
        CREATE TABLE IF NOT EXISTS Lecture_price_history (
            lecture_id VARCHAR(255),
            price INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (lecture_id, created_at)
        );""",
        mysql_conn_id="mysql_conn",
    )

    create_review_analysis_table = MySqlOperator(
        task_id="create_review_analysis_table",
        sql="""
        CREATE TABLE IF NOT EXISTS Review_analysis (
            lecture_id VARCHAR(255),
            lecture_name VARCHAR(255),
            result VARCHAR(1023),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );""",
        mysql_conn_id="mysql_conn",
    )

    create_inflearn_id_table = MySqlOperator(
        task_id="create_inflearn_id_table",
        sql="""
        CREATE TABLE IF NOT EXISTS Inflearn_id (
            lecture_id VARCHAR(255) PRIMARY KEY,
            inflearn_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        mysql_conn_id="mysql_conn",
    )
    
    create_udemy_table = MySqlOperator(
        task_id="create_udemy_table",
        sql="""
        CREATE TABLE IF NOT EXISTS Udemy(
            course_id int(255),
            lecture_id VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );""",
        mysql_conn_id="mysql_conn"
    )