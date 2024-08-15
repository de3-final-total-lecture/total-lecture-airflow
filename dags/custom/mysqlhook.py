from airflow.providers.mysql.hooks.mysql import MySqlHook


class CustomMySqlHook(MySqlHook):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.local_infile = kwargs.pop("local_infile", True)

    def bulk_insert(self, sql, parameters):
        """
        Execute the provided SQL query with parameters against the MySQL database.

        :param sql: The SQL query to execute.
        :param parameters: The parameters to substitute into the query.
        """
        self.log.info("Running SQL: %s", sql)
        self.log.info("With parameters: %s", parameters)

        conn = self.get_conn()
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, parameters)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.log.error("Error executing SQL: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def bulk_load(self, table: str, tmp_file: str) -> None:
        conn = self.get_conn()
        cur = conn.cursor()

        # 테이블의 컬럼 정보 가져오기
        cur.execute(f"DESCRIBE {table}")
        columns = [column[0] for column in cur.fetchall()]

        # CSV 파일의 컬럼 순서에 맞게 매핑
        load_data_sql = f"""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            FIELDS TERMINATED BY ';' 
            IGNORE 1 ROWS
            (
                lecture_id,
                lecture_url,
                lecture_name,
                origin_price,
                price,
                @description,
                @what_do_i_learn,
                @tag,
                @level,
                teacher,
                scope,
                review_count,
                lecture_time,
                thumbnail_url,
                @is_new,
                @is_recommend,
                platform_name,
                keyword,
                like_count,
                @created_at,
                @updated_at
            )
            SET
                description = IFNULL(@description, 'default_description'),
                what_do_i_learn = IFNULL(@what_do_i_learn, 'default_learn'),
                tag = IFNULL(@tag, 'default_tag'),
                level = IFNULL(@level, 'default_level'),
                is_new = IF(LOWER(TRIM(@is_new)) = 'true', 1, 0),
                is_recommend = IF(LOWER(TRIM(@is_recommend)) = 'true', 1, 0),
                created_at = IFNULL(NULLIF(@created_at, ''), NOW()),
                updated_at = IFNULL(NULLIF(@updated_at, ''), NOW());
        """
        cur.execute(load_data_sql)
        conn.commit()
