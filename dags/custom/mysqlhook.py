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

        # created_at과 updated_at을 제외한 컬럼 목록 생성
        regular_columns = [
            col for col in columns if col not in ("created_at", "updated_at")
        ]
        load_data_sql = f"""
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            FIELDS TERMINATED BY ';'
            IGNORE 1 LINES
            ({', '.join(regular_columns)}, @created_at, @updated_at)
            SET 
                created_at = IFNULL(@created_at, NOW()),
                updated_at = IFNULL(@updated_at, NOW())
        """
        cur.execute(load_data_sql)
        conn.commit()


# def bulk_load(self, table: str, tmp_file: str) -> None:
#         conn = self.get_conn()
#         cur = conn.cursor()
#         cur.execute(
#             """
#             LOAD DATA LOCAL INFILE '{tmp_file}'
#             IGNORE
#             INTO TABLE {table}
#             FIELDS TERMINATED BY ';'
#             IGNORE 1 LINES;
#             """.format(
#                 tmp_file=tmp_file, table=table
#             )
#         )
#         conn.commit()
