from airflow.providers.mysql.hooks.mysql import MySqlHook


class CustomMySqlHook(MySqlHook):
    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     self.local_infile = kwargs.pop("local_infile", True)

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

    # def bulk_load(self, table: str, tmp_file: str) -> None:
    #     conn = self.get_conn()
    #     cur = conn.cursor()
    #     cur.execute(
    #         """
    #         LOAD DATA LOCAL INFILE '{tmp_file}'
    #         IGNORE
    #         INTO TABLE {table}
    #         FIELDS TERMINATED BY ';'
    #         IGNORE 1 LINES;
    #         """.format(
    #             tmp_file=tmp_file, table=table
    #         )
    #     )
    #     conn.commit()
    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Load a tab-delimited file into a database table."""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            f"LOAD DATA LOCAL INFILE %s INTO TABLE {table}",
            (tmp_file,),
        )
        conn.commit()
        conn.close()  # type: ignore[misc]
