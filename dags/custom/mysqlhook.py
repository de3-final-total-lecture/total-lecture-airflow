from airflow.providers.mysql.hooks.mysql import MySqlHook


class CustomMySqlHook(MySqlHook):
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

    def bulk_load(self, table: str, tmp_file: str, delimiter: str = "\t") -> None:
        """Loads a file into a database table with the specified delimiter"""
        conn = self.get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            LOAD DATA LOCAL INFILE '{tmp_file}'
            INTO TABLE {table}
            FIELDS TERMINATED BY '{delimiter}'
            """.format(
                tmp_file=tmp_file, table=table, delimiter=delimiter
            )
        )
        conn.commit()
