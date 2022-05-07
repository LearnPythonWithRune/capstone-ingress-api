from datetime import datetime

import mysql.connector  # type: ignore


class Storage:
    def __init__(self, host: str):
        self.host = host

    def _setup_db(self):
        self.mydb = mysql.connector.connect(
            host=self.host,
            user="root",
            password="password"
        )

        self.cur = self.mydb.cursor()
        self.cur.execute("USE DB")

    def _query_db(self, sql_stmt: str):
        self._setup_db()
        self.cur.execute(sql_stmt)
        self.mydb.commit()
        self._close()

    def _close(self):
        self.cur.close()
        self.mydb.close()

    def ingest(self, crawl_time: datetime, ingest_time: datetime, pipeline_name: str,
               ingest_key: str, ingest_value: str):
        sql_stmt = f"INSERT INTO ingress(crawl_time, ingest_time, pipeline_name, ingest_key, ingest_value) " \
                   f"VALUES('{crawl_time}', '{ingest_time}', '{pipeline_name}', '{ingest_key}', '{ingest_value}')"
        self._query_db(sql_stmt)
