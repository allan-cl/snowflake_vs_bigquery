import os
from dotenv import load_dotenv

import snowflake.connector

from tests.profile_utils import BenchmarkProfiler

load_dotenv()


class SnowflakeBenchmark:
    def __init__(self):
        self.connection = None
        self.profiler = BenchmarkProfiler()
        self.user = os.environ.get("SF_USERNAME")
        self.password = os.environ.get("SF_PASSWORD")
        self.account = os.environ.get("SF_ACCOUNT")

    def connect(self):
        self.profiler.start()
        self.connection = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            account=self.account
        )
        return self.profiler.stop()

    def run_select(self, query):
        cursor = self.connection.cursor()
        self.profiler.start()
        cursor.execute(query)
        cursor.fetchall()
        return self.profiler.stop()

    def insert_data(self, table, values):
        cursor = self.connection.cursor()
        query = f"INSERT INTO {table} VALUES {values}"
        self.profiler.start()
        cursor.execute(query)
        return self.profiler.stop()

    def update_data(self, table, set_clause, where_clause):
        cursor = self.connection.cursor()
        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self.timer.start()
        cursor.execute(query)
        return self.timer.stop()
