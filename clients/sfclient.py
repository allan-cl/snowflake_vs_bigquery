import os
from pathlib import Path
from dotenv import load_dotenv

import pandas as pd
import snowflake.connector

from utils.profilers import exec_time_profiler


load_dotenv()


class SnowflakeDBCursor:
    def __init__(self, database_name="test_db"):
        self.conn = None
        self.cursor = None
        self.db_name = database_name

    def __enter__(self):
        # Code to acquire the resource.
        self.conn = snowflake.connector.connect(
            user=os.environ.get("SF_USERNAME"),
            password=os.environ.get("SF_PASSWORD"),
            account=os.environ.get("SF_ACCOUNT"),
            database=self.db_name,
            schema="PUBLIC")
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        # Code to release the resource.
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


class SnowflakeClient:

    def __init__(self, database_name="test_db") -> None:
        self.db_name = database_name
        self.user = os.environ.get("SF_USERNAME")
        self.upwd = os.environ.get("SF_PASSWORD")
        self.acct = os.environ.get("SF_ACCOUNT")

        self.users_tbl_name = "users"
        self.orders_tbl_name = "orders"

    def get_sf_conn(self):
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.upwd,
            account=self.acct)
        return conn

    def create_database(self) -> None:
        self.delete_database()
        conn = self.get_sf_conn()
        cursor = conn.cursor()
        cursor.execute(command=f"DROP DATABASE IF EXISTS {self.db_name}")
        cursor.execute(command=f"CREATE DATABASE {self.db_name}")
        cursor.close()
        conn.close()

    def delete_database(self) -> None:
        conn = self.get_sf_conn()
        cursor = conn.cursor()
        cursor.execute(command=f"DROP DATABASE IF EXISTS {self.db_name}")
        cursor.close()
        conn.close()

    def create_users_table(self) -> None:
        with SnowflakeDBCursor(database_name=self.db_name) as cursor:
            cursor.execute(f"""
                CREATE OR REPLACE TABLE {self.users_tbl_name} (
                    user_id VARCHAR(36) PRIMARY KEY,
                    username VARCHAR(100),
                    email VARCHAR(100)
                )
            """)

    def delete_users_table(self) -> None:
        with SnowflakeDBCursor(database_name=self.db_name) as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.users_tbl_name}")

    def create_orders_table(self) -> None:
        with SnowflakeDBCursor(database_name=self.db_name) as cursor:
            cursor.execute(f"""
                CREATE OR REPLACE TABLE {self.orders_tbl_name} (
                    order_id VARCHAR(36) PRIMARY KEY,
                    user_id VARCHAR(36) REFERENCES users(user_id),
                    product VARCHAR(200),
                    amount INT,
                    order_time TIMESTAMP
                )
            """)

    def delete_orders_table(self) -> None:
        with SnowflakeDBCursor(database_name=self.db_name) as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {self.orders_tbl_name}")

    def _get_data_path(self) -> Path:
        return Path(__file__).resolve().parent.parent.joinpath("data")

    @exec_time_profiler
    def insert_users_data(self):
        with SnowflakeDBCursor(database_name=self.db_name) as cursor:
            chunk_size = 16384
            users_data_path = self._get_data_path().joinpath("faker_users.pkl").as_posix()
            users_df = pd.read_pickle(filepath_or_buffer=users_data_path)
            users_data: list[str] = [(row.user_id, row.username, row.email) for row in users_df.itertuples()]
            users_data_chunks = [users_data[i:i + chunk_size] for i in range(0, len(users_data), chunk_size)]
            for users_data_chunk in users_data_chunks:
                cursor.executemany(
                    command="INSERT INTO users (user_id, username, email) values (%s, %s, %s)",
                    seqparams=users_data_chunk)

    @exec_time_profiler
    def insert_orders_data(self):
        with SnowflakeDBCursor(database_name=self.db_name) as cursor:
            chunk_size = 16384
            orders_data_path = self._get_data_path().joinpath("faker_orders.pkl").as_posix()
            orders_df = pd.read_pickle(filepath_or_buffer=orders_data_path)
            orders_data: list[str] = [(row.order_id, row.user_id, row.product, row.amount, row.order_time) for row in orders_df.itertuples()]
            orders_data_chunks = [orders_data[i:i + chunk_size] for i in range(0, len(orders_data), chunk_size)]
            for orders_data_chunk in orders_data_chunks:
                cursor.executemany(
                    command="INSERT INTO orders (order_id, user_id, product, amount, order_time) values (%s, %s, %s, %s, %s)",
                    seqparams=orders_data_chunk)
