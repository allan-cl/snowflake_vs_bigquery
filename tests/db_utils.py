import os
import random
import uuid
from dotenv import load_dotenv
from datetime import datetime, timedelta

import snowflake.connector

from tests.profile_utils import exec_time_profiler


load_dotenv()
SF_DATABASE_NAME = "TEST_DB"


class TestDBCursor:
    def __init__(self):
        self.conn = None
        self.cursor = None

    def __enter__(self):
        # Code to acquire the resource.
        self.conn = snowflake.connector.connect(
            user=os.environ.get("SF_USERNAME"),
            password=os.environ.get("SF_PASSWORD"),
            account=os.environ.get("SF_ACCOUNT"),
            database=SF_DATABASE_NAME)
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback):
        # Code to release the resource.
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


def get_sf_connection():
    conn = snowflake.connector.connect(
        user=os.environ.get("SF_USERNAME"),
        password=os.environ.get("SF_PASSWORD"),
        account=os.environ.get("SF_ACCOUNT")
    )
    return conn


def get_sf_test_connection():
    conn = snowflake.connector.connect(
        user=os.environ.get("SF_USERNAME"),
        password=os.environ.get("SF_PASSWORD"),
        account=os.environ.get("SF_ACCOUNT"),
        database=SF_DATABASE_NAME
    )
    return conn


def setup_test_database():
    with TestDBCursor() as cursor:
        cursor.execute(f"CREATE OR REPLACE DATABASE {SF_DATABASE_NAME}")
        cursor.execute("""
            CREATE OR REPLACE TABLE users (
                user_id VARCHAR(36) PRIMARY KEY,
                username VARCHAR(100),
                email VARCHAR(100)
            )
        """)
        cursor.execute("""
            CREATE OR REPLACE TABLE orders (
                order_id VARCHAR(36) PRIMARY KEY,
                user_id VARCHAR(36) REFERENCES users(user_id),
                product VARCHAR(200),
                amount INT,
                order_time TIMESTAMP
            )
        """)


def cleanup_test_database():
    with TestDBCursor() as cursor:
        cursor.execute(f"DROP DATABASE IF EXISTS {SF_DATABASE_NAME}")


@exec_time_profiler
def insert_users_and_orders_data(user_count):
    """
    create <user_count> users data, each user has [0-100] orders
    """
    with TestDBCursor() as cursor:
        chunk_size = 16384

        users_data: list[str] = [(f'{i+1:0>36}', f'user{i+1:0>9}', f'user{i+1:0>9}@example.com') for i in range(user_count)]
        users_data_chunks = [users_data[i:i + chunk_size] for i in range(0, len(users_data), chunk_size)]
        cursor.executemany(
            command="INSERT INTO users (user_id, username, email) values (%s, %s, %s)",
            seqparams=users_data)

        orders_data = []
        for idx in range(user_count):
            user_id = f'{idx+1:0>36}'
            for _ in range(random.randint(a=0, b=100)):
                orders_data.append((str(uuid.uuid4()),
                                    user_id,
                                    f"product_{random.randint(a=1, b=100)}",
                                    random.randint(a=1, b=1000),
                                    datetime.now() - timedelta(days=random.randint(a=1, b=100))))
        orders_data_chunks = [orders_data[i:i + chunk_size] for i in range(0, len(orders_data), chunk_size)]
        for orders_data_chunk in orders_data_chunks:
            cursor.executemany(
                command="INSERT INTO orders (order_id, user_id, product, amount, order_time) values (%s, %s, %s, %s, %s)",
                seqparams=orders_data_chunk)
