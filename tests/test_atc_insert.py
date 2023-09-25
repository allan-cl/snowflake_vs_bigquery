import random
import uuid
from datetime import datetime, timedelta

from tests.db_utils import TestDBCursor
from tests.profile_utils import exec_time_profiler


# Set up the table before running the tests
# @pytest.fixture(scope="module", autouse=True)
# def setup_module():
#     # conn = get_connection()
#     pass


# @pytest.fixture(scope="session", autouse=True)
# def setup_session():
#     yield
#     cleanup_test_database()

@exec_time_profiler
def test_insert_data():
    user_count = 10_000
    order_count_per_user = 100
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
            for _ in range(order_count_per_user):
                orders_data.append((str(uuid.uuid4()),
                                    user_id,
                                    f"product_{random.randint(a=1, b=100)}",
                                    random.randint(a=1, b=1000),
                                    datetime.now() - timedelta(days=random.randint(a=1, b=365))))
        orders_data_chunks = [orders_data[i:i + chunk_size] for i in range(0, len(orders_data), chunk_size)]
        for orders_data_chunk in orders_data_chunks:
            cursor.executemany(
                command="INSERT INTO orders (order_id, user_id, product, amount, order_time) values (%s, %s, %s, %s, %s)",
                seqparams=orders_data_chunk)
