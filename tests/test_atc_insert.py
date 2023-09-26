import pytest

from utils.profilers import exec_time_profiler
from clients.sfclient import SnowflakeClient
from clients.bqclient import BigQueryClient

sf_client = SnowflakeClient()
bq_client = BigQueryClient()


# Set up the table before running the tests
# @pytest.fixture(scope="module", autouse=True)
# def setup_module():
#     sf_client.create_database()
#     sf_client.create_users_table()

#     bq_client.create_dataset()
#     bq_client.create_users_table()


# @pytest.fixture(scope="session", autouse=True)
# def setup_session():
#     yield
#     cleanup_test_database()


def test_sf_insert_users_data():
    sf_client.insert_users_data()


def test_sf_insert_orders_data():
    sf_client.insert_orders_data()


def test_bq_insert_users_data():
    bq_client.insert_users_data()


def test_bq_insert_orders_data():
    bq_client.insert_orders_data()
