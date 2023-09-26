# from utils.profilers import exec_time_profiler

from tests.query_case_list_orders_count_per_user import bq_list_orders_count_per_user, sf_list_orders_count_per_user
from tests.query_case_list_orders_count_per_user_and_yearmonth import bq_list_orders_count_per_user_and_yearmonth, sf_list_orders_count_per_user_and_yearmonth
from tests.query_case_list_top5_users_purchased_most_products import bq_list_top5_users_purchased_most_products, sf_list_top5_users_purchased_most_products
from tests.query_case_find_orders_amount_exceeding_999 import bq_find_orders_amount_exceeding_999, sf_find_orders_amount_exceeding_999


# Set up the table before running the tests
# @pytest.fixture(scope="module", autouse=True)
# def setup_module():
#     # conn = get_connection()
#     pass


# @pytest.fixture(scope="session", autouse=True)
# def setup_session():
#     yield
#     cleanup_test_database()


# Test Case: list_orders_count_per_user
def test_sf_list_orders_count_per_user(benchmark):
    benchmark(sf_list_orders_count_per_user)


def test_bq_list_orders_count_per_user(benchmark):
    benchmark(bq_list_orders_count_per_user)


# Test Case: list_orders_count_per_user_and_yearmonth
def test_sf_orders_count_per_user_and_yearmonth(benchmark):
    benchmark(sf_list_orders_count_per_user_and_yearmonth)


def test_bq_orders_count_per_user_and_yearmonth(benchmark):
    benchmark(bq_list_orders_count_per_user_and_yearmonth)


# Test Case: list_top5_users_purchased_most_products
def test_sf_list_top5_users_purchased_most_products(benchmark):
    benchmark(sf_list_top5_users_purchased_most_products)


def test_bq_list_top5_users_purchased_most_products(benchmark):
    benchmark(bq_list_top5_users_purchased_most_products)


# # Test Case: find_orders_amount_exceeding_999
def test_sf_find_orders_amount_exceeding_999(benchmark):
    benchmark(sf_find_orders_amount_exceeding_999)


def test_bq_find_orders_amount_exceeding_999(benchmark):
    benchmark(bq_find_orders_amount_exceeding_999)
