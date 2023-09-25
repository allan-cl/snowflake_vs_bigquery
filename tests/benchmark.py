import pytest

# from tests.db_utils import get_sf_connection
# from db_utils, setup_test_table, insert_data


# Set up the table before running the tests
@pytest.fixture(scope="module", autouse=True)
def setup_module():
    # conn = get_connection()
    pass


# def test_create_conn_bm(benchmark) -> None:
#     benchmark(get_sf_connection)


# def test_insert_benchmark(benchmark):
#     benchmark(insert_data, num_records=100)

# def test_select_benchmark(benchmark):
#     conn = get_connection()
#     cursor = conn.cursor()

#     # Benchmark the SELECT statement
#     benchmark(cursor.execute, "SELECT * FROM pytest_benchmark")

#     cursor.close()
#     conn.close()
