from clients.sfclient import SnowflakeClient, SnowflakeDBCursor
from clients.bqclient import BigQueryClient

sf_client = SnowflakeClient()
bq_client = BigQueryClient()

# Set up the table before running the tests
# @pytest.fixture(scope="module", autouse=True)
# def setup_module():
#     # conn = get_connection()
#     pass


# @pytest.fixture(scope="session", autouse=True)
# def setup_session():
#     yield
#     cleanup_test_database()


def test_create_sf_database(benchmark):
    benchmark(sf_client.create_database)


def test_create_sf_users_table(benchmark):
    benchmark(sf_client.create_users_table)


def test_create_sf_orders_table(benchmark):
    benchmark(sf_client.create_orders_table)


def test_create_bq_dataset(benchmark):
    benchmark(bq_client.create_dataset)


def test_create_bq_users_table(benchmark):
    benchmark(bq_client.create_users_table)


def test_create_bq_orders_table(benchmark):
    benchmark(bq_client.create_orders_table)


def test_valid_sf_users_table():
    with SnowflakeDBCursor() as cursor:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'USERS'
            AND COLUMN_NAME = 'USER_ID';
        """
        cursor.execute(query)
        result = cursor.fetchone()
        assert result is not None, "Table or column does not exist."
        assert result[1] == "TEXT", f"Expected column type to be TEXT, but got {result[1]}"


def test_valid_sf_orders_table():
    with SnowflakeDBCursor() as cursor:
        query = """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = 'ORDERS'
            AND COLUMN_NAME = 'AMOUNT';
        """
        cursor.execute(query)
        result = cursor.fetchone()
        assert result is not None, "Table or column does not exist."
        assert result[1] == "NUMBER", f"Expected column type to be NUMBER, but got {result[1]}"
