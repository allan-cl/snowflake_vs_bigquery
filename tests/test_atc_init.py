
from tests.db_utils import TestDBCursor, get_sf_connection, setup_test_database


# Set up the table before running the tests
# @pytest.fixture(scope="module", autouse=True)
# def setup_module():
#     # conn = get_connection()
#     pass


# @pytest.fixture(scope="session", autouse=True)
# def setup_session():
#     yield
#     cleanup_test_database()


def test_create_conn():
    conn = get_sf_connection()
    assert conn is not None


def test_setup_database():
    setup_test_database()


def test_valid_users_table():
    with TestDBCursor() as cursor:
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


def test_valid_orders_table():
    with TestDBCursor() as cursor:
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
