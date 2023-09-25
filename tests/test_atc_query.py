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
def test_orders_count_per_user():
    with TestDBCursor() as cursor:
        query = """
            SELECT
                u.user_id,
                u.username,
                COUNT(o.order_id) AS total_orders
            FROM
                users u
            LEFT JOIN
                orders o ON u.user_id = o.user_id
            GROUP BY
                u.user_id, u.username
            ORDER BY
                total_orders DESC;
        """
        cursor.execute(query)
        result = cursor.fetchall()
        assert len(result) > 0, "No results returned."


@exec_time_profiler
def test_orders_count_per_user_and_yearmonth():
    with TestDBCursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT EXTRACT(YEAR FROM order_time) || '-' || LPAD(EXTRACT(MONTH FROM order_time), 2, '0') AS year_month
            FROM orders
            ORDER BY year_month
        """)
        year_months = [row[0] for row in cursor.fetchall()]

        # Construct the PIVOT query dynamically
        pivot_query = f"""
            SELECT * FROM (
                SELECT user_id,
                    EXTRACT(YEAR FROM order_time) || '-' || LPAD(EXTRACT(MONTH FROM order_time), 2, '0') AS year_month
                FROM orders
            )
            PIVOT (
                COUNT(year_month)
                FOR year_month IN ({', '.join(["'" + ym + "'" for ym in year_months])})
            )
            ORDER BY user_id;
        """

        # Execute the PIVOT query
        cursor.execute(pivot_query)
        results = cursor.fetchall()
        assert len(results) > 0, "No results returned."
