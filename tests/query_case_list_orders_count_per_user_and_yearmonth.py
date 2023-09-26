from clients.bqclient import BigQueryClient
from clients.sfclient import SnowflakeDBCursor


bq_client = BigQueryClient()


def bq_list_orders_count_per_user_and_yearmonth():
    query: str = f"""
        SELECT DISTINCT
        FORMAT_TIMESTAMP('%Y-%m', order_time) AS year_month
        FROM {bq_client.dataset_id}.{bq_client.orders_table_id}
        ORDER BY year_month;
    """
    result = bq_client.execute_query(query=query)
    year_months = [row.values()[0] for row in result]

    pivot_query = f"""
        SELECT u.user_id,
            {', '.join([f"IFNULL(SUM(IF(FORMAT_TIMESTAMP('%Y-%m', o.order_time) = '{ym}', 1, 0)), 0) AS `{ym}`" for ym in year_months])}
        FROM {bq_client.dataset_id}.{bq_client.users_table_id} u
        LEFT JOIN {bq_client.dataset_id}.{bq_client.orders_table_id} o
        ON u.user_id = o.user_id
        GROUP BY u.user_id
        ORDER BY u.user_id;
    """
    result = bq_client.execute_query(query=pivot_query)
    assert result.total_rows == 10000


def sf_list_orders_count_per_user_and_yearmonth():
    with SnowflakeDBCursor() as cursor:
        cursor.execute("""
            SELECT DISTINCT TO_VARCHAR(order_time, 'YYYY-MM') AS year_month
            FROM orders
            ORDER BY year_month
        """)
        year_months = [row[0] for row in cursor.fetchall()]

        # Construct the PIVOT query dynamically
        pivot_query = f"""
            SELECT * FROM (
                SELECT u.user_id,
                       TO_VARCHAR(o.order_time, 'YYYY-MM') AS year_month
                FROM users u
                LEFT JOIN orders o
                ON u.user_id = o.user_id
            )
            PIVOT (
                COUNT(year_month)
                FOR year_month IN ({', '.join(["'" + ym + "'" for ym in year_months])})
            )
            ORDER BY user_id;
        """
        # Execute the PIVOT query
        cursor.execute(command=pivot_query)
        results = cursor.fetchall()
        assert len(results) == 10000
