from clients.bqclient import BigQueryClient
from clients.sfclient import SnowflakeDBCursor


bq_client = BigQueryClient()


def bq_list_orders_count_per_user():
    query = f"""
        SELECT
            u.user_id,
            u.username,
            COUNT(o.order_id) AS total_orders
        FROM
            {bq_client.dataset_id}.{bq_client.users_table_id} u
        LEFT JOIN
            {bq_client.dataset_id}.{bq_client.orders_table_id} o ON u.user_id = o.user_id
        GROUP BY
            u.user_id, u.username
        ORDER BY
            total_orders DESC;
    """
    bq_client.execute_query(query=query)


def sf_list_orders_count_per_user():
    with SnowflakeDBCursor() as cursor:
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
        cursor.execute(command=query)
        result = cursor.fetchall()
        assert len(result) > 0, "No results returned."
