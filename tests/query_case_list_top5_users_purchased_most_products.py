from clients.bqclient import BigQueryClient
from clients.sfclient import SnowflakeDBCursor


bq_client = BigQueryClient()


def sf_list_top5_users_purchased_most_products():
    with SnowflakeDBCursor() as cursor:
        query = """
            SELECT
                u.user_id,
                u.username,
                COUNT(o.order_id) as order_count
            FROM
                users u
            JOIN
                orders o
            ON
                u.user_id = o.user_id
            GROUP BY
                u.user_id, u.username
            ORDER BY
                username DESC, order_count DESC
            LIMIT 5;
        """
        cursor.execute(command=query)
        result = cursor.fetchall()
        assert len(result) > 0, "No results returned."


def bq_list_top5_users_purchased_most_products():
    query = f"""
        SELECT
            u.user_id,
            u.username,
            COUNT(o.order_id) as order_count
        FROM
            {bq_client.dataset_id}.{bq_client.users_table_id} u
        JOIN
            {bq_client.dataset_id}.{bq_client.orders_table_id} o
        ON
            u.user_id = o.user_id
        GROUP BY
            u.user_id, u.username
        ORDER BY
            username DESC, order_count DESC
        LIMIT 5;
    """
    bq_client.execute_query(query=query)
