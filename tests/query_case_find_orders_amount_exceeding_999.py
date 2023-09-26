from clients.bqclient import BigQueryClient
from clients.sfclient import SnowflakeDBCursor


bq_client = BigQueryClient()


def sf_find_orders_amount_exceeding_999():
    with SnowflakeDBCursor() as cursor:
        query = """
            SELECT
                o.order_id,
                u.username,
                o.product,
                o.amount,
                o.order_time
            FROM
                orders o
            JOIN
                users u
            ON
                o.user_id = u.user_id
            WHERE
                o.amount > 999
            ORDER BY
                o.order_time DESC;
        """
        cursor.execute(command=query)
        result = cursor.fetchall()
        assert len(result) > 0, "No results returned."


def bq_find_orders_amount_exceeding_999():
    query = f"""
        SELECT
            o.order_id,
            u.username,
            o.product,
            o.amount,
            o.order_time
        FROM
            {bq_client.dataset_id}.{bq_client.orders_table_id} o
        JOIN
            {bq_client.dataset_id}.{bq_client.users_table_id} u
        ON
            o.user_id = u.user_id
        WHERE
            o.amount > 999
        ORDER BY
            o.order_time DESC;
    """
    bq_client.execute_query(query=query)
