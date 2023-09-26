from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from utils.profilers import exec_time_profiler


class BigQueryClient:
    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(
            Path(__file__).resolve().parent.joinpath("bq_conf.json").as_posix(),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        self.client: bigquery.Client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        self.project_id = credentials.project_id
        self.dataset_id = "test_db"
        self.users_table_id = "users"
        self.orders_table_id = "orders"

    def list_datasets(self):
        datasets = list(self.client.list_datasets())
        if datasets:
            print("Datasets in project {}:".format(self.client.project))
            for dataset in datasets:
                print(dataset.dataset_id)
        else:
            print("No datasets in project {}.".format(self.client.project))

    def create_dataset(self):
        # delete dataset if exists
        self.client.delete_dataset(dataset=f"{self.project_id}.{self.dataset_id}", delete_contents=True, not_found_ok=True)
        # create a new dataset
        dataset_ref = self.client.dataset(dataset_id=self.dataset_id, project=self.project_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = self.client.create_dataset(dataset=dataset)
        print(f"Created dataset {dataset.project}.{dataset.dataset_id}")

    def create_users_table(self):
        table_id: str = f"{self.dataset_id}.{self.users_table_id}"
        try:
            # delete users table if exists
            self.client.query(query=f"DROP TABLE IF EXISTS {table_id}", project=self.project_id)
            # create a new users table
            create_users_table: str = f"""
            CREATE OR REPLACE TABLE {table_id} (
                user_id STRING NOT NULL,
                username STRING,
                email STRING,
                PRIMARY KEY (user_id) NOT ENFORCED
            )
            """
            result = self.client.query(query=create_users_table, project=self.project_id)
            print(f"Created data table {table_id} {result.result()}")
        except Exception as e:
            print(f"Error creating orders table {table_id}: {e}")

    def create_orders_table(self):
        table_id: str = f"{self.dataset_id}.{self.orders_table_id}"
        try:
            # detele orders table if exists
            self.client.query(query=f"DROP TABLE IF EXISTS {table_id}", project=self.project_id)
            # create a new orders table
            create_orders_table: str = f"""
            CREATE OR REPLACE TABLE {table_id} (
                order_id STRING NOT NULL,
                user_id STRING,
                product STRING,
                amount INT64,
                order_time TIMESTAMP,
                PRIMARY KEY (order_id) NOT ENFORCED,
                FOREIGN KEY (user_id) REFERENCES {self.dataset_id}.{self.users_table_id} (user_id) NOT ENFORCED
            );
            """
            result = self.client.query(query=create_orders_table, project=self.project_id)
            print(f"Created orders table {table_id} {result.result()}")
        except Exception as e:
            print(f"Error creating orders table {table_id}: {e}")

    def execute_query(self, query):
        return self.client.query(query=query, project=self.project_id).result()

    # def delete_table(self, dataset_id, table_id):
    #     table_ref = self.client.dataset(dataset_id).table(table_id)
    #     self.client.delete_table(table_ref)
    #     print(f"Deleted table {table_ref.project}.{table_ref.dataset_id}.{table_ref.table_id}")

    # def delete_dataset(self, dataset_id):
    #     dataset_ref = self.client.dataset(dataset_id)
    #     self.client.delete_dataset(dataset_ref, delete_contents=True, not_found_ok=True)
    #     print(f"Deleted dataset {dataset_ref.project}.{dataset_ref.dataset_id}")

    # @exec_time_profiler
    def insert_users_data(self):
        table_ref = self.client.dataset(dataset_id=self.dataset_id, project=self.project_id).table(table_id=self.users_table_id)
        users_table = self.client.get_table(table=table_ref)

        users_data_path = self._get_data_path().joinpath("faker_users.pkl").as_posix()
        users_df = pd.read_pickle(filepath_or_buffer=users_data_path)
        errors = self.client.insert_rows_from_dataframe(table=users_table, dataframe=users_df, chunk_size=5000)
        print("errors while inserting uses data: {}".format(errors))

    # @exec_time_profiler
    def insert_orders_data(self):
        table_ref = self.client.dataset(dataset_id=self.dataset_id, project=self.project_id).table(table_id=self.orders_table_id)
        orders_table = self.client.get_table(table=table_ref)

        orders_data_path = self._get_data_path().joinpath("faker_orders.pkl").as_posix()
        orders_df = pd.read_pickle(filepath_or_buffer=orders_data_path)
        errors = self.client.insert_rows_from_dataframe(table=orders_table, dataframe=orders_df, chunk_size=5000)
        print("errors while inserting orders data: {}".format(errors))

    def truncate_table(self, table_id: str):
        try:
            truncate_sql = f"TRUNCATE TABLE `{self.dataset_id}.{table_id}`"
            self.client.query(query=truncate_sql, project=self.project_id).result()
            print(f"Table {self.dataset_id}.{table_id} truncated successfully.")
        except Exception as e:
            print(f"Error truncating table {self.dataset_id}.{table_id}: {e}")

    def _get_data_path(self) -> Path:
        return Path(__file__).resolve().parent.parent.joinpath("data")


# if __name__ == "__main__":
#     bq_client = BigQueryClient()

#     # bq_client.create_orders_table()
#     # bq_client.insert_users_data()
#     bq_client.insert_orders_data()
