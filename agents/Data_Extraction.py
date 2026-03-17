import pandas as pd
from DataBase.db_connection import get_engine


class DataExtractionAgent:

    def __init__(self):
        self.engine = get_engine()

    def get_table(self, table_name):

        print(f"Extracting {table_name}...")

        query = f"SELECT * FROM {table_name}"

        df = pd.read_sql(query, self.engine)

        print(f"{table_name} loaded: {df.shape}")

        return df


    def get_all_tables(self):

        tables = [
            "customers",
            "orders",
            "order_items",
            "order_payments",
            "order_reviews",
            "products",
            "sellers",
            "geolocation"
        ]

        data = {}

        for table in tables:

            data[table] = self.get_table(table)

        return data