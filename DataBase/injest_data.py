import pandas as pd
import os
from db_connection import get_engine

# folder where dataset exists
DATA_PATH = "Datas/Brazilian E-Commerce Public Dataset"

engine = get_engine()

datasets = {

"customers":"olist_customers_dataset.csv",
"orders":"olist_orders_dataset.csv",
"order_items":"olist_order_items_dataset.csv",
"order_payments":"olist_order_payments_dataset.csv",
"order_reviews":"olist_order_reviews_dataset.csv",
"products":"olist_products_dataset.csv",
"sellers":"olist_sellers_dataset.csv",
"geolocation":"olist_geolocation_dataset.csv",
"category_translation":"product_category_name_translation.csv"

}

def ingest():

    for table,file in datasets.items():

        path = os.path.join(DATA_PATH,file)

        print(f"\nLoading {file}")

        df = pd.read_csv(path)

        print(f"Inserting into {table}")

        df.to_sql(
            table,
            engine,
            if_exists="append",
            index=False,
            chunksize=5000
        )

        print(f"{table} completed")

if __name__ == "__main__":

    ingest()