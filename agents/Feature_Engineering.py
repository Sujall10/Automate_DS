import pandas as pd
import numpy as np
import os
from DataBase.db_connection import get_engine


class FeatureEngineeringAgent:

    def __init__(self):
        self.engine = get_engine()

    def auto_feature_engineering(self, table_name):

        print(f"\n Auto Feature Engineering on: {table_name}\n")

        df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

        print("Original Shape:", df.shape)

        # ---------------- DETECT COLUMN TYPES ----------------
        num_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        cat_cols = df.select_dtypes(include=['object']).columns.tolist()

        # ---------------- DETECT DATETIME ----------------
        date_cols = []

        for col in df.columns:
            if "date" in col or "timestamp" in col:
                try:
                    df[col] = pd.to_datetime(df[col])
                    date_cols.append(col)
                except:
                    pass

        print("Numerical Columns:", num_cols)
        print("Categorical Columns:", cat_cols)
        print("Datetime Columns:", date_cols)

        # ---------------- DETECT ID COLUMNS ----------------
        id_cols = [col for col in df.columns if df[col].nunique() == len(df)]
        print("Detected ID Columns:", id_cols)

        # ---------------- DATETIME FEATURES ----------------
        print("\nCreating Datetime Features...")

        for col in date_cols:
            df[f"{col}_year"] = df[col].dt.year
            df[f"{col}_month"] = df[col].dt.month
            df[f"{col}_day"] = df[col].dt.day
            df[f"{col}_weekday"] = df[col].dt.weekday

        # ---------------- NUMERICAL FEATURES ----------------
        print("\nCreating Numerical Features...")

        for col in num_cols:

            if col not in id_cols:

                # log transform (safe)
                df[f"{col}_log"] = np.log1p(df[col].clip(lower=0))

                # binning
                df[f"{col}_bin"] = pd.qcut(df[col], q=5, duplicates='drop')

        # ---------------- CATEGORICAL FEATURES ----------------
        print("\nCreating Categorical Features...")

        for col in cat_cols:

            if col not in id_cols:

                # frequency encoding
                freq = df[col].value_counts()
                df[f"{col}_freq"] = df[col].map(freq)

        # ---------------- INTERACTION FEATURES ----------------
        print("\nCreating Interaction Features...")

        if len(num_cols) >= 2:
            for i in range(min(2, len(num_cols)-1)):
                col1 = num_cols[i]
                col2 = num_cols[i+1]

                if col1 not in id_cols and col2 not in id_cols:
                    df[f"{col1}_x_{col2}"] = df[col1] * df[col2]

        # ---------------- DROP ORIGINAL DATETIME ----------------
        df = df.drop(columns=date_cols, errors='ignore')

        # ---------------- SAVE ----------------
        os.makedirs("Datas/features", exist_ok=True)

        csv_path = f"Datas/features/{table_name}_features.csv"
        df.to_csv(csv_path, index=False)

        print(f"\n Features saved at: {csv_path}")

        # ---------------- SAVE TO DATABASE ----------------
        df.to_sql(
            f"{table_name}_features",
            self.engine,
            if_exists="replace",
            index=False
        )

        print(f" Saved to DB as {table_name}_features")

        print("\n Feature Engineering Completed!")
        print("New Shape:", df.shape)

        return df