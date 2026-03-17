import pandas as pd
from DataBase.db_connection import get_engine


class EDAAgent:

    def __init__(self):
        self.engine = get_engine()

    def analyze_table(self, table_name):

        print(f"\n Analyzing {table_name}...\n")

        df = pd.read_sql(f"SELECT * FROM {table_name}", self.engine)

        # ---------------- BASIC INFO ----------------
        print(f"Shape: {df.shape}")

        num_cols = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        cat_cols = df.select_dtypes(include=['object']).columns.tolist()

        print(f"\nNumerical Columns: {num_cols}")
        print(f"Categorical Columns: {cat_cols}")

        # ---------------- MISSING VALUES ----------------
        print("\n Missing Value Analysis:")

        missing = df.isnull().sum()
        missing_percent = (missing / len(df)) * 100

        for col in df.columns:
            if missing[col] > 0:
                print(f"{col}: {missing[col]} ({missing_percent[col]:.2f}%)")

        # ---------------- DUPLICATES ----------------
        print("\n Duplicate Analysis:")

        duplicates = df.duplicated().sum()

        if duplicates > 0:
            print(f" {duplicates} duplicate rows found")
        else:
            print("No duplicate rows found")

        # ---------------- SMART INSIGHTS ----------------
        print("\n Smart Insights:")

        for col in df.columns:

            # High missing values
            if missing_percent[col] > 40:
                print(f" {col} has >40% missing → consider dropping")

            # Low variance (constant column)
            if df[col].nunique() == 1:
                print(f" {col} has only one unique value → useless feature")

            # High cardinality categorical
            if col in cat_cols and df[col].nunique() > 50 and "date" not in col:
                print(f" {col} has high cardinality → encoding needed")

        # ---------------- CORRELATION ----------------
        print("\n Correlation Analysis:")

        if len(num_cols) > 1:
            corr = df[num_cols].corr()

            for col in corr.columns:
                for row in corr.index:
                    if col != row and abs(corr.loc[col, row]) > 0.8:
                        print(f" High correlation: {col} ↔ {row} ({corr.loc[col, row]:.2f})")
        else:
            print("Not enough numerical columns for correlation")

        # ---------------- COVARIANCE ----------------
        print("\n Covariance Analysis:")

        if len(num_cols) > 1:
            cov = df[num_cols].cov()

            print(cov.head())
        else:
            print("Not enough numerical columns for covariance")

        # ---------------- TARGET SUGGESTION ----------------
        print("\n Target Suggestions:")

        for col in num_cols:

            unique_vals = df[col].nunique()

            if unique_vals < 10:
                print(f" {col} could be classification target")

            elif unique_vals > 50:
                print(f" {col} could be regression target")

        print("\n Analysis Completed\n")

        return df


    def analyze_all_tables(self):

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

        for table in tables:
            self.analyze_table(table)