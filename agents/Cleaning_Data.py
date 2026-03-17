import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import os
import joblib
from DataBase.db_connection import get_engine


class CleaningAgent:

    def __init__(self):
        self.engine = get_engine()

    def clean_data(self, file_path):

        print("\n Starting Data Cleaning...\n")

        df = pd.read_csv(file_path)

        print("Original Shape:", df.shape)

        # ---------------- REMOVE DUPLICATES ----------------
        duplicates = df.duplicated().sum()

        if duplicates > 0:
            print(f" Removing {duplicates} duplicate rows")
            df = df.drop_duplicates()
        else:
            print("No duplicates found")

        # ---------------- HANDLE MISSING VALUES ----------------
        print("\nHandling Missing Values...")

        for col in df.columns:

            if df[col].isnull().sum() > 0:

                if df[col].dtype in ['int64', 'float64']:
                    df[col] = df[col].fillna(df[col].mean())
                    print(f"{col} → filled with mean")

                else:
                    mode_val = df[col].mode()
                    df[col] = df[col].fillna(mode_val[0] if not mode_val.empty else "Unknown")
                    print(f"{col} → filled with mode")

        # ---------------- DETECT ID COLUMNS ----------------
        print("\nDetecting ID Columns...")

        id_cols = []

        for col in df.columns:
            if df[col].nunique() == len(df):
                id_cols.append(col)

        print("Detected ID columns:", id_cols)

        # ---------------- OUTLIER HANDLING ----------------
        print("\nHandling Outliers...")

        num_cols = df.select_dtypes(include=['int64', 'float64']).columns
        num_cols = [col for col in num_cols if col not in id_cols]

        for col in num_cols:

            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1

            lower = Q1 - 1.5 * IQR
            upper = Q3 + 1.5 * IQR

            outliers = ((df[col] < lower) | (df[col] > upper)).sum()

            if outliers > 0:
                print(f"{col}: {outliers} outliers capped")

                df[col] = df[col].clip(lower, upper)

        # ---------------- FEATURE SCALING ----------------
        print("\nScaling Features...")

        scaler = StandardScaler()

        df[num_cols] = scaler.fit_transform(df[num_cols])

        # ---------------- SAVE SCALER ----------------
        os.makedirs("models", exist_ok=True)
        joblib.dump(scaler, "models/scaler.pkl")
        print("Scaler saved at models/scaler.pkl")

        # ---------------- SAVE CSV ----------------
        os.makedirs("Datas/processed", exist_ok=True)

        csv_path = "Datas/processed/cleaned_data.csv"
        df.to_csv(csv_path, index=False)

        print(f"\n CSV saved at: {csv_path}")

        # ---------------- SAVE TO DATABASE ----------------
        print("\n Saving cleaned data to PostgreSQL...")

        df.to_sql(
            "cleaned_customer_features",
            self.engine,
            if_exists="replace",
            index=False
        )

        print(" Saved to database as 'cleaned_customer_features' table!")

        # ---------------- FINAL OUTPUT ----------------
        print("\n Cleaning Completed!")
        print("Final Shape:", df.shape)

        return df