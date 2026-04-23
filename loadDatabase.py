import pymysql
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

print("🔍 Starting database import...")

# Basic connection details
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
folder_path = os.getenv('DATASET_PATH')

# --- Create Database if it doesn't exist ---
temp_conn = pymysql.connect(user=user, password=password, host=host)
cursor = temp_conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database}`")
cursor.close()
temp_conn.close()
print(f"✅ Database `{database}` ready.")
# -------------------------------------------

# SQLAlchemy engine using pymysql
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
print(f"📂 Found {len(csv_files)} CSV file(s) in {folder_path}\n")

for file in csv_files:
    file_path = os.path.join(folder_path, file)
    table_name = file.replace('.csv', '')

    print(f"🚀 Importing {file}...")
    try:
        with engine.begin() as connection:
            df = pd.read_csv(file_path)
            df.to_sql(
                table_name,
                con=connection,
                if_exists='replace',
                index=False,
                chunksize=10000
            )
        print(f"✅ {table_name} imported successfully. ({len(df):,} rows)\n")
    except Exception as e:
        print(f"❌ Failed to import {table_name}: {e}\n")
        continue

print("🏁 Process complete.")