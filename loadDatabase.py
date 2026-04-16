import mysql.connector
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

# Basic connection details
user = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
host = os.getenv('DB_HOST')
database = os.getenv('DB_NAME')
folder_path = os.getenv('DATASET_PATH')

# --- NEW BLOCK: Create Database if it doesn't exist ---
temp_conn = mysql.connector.connect(user=user, password=password, host=host)
cursor = temp_conn.cursor()
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
cursor.close()
temp_conn.close()
# ------------------------------------------------------

# Now continue with SQLAlchemy
engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')

for file in os.listdir(folder_path):
    if file.endswith('.csv'):
        file_path = os.path.join(folder_path, file)
        table_name = file.replace('.csv', '')
        
        print(f"🚀 Importing {file}...")
        
        try:
            # We use a context manager (with engine.begin()) to handle 
            # the transaction and auto-rollback if things go south.
            with engine.begin() as connection:
                # 1. Read the CSV
                df = pd.read_csv(file_path)
                
                # 2. Use chunksize to avoid memory/timeout issues
                # 10,000 is a good sweet spot for local MySQL
                df.to_sql(
                    table_name, 
                    con=connection, 
                    if_exists='replace', 
                    index=False, 
                    chunksize=10000
                )
            print(f"✅ {table_name} imported successfully.")
            
        except Exception as e:
            print(f"❌ Failed to import {table_name}: {e}")
            # The 'with engine.begin()' automatically rolls back here
            continue 

print("\n🏁 Process complete.")
