import sqlite3
import pandas as pd

print("1")
# Path to your SQLite database
sqlite_file = r"C:\Users\Omar\Documents\data-engineering-d25\olist.sqlite"
print("2")
# Connect to SQLite
conn = sqlite3.connect(sqlite_file)
print("3")
# Get a list of all tables
query = "SELECT name FROM sqlite_master WHERE type='table';"
tables = [row[0] for row in conn.execute(query).fetchall()]

# Export each table to a CSV file
for table in tables:
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    df.to_csv(f"{table}.csv", index=False)
    print(f"Exported {table} to {table}.csv")

# Close the connection
conn.close()
