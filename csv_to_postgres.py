import psycopg2
import pandas as pd

# Database connection
conn = psycopg2.connect(
    dbname="postgres",
    user="project-postgres",
    password="ready-de-25",
    host="34.67.200.93",  # Change if needed
    port="5432",
)
cursor = conn.cursor()


# Function to create a table dynamically
def create_table_from_csv(file_path, table_name):
    df = pd.read_csv(file_path)
    columns = []

    for col, dtype in zip(df.columns, df.dtypes):
        if "int" in str(dtype):
            pg_type = "INTEGER"
        elif "float" in str(dtype):
            pg_type = "FLOAT"
        elif "datetime" in str(dtype):
            pg_type = "TIMESTAMP"
        else:
            pg_type = "VARCHAR"
        columns.append(f"{col} {pg_type}")

    columns_query = ", ".join(columns)
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_query});"
    cursor.execute(create_table_query)
    conn.commit()
    print(f"Table {table_name} created successfully.")


# Function to populate the table
def populate_table_from_csv(file_path, table_name):
    df = pd.read_csv(file_path)
    cols = ", ".join(list(df.columns))
    values = ", ".join(["%s"] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({values})"
    i = 0
    for row in df.itertuples(index=False):
        i = i + 1
        cursor.execute(insert_query, row)
        print(i)

    conn.commit()
    print(f"Data inserted into {table_name} successfully.")


# Example usage for multiple CSV files
csv_files = {
    "leads_qualified": r"C:\Users\Omar\Downloads\project_dataset\leads_qualified.csv",
    "orders": r"C:\Users\Omar\Downloads\project_dataset\orders.csv",
    "products": r"C:\Users\Omar\Downloads\project_dataset\products.csv",
    "order_reviews": r"C:\Users\Omar\Downloads\project_dataset\order_reviews.csv",
    "order_items": r"C:\Users\Omar\Downloads\project_dataset\order_items.csv",
    "geolocation": r"C:\Users\Omar\Downloads\project_dataset\geolocation.csv",
    "customers": r"C:\Users\Omar\Downloads\project_dataset\customers.csv",
    "product_category_name_translation": r"C:\Users\Omar\Downloads\project_dataset\product_category_name_translation.csv",
}

for table_name, file_path in csv_files.items():
    create_table_from_csv(file_path, table_name)
    populate_table_from_csv(file_path, table_name)

cursor.close()
conn.close()
