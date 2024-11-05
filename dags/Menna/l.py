from pathlib import Path

json_file_path = Path(r'E:\ready\airflow_task\data-engineering-d25\dags\Menna\customer_table.json')
print(json_file_path)

parent_directory = json_file_path.parent
print(parent_directory)
schema_file_path = parent_directory / json_file_path.name
print(schema_file_path)