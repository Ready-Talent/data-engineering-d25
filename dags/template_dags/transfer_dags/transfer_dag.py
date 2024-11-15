from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import (
    PostgresToGCSOperator,
)

dag = DAG(
    dag_id="e-comerce_transfers",
    description="transfer e-commerce tables from postgres to BQ",
    schedule_interval=None,
    start_date=datetime(2024, 4, 20),
    catchup=False,
)

start = EmptyOperator(task_id="start", dag=dag)

end = EmptyOperator(task_id="end", dag=dag)

GCS_BUCKET = "postgres-to-gcs"

tables = ["product", "customer", "order"]

for table in tables:

    postgres_to_gcs = PostgresToGCSOperator(
        task_id=f"postgres_to_gcs_{table}",
        postgres_conn_id="postgres_connection",
        bucket=GCS_BUCKET,
        sql=f"SELECT * FROM src01.{table}",
        filename=f"omar_thabet/{table}.csv",
        export_format="csv",
        gzip=False,
        use_server_side_cursor=False,
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id=f"gcs_to_bq_{table}",
        bucket=GCS_BUCKET,
        source_objects=[f"omar_thabet/{table}.csv"],
        source_format="CSV",
        destination_project_dataset_table=f"landing.{table}",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
        ignore_unknown_values=True,
        field_delimiter=",",
        dag=dag,
        skip_leading_rows=1,
        max_bad_records=1000000,
    )
    start >> postgres_to_gcs >> gcs_to_bq >> end


# # customer table transfers

# postgres_to_gcs_customer = PostgresToGCSOperator(
#     task_id="customer_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.customer",
#     filename="omar_thabet/customer.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_customer = GCSToBigQueryOperator(
#     task_id="customer_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.customer",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     skip_leading_rows=1,
#     max_bad_records=1000000,
# )

# # address table transfers

# postgres_to_gcs_address = PostgresToGCSOperator(
#     task_id="address_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.address",
#     filename="omar_thabet/address.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_address = GCSToBigQueryOperator(
#     task_id="address_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.address",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     max_bad_records=1000000,
#     skip_leading_rows=1,
# )

# # channel table transfers

# postgres_to_gcs_channel = PostgresToGCSOperator(
#     task_id="channel_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.channel",
#     filename="omar_thabet/channel.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_channel = GCSToBigQueryOperator(
#     task_id="channel_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.channel",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     max_bad_records=1000000,
#     skip_leading_rows=1,
# )

# # order table transfers

# postgres_to_gcs_order = PostgresToGCSOperator(
#     task_id="order_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.order",
#     filename="omar_thabet/order.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_order = GCSToBigQueryOperator(
#     task_id="order_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.order",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     max_bad_records=1000000,
#     skip_leading_rows=1,
# )

# # order_detail table transfers

# postgres_to_gcs_order_detail = PostgresToGCSOperator(
#     task_id="order_detail_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.order_detail",
#     filename="omar_thabet/order_detail.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_order_detail = GCSToBigQueryOperator(
#     task_id="order_detail_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.order_detail",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     max_bad_records=1000000,
#     skip_leading_rows=1,
# )

# # payment table transfers

# postgres_to_gcs_payment = PostgresToGCSOperator(
#     task_id="payment_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.payment",
#     filename="omar_thabet/payment.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_payment = GCSToBigQueryOperator(
#     task_id="payment_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.payment",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     max_bad_records=1000000,
#     skip_leading_rows=1,
# )

# # payment table transfers

# postgres_to_gcs_payment_type = PostgresToGCSOperator(
#     task_id="payment_type_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.payment_type",
#     filename="omar_thabet/payment_type.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_payment_type = GCSToBigQueryOperator(
#     task_id="payment_type_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.payment_type",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     max_bad_records=1000000,
#     skip_leading_rows=1,
# )

# # product table transfers

# postgres_to_gcs_product = PostgresToGCSOperator(
#     task_id="product_postgres_to_gcs",
#     postgres_conn_id="postgres_connection",
#     bucket=GCS_BUCKET,
#     sql="SELECT * FROM src01.product",
#     filename="omar_thabet/product.csv",
#     export_format="csv",
#     gzip=False,
#     use_server_side_cursor=False,
# )


# gcs_to_bq_product = GCSToBigQueryOperator(
#     task_id="product_gcs_to_bq",
#     bucket=GCS_BUCKET,
#     source_objects=["omar_thabet/*.csv"],
#     source_format="CSV",
#     destination_project_dataset_table="landing.product",
#     write_disposition="WRITE_TRUNCATE",
#     create_disposition="CREATE_IF_NEEDED",
#     autodetect=True,
#     ignore_unknown_values=True,
#     field_delimiter=",",
#     dag=dag,
#     max_bad_records=1000000,
#     skip_leading_rows=1,
# )


# (
#     start
#     >> [
#         postgres_to_gcs_customer,
#         postgres_to_gcs_payment,
#         postgres_to_gcs_address,
#         postgres_to_gcs_channel,
#         postgres_to_gcs_order,
#         postgres_to_gcs_order_detail,
#         postgres_to_gcs_payment_type,
#         postgres_to_gcs_product,
#     ]
#     >> dummy
# )

# (
#     dummy
#     >> [
#         gcs_to_bq_customer,
#         gcs_to_bq_payment,
#         gcs_to_bq_address,
#         gcs_to_bq_order,
#         gcs_to_bq_channel,
#         gcs_to_bq_order_detail,
#         gcs_to_bq_payment_type,
#         gcs_to_bq_product,
#     ]
#     >> end
# )
