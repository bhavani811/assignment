from airflow import DAG
from airflow.contrib.operators.gcs_list_operator import GoogleCloudStorageListOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
from google.cloud import storage
import os

# Define a generic function to fetch the latest file from the buckets pupildata and pupilattendance
def fetch_latest_file(bucket_name, folder_path, **kwargs):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_path)
    latest_blob = max(blobs, key=lambda x: x.updated)
    latest_file_name = latest_blob.name
    print("Latest file name:", latest_file_name)
    return latest_file_name

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'pupil_pipeline',
    default_args=default_args,
    description='A DAG to load the latest file from GCS to BigQuery',
    schedule_interval='0 18 * * *',  # Cron expression for 6:00 PM every day
)

# Define the task to fetch the latest pupildata csv file from  pupildata bucket
fetch_latest_pupildata_task = PythonOperator(
    task_id='fetch_latest_pupildata_task',
    python_callable=fetch_latest_file,
    op_kwargs={'bucket_name': 'new_globe', 'folder_path': 'pupildata/'},
    provide_context=True,
    dag=dag,
)

# Define the task to fetch the latest pupilattendance csv file from pupilattendance bucket
fetch_latest_pupilattendance_task = PythonOperator(
    task_id='fetch_latest_pupilattendance_task',
    python_callable=fetch_latest_file,
    op_kwargs={'bucket_name': 'new_globe', 'folder_path': 'pupilattendance/'},
    provide_context=True,
    dag=dag,
)

# Define the task to load the latest pupildata file into BigQuery temp_pupil_data table in append mode
load_pupildata_bigquery = GCSToBigQueryOperator(
    task_id='load_pupildata_bigquery',
    bucket='new_globe',
    source_objects="{{ task_instance.xcom_pull(task_ids='fetch_latest_pupildata_task') }}",
    source_format='CSV',
    destination_project_dataset_table='new-globe-419310.de_assignment.temp_pupil_data', 
    write_disposition='WRITE_APPEND',  # Append data to the existing table
     dag=dag,
)

# Define the task to load the latest pupilattendance file into BigQuery temp_pupil_attdendance table in append mode
load_pupilattendance_bigquery = GCSToBigQueryOperator(
    task_id='load_pupilattendance_bigquery',
    bucket='new_globe',
    source_objects="{{ task_instance.xcom_pull(task_ids='fetch_latest_pupilattendance_task') }}",
    source_format='CSV',
    destination_project_dataset_table='new-globe-419310.de_assignment.temp_pupil_attdendance', 
    write_disposition='WRITE_APPEND',  # Append data to the existing table
     dag=dag,
)

 # task to call the stored procedure to load fact table
load_fact_table = BigQueryOperator(
        task_id='load_fact_table',
        sql="CALL `new-globe-419310.de_assignment.load_fct_table`();",
        use_legacy_sql=False,  # Use standard SQL
        dag=dag,
    )



  #  task to call the stored procedure to load dim tables
load_dim_tables = BigQueryOperator(
        task_id='load_dim_tables',
        sql="CALL `new-globe-419310.de_assignment.load_dim_tables`();",
        use_legacy_sql=False,  # Use standard SQL
        dag=dag,
    )

# Set up dependencies
[fetch_latest_pupildata_task >> load_pupildata_bigquery]>>load_dim_tables>>load_fact_table
[fetch_latest_pupilattendance_task >> load_pupilattendance_bigquery]>>load_dim_tables>>load_fact_table
