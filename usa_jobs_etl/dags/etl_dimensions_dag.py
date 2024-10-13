from airflow import DAG
from airflow.operators.python import PythonOperator
from etl import extract_data, transform_data, load_to_redshift, request_api, db_services
from datetime import datetime
import os

DATA_PATH = os.path.dirname(os.path.realpath(__file__))

def etl_job_category():
    print(os.getenv('REDSHIFT_CONN_STRING'))
    data_path = extract_data.extract_data_job_categories('{{ ds_add(ds, -1) }}', DATA_PATH)
    transformed_data_parquet = transform_data.transform_data_category(data_path)
    load_to_redshift.load_categories_redshift(transformed_data_parquet)

def etl_position_types():
    print(os.getenv('REDSHIFT_CONN_STRING'))
    data_path = extract_data.extract_data_position_type('{{ ds_add(ds, -1) }}', DATA_PATH)
    transformed_data_parquet = transform_data.transform_data_position_types(data_path)
    load_to_redshift.load_position_types_redshift(transformed_data_parquet)

def etl_organizations():
    print(os.getenv('REDSHIFT_CONN_STRING'))
    data_path = extract_data.extract_data_organization('{{ ds_add(ds, -1) }}', DATA_PATH)
    transformed_data_parquet = transform_data.transform_data_organization(data_path)
    load_to_redshift.load_organization_redshift(transformed_data_parquet)
    
    
with DAG(
    'etl_domensions_dag',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline para extraccion, transformacion y carga de datos de Dimensiones',
    schedule_interval='@daily',
    start_date= datetime(2024, 10, 7),
    catchup=True,
) as dag:

    # Task 1: ETL data Organizations
    etl_organizations_task = PythonOperator(
        task_id='etl_organizations',
        python_callable=etl_organizations,
        op_kwargs={},
    )

    # Task 2: ETL data Job Category
    etl_job_category_task = PythonOperator(
        task_id='etl_job_category',
        python_callable=etl_job_category,
        op_kwargs={},
    )

    # Task 3: ETL data Position Type
    etl_position_types_task = PythonOperator(
        task_id='etl_position_types',
        python_callable=etl_position_types,
        op_kwargs={},
    )


[etl_job_category_task, etl_position_types_task, etl_organizations_task]