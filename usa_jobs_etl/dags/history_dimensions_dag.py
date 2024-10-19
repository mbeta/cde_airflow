from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.etl import extract_data, transform_data, load_to_redshift
from datetime import datetime
import os


DATA_TEMP = os.getenv('DATA_TEMP')

def history_job_category(fecha_contexto):
    print(f'{fecha_contexto}: Se inicia carga HISTORICA de Dimension Job Category')
    data_path = extract_data.extract_data_job_categories('', DATA_TEMP)
    print(data_path)
    if(data_path):
        transformed_data_parquet = transform_data.transform_data_category(data_path)
        load_to_redshift.load_categories_redshift(transformed_data_parquet)

def history_position_types(fecha_contexto):
    print(f'{fecha_contexto}: Se inicia carga HISTORICA de Dimension Position Types')
    data_path = extract_data.extract_data_position_type('', DATA_TEMP)
    print(data_path)
    if(data_path):
        transformed_data_parquet = transform_data.transform_data_position_types(data_path)
        load_to_redshift.load_position_types_redshift(transformed_data_parquet)
    
    

def history_organizations(fecha_contexto):
    print(f'{fecha_contexto}: Se inicia carga HISTORICA de Dimension Organizations')
    data_path = extract_data.extract_data_organization('', DATA_TEMP)
    print(data_path)
    if(data_path):
        transformed_data_parquet = transform_data.transform_data_organization(data_path)
        load_to_redshift.load_organization_redshift(transformed_data_parquet)
    
    
with DAG(
    'history_dimensions_dag',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline para extraccion, transformacion y carga de datos de Dimensiones HISTORICO',
    schedule_interval=None,
    start_date= datetime(2024, 10, 7),
    catchup=False,
) as dag:

    # Task 1: ETL data Organizations
    history_organizations_task = PythonOperator(
        task_id='history_organizations',
        python_callable=history_organizations,
        op_kwargs={'fecha_contexto':'{{ ds }}'},
    )

    # Task 2: ETL data Job Category
    history_job_category_task = PythonOperator(
        task_id='history_job_category',
        python_callable=history_job_category,
        op_kwargs={'fecha_contexto':'{{ ds }}'},
    )

    # Task 3: ETL data Position Type
    history_position_types_task = PythonOperator(
        task_id='history_position_types',
        python_callable=history_position_types,
        op_kwargs={'fecha_contexto':'{{ ds }}'},
    )

#Ejecucion en paralelo
[history_job_category_task, history_position_types_task, history_organizations_task]