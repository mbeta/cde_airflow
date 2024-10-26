from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException 
from plugins.etl import db_services, extract_data, transform_data, load_to_redshift
from datetime import datetime
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DATA_TEMP = os.getenv('DATA_TEMP')

def history_job_category(fecha_contexto):
    try:
        logger.info(f'{fecha_contexto}: Se inicia carga HISTORICA de Dimension Job Category')
        #Se crea tablas si no existen
        logger.info("Se crea tabla dim_job_category si no existe")
        db_services.create_job_category_table()
        
        data_path = extract_data.extract_data_job_categories('', DATA_TEMP)
        logger.info(data_path)
        if(data_path):
            transformed_data_parquet = transform_data.transform_data_category(data_path)
            load_to_redshift.load_categories_redshift(transformed_data_parquet)
    except Exception as e:
        logger.error(f'Error en el proceso HISTORY - ETL de Job Category: {str(e)}')
        raise AirflowException(f'Fallo en el proceso HISTORY - ETL de Job Category: {str(e)}')

def history_position_types(fecha_contexto):
    try:
        logger.info(f'{fecha_contexto}: Se inicia carga HISTORICA de Dimension Position Types')
        logger.info("Se crea tabla dim_position_type si no existe")
        db_services.create_position_type_table()
        data_path = extract_data.extract_data_position_type('', DATA_TEMP)
        logger.info(data_path)
        if(data_path):
            transformed_data_parquet = transform_data.transform_data_position_types(data_path)
            load_to_redshift.load_position_types_redshift(transformed_data_parquet)
    except Exception as e:
        logger.error(f'Error en el proceso HISTORY - ETL de Position Types: {str(e)}')
        raise AirflowException(f'Fallo en el proceso HISTORY - ETL de Position Types: {str(e)}')
    
    

def history_organizations(fecha_contexto):
    try:
        logger.info(f'{fecha_contexto}: Se inicia carga HISTORICA de Dimension Organizations')
        logger.info("Se crea tabla dim_organization si no existe")
        db_services.create_organization_table()
        
        data_path = extract_data.extract_data_organization('', DATA_TEMP)
        logger.info(data_path)
        if(data_path):
            transformed_data_parquet = transform_data.transform_data_organization(data_path)
            load_to_redshift.load_organization_redshift(transformed_data_parquet)
    except Exception as e:
        logger.error(f'Error en el proceso HISTORY - ETL de Organizarions: {str(e)}')
        raise AirflowException(f'Fallo en el proceso HISTORY - ETL de Organizations: {str(e)}')
    
    
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