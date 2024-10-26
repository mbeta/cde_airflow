from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.etl import db_services, extract_data, transform_data, load_to_redshift
from datetime import datetime, timedelta
import os
from airflow.exceptions import AirflowException 
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_TEMP = os.getenv('DATA_TEMP')

def etl_job_category(fecha_contexto):
    #ETL para Dimension Job Category
    try:
        #Se crea tablas si no existen
        logger.info("Se crea tabla dim_job_category si no existe")
        db_services.create_job_category_table()
        logger.info(f'Fecha contexto ejecucion: {fecha_contexto}')
        lastmodified = (datetime.strptime(fecha_contexto, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f'Fecha ultima modificacion para consulta API: {lastmodified}')
        data_path = extract_data.extract_data_job_categories(lastmodified, DATA_TEMP)
        logger.info(f'Parquet de datos guardados en: {data_path}')
        if(data_path):
            transformed_data_parquet = transform_data.transform_data_category(data_path)
            load_to_redshift.load_categories_redshift(transformed_data_parquet)
    
    except Exception as e:
        logger.error(f'Error en el proceso ETL de Job Category: {str(e)}')
        raise AirflowException(f'Fallo en el proceso ETL de Job Category: {str(e)}')

def etl_position_types(fecha_contexto):
    #ETL para Dimension Position Types
    try: 
        logger.info("Se crea tabla dim_position_type si no existe")
        db_services.create_position_type_table()
        logger.info(f'Fecha contexto ejecucion: {fecha_contexto}')
        lastmodified = (datetime.strptime(fecha_contexto, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f'Fecha ultima modificacion para consulta API: {lastmodified}')
        data_path = extract_data.extract_data_position_type(lastmodified, DATA_TEMP)
        logger.info(f'Parquet de datos guardados en: {data_path}')
        if(data_path):
            transformed_data_parquet = transform_data.transform_data_position_types(data_path)
            load_to_redshift.load_position_types_redshift(transformed_data_parquet)
    except Exception as e:
        logger.error(f'Error en el proceso ETL de Position Types: {str(e)}')
        raise AirflowException(f'Fallo en el proceso ETL de Position Types: {str(e)}')
    
    

def etl_organizations(fecha_contexto):
    
    #ETL para Dimension Organizations
    try:
        logger.info("Se crea tabla dim_organization si no existe")
        db_services.create_organization_table()
        logger.info(f'Fecha contexto ejecucion: {fecha_contexto}')
        lastmodified = (datetime.strptime(fecha_contexto, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f'Fecha ultima modificacion para consulta API: {lastmodified}')
        data_path = extract_data.extract_data_organization(lastmodified, DATA_TEMP)
        logger.info(f'Parquet de datos guardados en: {data_path}')
        if(data_path):
            transformed_data_parquet = transform_data.transform_data_organization(data_path)
            load_to_redshift.load_organization_redshift(transformed_data_parquet)
    except Exception as e:
        logger.error(f'Error en el proceso ETL de Organizations: {str(e)}')
        raise AirflowException(f'Fallo en el proceso ETL de Organizations: {str(e)}')
    
    
with DAG(
    'etl_dimensions_dag',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL pipeline para extraccion, transformacion y carga de datos de Dimensiones',
    schedule_interval='@daily',
    start_date= datetime(2024, 10, 24),
    catchup=False,
) as dag:

    # Task 1: ETL data Organizations
    etl_organizations_task = PythonOperator(
        task_id='etl_organizations',
        python_callable=etl_organizations,
        op_kwargs={'fecha_contexto':'{{ ds }}'},
    )

    # Task 2: ETL data Job Category
    etl_job_category_task = PythonOperator(
        task_id='etl_job_category',
        python_callable=etl_job_category,
        op_kwargs={'fecha_contexto':'{{ ds }}'},
    )

    # Task 3: ETL data Position Type
    etl_position_types_task = PythonOperator(
        task_id='etl_position_types',
        python_callable=etl_position_types,
        op_kwargs={'fecha_contexto':'{{ ds }}'},
    )

#Ejecucion en paralelo
[etl_job_category_task, etl_position_types_task, etl_organizations_task]