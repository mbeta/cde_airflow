from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException 
from plugins.etl import db_services
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_TEMP = os.getenv('DATA_TEMP')

def drop_schema_func(**kwargs):
    
    logger.info(f'Parametros de extract_data_jobs:\n')
    try:
        db_services.drop_schema()
    except Exception as e:
        logger.error(f'Error en el proceso DROP SCHEMAS: {str(e)}')
        raise AirflowException(f'Fallo en el proceso DROP SCHEMAS: {str(e)}')




with DAG(
    'drop_schema_dag',
    default_args={
        'dependeds_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='Funcionalidad para eliminar todas las tablas del schema en Redshift ',
    schedule_interval=None,
    catchup=False,#para backfill
) as dag:

 # Task 1: Drop Schema
    drop_schema_task = PythonOperator(
        task_id='drop_schema',
        python_callable=drop_schema_func,
        op_kwargs={},
    )




# Definir dependencias
drop_schema_task