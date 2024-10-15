from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from plugins.etl import extract_data, transform_data, load_to_redshift
import os
# import sys

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DATA_TEMP = os.getenv('DATA_TEMP')

def history_extract_data_func(**kwargs):
    output_parquet = kwargs['output_parquet']
    keyword = kwargs['keyword']
    # Enviamos cantidad de dias al maximo posible (60 días)
    # Para no procesar dias incompletos se descuenta 1 a la fecha y se descartan las publicaciones del dia
    date_posted = 60
    #Descontamos un dia un quitamos HH:mm:ss
    date_control = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    print(f'Parametros de extract_data_jobs:\n' 
          f'Palabra Clave: {keyword},\n' 
          f'Path para parquet: {output_parquet}, \n' 
          f'Cantidad en días: {date_posted}, \n' 
          f'Control de fecha: {date_control}')
    
    return extract_data.extract_data_jobs(keyword,output_parquet, date_posted, date_control)

def history_transform_data_func(**kwargs):
       
    parquet_file = kwargs['ti'].xcom_pull(task_ids='history_extract_data')
    print(f'parquet_file: {parquet_file}')
    return transform_data.transform_data_jobs(parquet_file)

def history_load_data_func(**kwargs):
    data_frame = kwargs['ti'].xcom_pull(task_ids='history_transform_data')
    print(f'data_frame: {data_frame}')
    load_to_redshift.load_jobs_redshift(data_frame)


with DAG(
    'history_usa_job_software_dag',
    default_args={
        'dependeds_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL para extraer datos HISTORICOS de Usa Jobs, transformar y cargar en Redshift ',
    schedule_interval=None,
    start_date=datetime(2024, 10, 7),
    catchup=False,#para backfill
) as dag:

 # Task 1: Extract data
    history_extract_data_task = PythonOperator(
        task_id='history_extract_data',
        python_callable=history_extract_data_func,
        op_kwargs={'output_parquet': DATA_TEMP, 
                   'keyword': 'Software'},
    )

    # Task 2: Transform data
    history_transform_data_task = PythonOperator(
        task_id='history_transform_data',
        python_callable=history_transform_data_func,
        op_kwargs={},
    )

    # Task 3: Load data into Redshift
    history_load_data_task = PythonOperator(
        task_id='history_load_data',
        python_callable=history_load_data_func,
        op_kwargs={},
    )


# Definir dependencias
history_extract_data_task >> history_transform_data_task >> history_load_data_task