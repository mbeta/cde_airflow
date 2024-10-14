from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from datetime import datetime
from plugins.etl import extract_data, transform_data, load_to_redshift
import os

DATA_PATH = os.path.dirname(os.path.realpath(__file__))

def extract_data_func(**kwargs):
    output_parquet = kwargs['output_parquet']
    keyword = kwargs['keyword']
    date_control = kwargs['date_control']
    # Calculamos la cantidad de dias de la fecha que se quiere consultar con la fecha actual
    # API solo traera resultados hasta los 60 dias
    # Para no procesar dias incompletos se incrementa en 1 y se descartan las publicaciones del dia
    date_posted = ((datetime.now() - datetime.strptime(date_control, '%Y-%m-%d')).days+1)
    print(f"Diferencia en días: {date_posted}")
    if date_posted > 60:
        raise AirflowSkipException(f"El DAG se está ejecutando con una fecha mayor a 60 días ({date_posted} días). Saltando ejecución.")
    
    print(f"Parametros de extract_data_jobs:\n 
          Palabra Clave: {keyword},\n 
          Path para parquet: {output_parquet}, \n 
          Cantidad en días: {date_posted}, \n 
          Control de fecha: {date_control}")
    
    return extract_data.extract_data_jobs(keyword,output_parquet, date_posted, date_control)

def transform_data_func(**kwargs):
    parquet_file = kwargs['ti'].xcom_pull(task_ids='extract_data_task')
    
    return transform_data.transform_data_jobs(parquet_file)

def load_data_func(**kwargs):
    data_frame = kwargs['ti'].xcom_pull(task_ids='transform_data_task')
    load_to_redshift.load_jobs_redshift(data_frame)


with DAG(
    'etl_usa_job_software_dag',
    default_args={
        'dependeds_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
    },
    description='ETL para extraer datos de Usa Jobs, transformar y cargar en Redshift',
    schedule_interval='@daily',
    sart_date=datetime(2024, 10, 7),
    catchup=True,#para backfill
) as dag:

 # Task 1: Extract data
    extract_task = PythonOperator(
        task_id='extract_data_task',
        python_callable=extract_data_func,
        op_kwargs={'output_parquet': DATA_PATH, 
                   'keyword': 'Software',
                   'date_control': '{{ ds }}'},
    )

    # Task 2: Transform data
    transform_task = PythonOperator(
        task_id='transform_data_task',
        python_callable=transform_data_func,
        op_kwargs={},
    )

    # Task 3: Load data into Redshift
    load_task = PythonOperator(
        task_id='load_data_task',
        python_callable=load_data_func,
        op_kwargs={},
    )


# Definir dependencias
wait_for_dimensions >> extract_data_task >> transform_data_task >> load_data_task