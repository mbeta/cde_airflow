from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import DagRun
from airflow import settings
from airflow.exceptions import AirflowException 
from datetime import datetime, timedelta
from plugins.etl import extract_data, transform_data, load_to_redshift
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_TEMP = os.getenv('DATA_TEMP')

def extract_data_func(**kwargs):
    output_parquet = kwargs['output_parquet']
    keyword = kwargs['keyword']
    fecha_contexto =kwargs['fecha_contexto']
    
    logger.info(f'Fecha contexto: {fecha_contexto}')
    #Descontamos un dia un quitamos HH:mm:ss
    date_control = (datetime.strptime(fecha_contexto, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    # Calculamos la cantidad de dias de la fecha que se quiere consultar con la fecha actual
    # API solo traera resultados hasta los 60 dias (date_posted, se lo consigna mediant calculo por posibles reintetos de corridas)
    # Para no procesar dias incompletos se descuenta 1 dia a la fecha y se descartan las publicaciones del dia
    # Es decir que todos los dias se procasaran los posteos del dia ANTERIOR
    date_posted = ((datetime.now() - datetime.strptime(date_control, '%Y-%m-%d')).days)
    logger.info(f"Diferencia en días: {date_posted}")
    if date_posted > 60:
        raise AirflowSkipException(f"El DAG se está ejecutando con una fecha mayor a 60 días ({date_posted} días). Saltando ejecución.")
    
    logger.info(f'Parametros de extract_data_jobs:\n' 
          f'Palabra Clave: {keyword},\n' 
          f'Path para parquet: {output_parquet}, \n' 
          f'Cantidad en días: {date_posted}, \n' 
          f'Control de fecha: {date_control}')
    
    try:
        return extract_data.extract_data_jobs(keyword,output_parquet, date_posted, date_control)
    except Exception as e:
        logger.error(f'Error en el proceso EXTRACT - ETL de Jobs: {str(e)}')
        raise AirflowException(f'Fallo en el proceso EXTRACT - ETL de Jobs: {str(e)}')

def transform_data_func(**kwargs):
    parquet_file = kwargs['ti'].xcom_pull(task_ids='extract_data')
    if(parquet_file):
        logger.info(f'parquet_file: {parquet_file}')
        try:
            return transform_data.transform_data_jobs(parquet_file)
        except Exception as e:
            logger.error(f'Error en el proceso TRANSFORM - ETL de Jobs: {str(e)}')
            raise AirflowException(f'Fallo en el proceso TRANSFORM - ETL de Jobs: {str(e)}')
    else:
        logger.info(f'Sin datos a transformar')
        return None

def load_data_func(**kwargs):
    data_frame = kwargs['ti'].xcom_pull(task_ids='transform_data')
    if data_frame is not None and not data_frame.empty:
        logger.info(f'Cantidad reg. data_frame: {len(data_frame)}')
        try:
            load_to_redshift.load_jobs_redshift(data_frame)
        except Exception as e:
            logger.error(f'Error en el proceso LOAD - ETL de Jobs: {str(e)}')
            raise AirflowException(f'Fallo en el proceso LOAD - ETL de Jobs: {str(e)}')
    else:
        logger.info(f'Sin datos a cargar en Redshift')

#Funcion para controlar si el DAG de dimensiones se ejecuto correctamente
def get_execution_date(dt, **kwargs):
    session = settings.Session()
    
    # Obtener la última ejecución del DAG
    dr = session.query(DagRun)\
        .filter(DagRun.dag_id == kwargs['task'].external_dag_id)\
        .order_by(DagRun.execution_date.desc())\
        .first()
    
    if not dr:
        raise AirflowSkipException(f"No se encontró una ejecución previa para el DAG {kwargs['task'].external_dag_id}")
    
    # Comparar la fecha de ejecución con la fecha actual
    current_time = datetime.now(dr.execution_date.tzinfo)  # Mantener la zona horaria consistente
    time_difference = current_time - dr.execution_date
    
    # Imprimir información útil para el debug
    logger.info(f"Última ejecución del DAG {kwargs['task'].external_dag_id}: {dr.execution_date}")
    logger.info(f"Diferencia en horas: {time_difference.total_seconds() / 3600}")
    
    # Verificar si han pasado más de 24 horas
    if time_difference > timedelta(hours=24):
        raise AirflowSkipException(f"Última ejecución del DAG {kwargs['task'].external_dag_id} fue hace más de 24 horas.")
    
    return dr.execution_date

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
    start_date=datetime(2024, 10, 7),
    catchup=True,#para backfill
) as dag:
   
    #Task para esperar Dag de dimensiones
    wait_for_dimensions = ExternalTaskSensor(
        task_id='wait_for_dimensions',
        external_dag_id='etl_dimensions_dag',  # Nombre del DAG que debe esperar
        external_task_id=None,  #Esperará a todo el DAG
        execution_date_fn=get_execution_date, #con que se haya ejecutado una vez en 24hs es suficiente
        mode='poke',  # Alternativa: 'reschedule' para menos carga de recursos
        timeout=3600,  # Tiempo máximo que esperará el sensor (1 Hora)
        poke_interval=120,  # Intervalo en segundos entre cada chequeo
        allowed_states=['success'],  # Espera a que el dag esté en estado de 'success'
        failed_states=['failed']  # Se detendrá si dag falla o es saltado
    )

    # Task 1: Extract data
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_func,
        op_kwargs={'output_parquet': DATA_TEMP, 
                   'keyword': 'Software',
                   'fecha_contexto': '{{ ds }}'},
    )

    # Task 2: Transform data
    transform_data_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_func,
        op_kwargs={},
    )

    # Task 3: Load data into Redshift
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_func,
        op_kwargs={},
    )


# Definir dependencias
wait_for_dimensions >> extract_data_task >> transform_data_task >> load_data_task