import pandas as pd
from sqlalchemy import create_engine
import os

def load_to_redshift(df: pd.DataFrame):
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    engine = create_engine(redshift_conn_string)
    
    # Cargar los datos en la tabla 'jobs'
    df.to_sql('jobs', engine, if_exists='append', index=False)
    
    print("Datos cargados a Redshift con éxito.")
