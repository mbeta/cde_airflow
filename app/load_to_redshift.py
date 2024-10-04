import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv


load_dotenv()

def load_to_redshift(df: pd.DataFrame):
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    engine = create_engine(redshift_conn_string)
    
    # Cargar los datos en la tabla 'jobs'
    df.to_sql('jobs', engine, if_exists='append', index=False)
    
    print("Datos cargados a Redshift con éxito.")


def load_organization_redshift(df: pd.DataFrame):
    """
    Carga los datos en Redshift con la lógica de UPSERT (actualizar si existe, insertar si no).
    Dimension: ORGANIZATION
    
    Arguments:
    df : pd.DataFrame : El DataFrame con los datos a cargar.
    """
    
    print("Se inicia Proceso de UPSERT para la dimension ORGANIZATION")
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    engine = create_engine(redshift_conn_string)
    
    # Listas para almacenar los datos de actualización e inserción
    update_queries = []
    insert_queries = []
    
      # Iniciar la conexión y transacción
    with engine.connect() as conn:
        for _, row in df.iterrows():
            code = row['Code']
            name = row['Name']
            parent_code = row['ParentCode']
            last_modified = row['LastModified']
            is_disabled = row['IsDisabled']

            # Verificar si el registro ya existe en la tabla DimOrganization
            query_check = f"SELECT COUNT(*) FROM {schema}.DimOrganization WHERE Code = %s"
            result = conn.execute(query_check, (code)).scalar()

            if result > 0:
                # Si el registro existe, agregar un UPDATE a la lista
                update_queries.append((
                    name, parent_code, last_modified, is_disabled, code
                ))
            else:
               # Si el registro no existe, agregar un INSERT a la lista
                insert_queries.append((
                    code, name, parent_code, last_modified, is_disabled
                ))
                
        
        # Ejecutar todas las actualizaciones en una sola operación
        if update_queries:
            query_update = f"""
                UPDATE {schema}.DimOrganization
                SET Name = %s, ParentCode = %s, LastModified = %s, IsDisabled = %s
                WHERE Code = %s
            """
            conn.execute(query_update, update_queries)

            print(f"{len(update_queries)} registros actualizados.")

        # Ejecutar todos los inserts en una sola operación
        if insert_queries:
            query_insert = f"""
                INSERT INTO {schema}.DimOrganization (Code, Name, ParentCode, LastModified, IsDisabled)
                VALUES (%s, %s, %s, %s, %s)
            """
            conn.execute(query_insert, insert_queries)

            print(f"{len(insert_queries)} registros insertados.")
    
    print("Proceso de UPSERT para la dimension ORGANIZATION completado.")
    
def load_categories_redshift(df: pd.DataFrame):
    """
    Carga los datos en Redshift con la lógica de UPSERT (actualizar si existe, insertar si no).
    Dimension: JOB CATEGORY
    
    Arguments:
    df : pd.DataFrame : El DataFrame con los datos a cargar.
    """
    
    print("Se inicia Proceso de UPSERT para la dimension JOB CATEGORY")
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    engine = create_engine(redshift_conn_string)
    
    # Listas para almacenar los datos de actualización e inserción
    update_queries = []
    insert_queries = []
    
      # Iniciar la conexión y transacción
    with engine.connect() as conn:
        for _, row in df.iterrows():
            code = row['Code']
            name = row['Name']
            job_family = row['JobFamily']
            last_modified = row['LastModified']
            is_disabled = row['IsDisabled']

            # Verificar si el registro ya existe en la tabla DimJobCategory
            query_check = f"SELECT COUNT(*) FROM {schema}.DimJobCategory WHERE Code = %s"
            result = conn.execute(query_check, (code)).scalar()

            if result > 0:
                # Si el registro existe, agregar un UPDATE a la lista
                update_queries.append((
                    name, job_family, last_modified, is_disabled, code
                ))
            else:
               # Si el registro no existe, agregar un INSERT a la lista
                insert_queries.append((
                    code, name, job_family, last_modified, is_disabled
                ))
                
        
        # Ejecutar todas las actualizaciones en una sola operación
        if update_queries:
            query_update = f"""
                UPDATE {schema}.DimJobCategory
                SET Name = %s, JobFamily = %s, LastModified = %s, IsDisabled = %s
                WHERE Code = %s
            """
            conn.execute(query_update, update_queries)

            print(f"{len(update_queries)} registros actualizados.")

        # Ejecutar todos los inserts en una sola operación
        if insert_queries:
            query_insert = f"""
                INSERT INTO {schema}.DimJobCategory (Code, Name, JobFamily, LastModified, IsDisabled)
                VALUES (%s, %s, %s, %s, %s)
            """
            conn.execute(query_insert, insert_queries)

            print(f"{len(insert_queries)} registros insertados.")
    
    print("Proceso de UPSERT para la dimension JOB CATEGORY completado.")