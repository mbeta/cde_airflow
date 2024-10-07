import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, insert
from sqlalchemy.exc import SQLAlchemyError
import os
from dotenv import load_dotenv
from db_services import get_redshift_connection


load_dotenv()

def get_redshift_table_schema(engine, schema, table_name):
    with engine.connect() as connection:
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table_name}';
        """
        return pd.read_sql(query, connection.execution_options(autocommit=True))

type_mapping = {
    'int64': 'integer',
    'float64': 'double precision',
    'object': 'varchar',
    'datetime64[ns]': 'timestamp',
    'bool': 'boolean'
}

def load_jobs_redshift(df: pd.DataFrame):
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    schema = os.getenv('REDSHIFT_SCHEMA')
    engine = create_engine(redshift_conn_string)

    try:
        # Asegurarse de que el DataFrame tiene las columnas correctas
        expected_columns = [
            'object_id', 'position_id', 'position_title', 
            'location_description', 'organization_code', 
            'department_code', 'job_category_code', 
            'position_start_date', 'position_end_date', 
            'publication_start_date', 'application_close_date', 
            'minimum_salary', 'maximum_salary', 
            'rate_interval_description', 'position_type_code', 
            'detail_position_type', 'version_date', 'duration'
        ]
        
        if not all(col in df.columns for col in expected_columns):
            raise ValueError("El DataFrame no contiene todas las columnas necesarias.")
        
        # Probar la conexión
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            print("Conexión exitosa a Redshift.")

            # Obtener el esquema de la tabla de Redshift
            redshift_schema = get_redshift_table_schema(engine, schema, 'jobs')
            # Verificar los tipos de datos
            for column in df.columns:
                if column in redshift_schema['column_name'].values:
                    redshift_type = redshift_schema[redshift_schema['column_name'] == column]['data_type'].values[0]
                    pandas_type = str(df[column].dtype)

                    # Mapeo de tipos de pandas a Redshift
                    if pandas_type not in type_mapping:
                        raise TypeError(f"Tipo de datos {pandas_type} no es compatible con Redshift.")
                    
                    expected_redshift_type = type_mapping[pandas_type]
                    if expected_redshift_type != redshift_type:
                        raise TypeError(f"Columna '{column}': Tipo de datos esperado en Redshift es '{redshift_type}', pero el DataFrame tiene '{pandas_type}'.")

            # Cargar el DataFrame en Redshift
            df.to_sql('jobs', con=engine, schema=schema, if_exists='append', index=False)

            print("Datos insertados exitosamente en Redshift.")

    except SQLAlchemyError as e:
        print(f"Error al insertar en Redshift: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        engine.dispose()  # Asegurarte de cerrar la conexión

# def load_jobs_redshift(df: pd.DataFrame):
#     # Crear la conexión a Redshift
#     redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
#     schema = os.getenv("REDSHIFT_SCHEMA")  # No necesitas agregar comillas aquí
#     engine = create_engine(redshift_conn_string)

#     try:
#         # Asegurarse de que el DataFrame tiene las columnas correctas
#         expected_columns = [
#             'object_id', 'position_id', 'position_title', 
#             'location_description', 'organization_code', 
#             'department_code', 'job_category_code', 
#             'position_start_date', 'position_end_date', 
#             'publication_start_date', 'application_close_date', 
#             'minimum_salary', 'maximum_salary', 
#             'rate_interval_description', 'position_type_code', 
#             'detail_position_type', 'version_date', 'duration'
#         ]
        
#         if not all(col in df.columns for col in expected_columns):
#             raise ValueError("El DataFrame no contiene todas las columnas necesarias.")


#         # Probar la conexión
#         with engine.connect() as connection:
#             result = connection.execute("SELECT 1")
#             print("Conexión exitosa a Redshift.")
            
            
#         # Cargar el DataFrame en Redshift
#         df.to_sql('jobs', con=engine, schema=schema, if_exists='append', index=False)

#         print("Datos insertados exitosamente en Redshift.")
#     except Exception as e:
#         print(f"Error al insertar en Redshift: {e}")
#     finally:
#         engine.dispose()  # Asegurarte de cerrar la conexión


# def load_jobs_redshift(jobs_df):
#     # Supongamos que 'conn' es tu conexión a Redshift
#     conn = get_redshift_connection()
    
#     # Define la metadata y la tabla
#     metadata = MetaData(bind=conn)
#     jobs_table = Table('jobs', metadata, autoload_with=conn)
    
#     # Prepara los datos a insertar
#     data = jobs_df.to_records(index=False)
    
#     print("Datos a insertar:", data)

#     # Genera la consulta de inserción
#     insert_query = insert(jobs_table)
    
#     # Ejecuta la inserción usando executemany
#     try:
#         conn.execute(insert_query, data)  # Asegúrate de que 'data' sea compatible con 'insert_query'
#     except Exception as e:
#         print("Error al insertar en Redshift:", e)
#     finally:
#         conn.close()

# def load_jobs_redshift(df: pd.DataFrame):
#     # Crear la conexión a Redshift
#     redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
#     schema = f'"{os.getenv("REDSHIFT_SCHEMA")}"'
#     engine = create_engine(redshift_conn_string)

#     # Consulta de inserción
#     insert_query = f"""
#     INSERT INTO {schema}.jobs (
#         object_id, position_id, position_title, location_description, 
#         organization_code, department_code, job_category_code, 
#         position_start_date, position_end_date, 
#         publication_start_date, application_close_date, 
#         minimum_salary, maximum_salary, 
#         rate_interval_description, position_type_code, 
#         detail_position_type, version_date, duration) 
#     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#     """

#     # Convierte el DataFrame a una lista de tuplas
#     data = [tuple(row) for row in df.to_numpy()]
    
#     print(f"Datos a insertar: {data}")

#     # Realiza la inserción usando executemany
#     with engine.connect() as conn:
#         conn.execute(insert_query, data)
        
#     print(f"{len(data)} registros insertados en la tabla jobs.")
        
        
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
            code = row['code']
            name = row['name']
            parent_code = row['parent_code']
            last_modified = row['last_modified']
            is_disabled = row['is_disabled']

            # Verificar si el registro ya existe en la tabla DimOrganization
            query_check = f"SELECT COUNT(*) FROM {schema}.dim_organization WHERE code = %s"
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
                UPDATE {schema}.dim_organization
                SET name = %s, parent_code = %s, last_modified = %s, is_disabled = %s
                WHERE code = %s
            """
            conn.execute(query_update, update_queries)

            print(f"{len(update_queries)} registros actualizados.")

        # Ejecutar todos los inserts en una sola operación
        if insert_queries:
            query_insert = f"""
                INSERT INTO {schema}.dim_organization (code, name, parent_code, last_modified, is_disabled)
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
            code = row['code']
            name = row['name']
            job_family = row['job_family']
            last_modified = row['last_modified']
            is_disabled = row['is_disabled']

            # Verificar si el registro ya existe en la tabla DimJobCategory
            query_check = f"SELECT COUNT(*) FROM {schema}.dim_job_category WHERE code = %s"
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
                UPDATE {schema}.dim_job_category
                SET name = %s, job_family = %s, last_modified = %s, is_disabled = %s
                WHERE code = %s
            """
            conn.execute(query_update, update_queries)

            print(f"{len(update_queries)} registros actualizados.")

        # Ejecutar todos los inserts en una sola operación
        if insert_queries:
            query_insert = f"""
                INSERT INTO {schema}.dim_job_category (code, name, job_family, last_modified, is_disabled)
                VALUES (%s, %s, %s, %s, %s)
            """
            conn.execute(query_insert, insert_queries)

            print(f"{len(insert_queries)} registros insertados.")
    
    print("Proceso de UPSERT para la dimension JOB CATEGORY completado.")
    
    
def load_position_types_redshift(df: pd.DataFrame):
    """
    Carga los datos en Redshift con la lógica de UPSERT (actualizar si existe, insertar si no).
    Dimension: POSITION TYPE
    
    Arguments:
    df : pd.DataFrame : El DataFrame con los datos a cargar.
    """
    
    print("Se inicia Proceso de UPSERT para la dimension POSITION TYPE")
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
            code = row['code']
            name = row['name']
            last_modified = row['last_modified']
            is_disabled = row['is_disabled']

            # Verificar si el registro ya existe en la tabla DimJobCategory
            query_check = f"SELECT COUNT(*) FROM {schema}.dim_position_type WHERE code = %s"
            result = conn.execute(query_check, (code)).scalar()

            if result > 0:
                # Si el registro existe, agregar un UPDATE a la lista
                update_queries.append((
                    name, last_modified, is_disabled, code
                ))
            else:
               # Si el registro no existe, agregar un INSERT a la lista
                insert_queries.append((
                    code, name, last_modified, is_disabled
                ))
                
        
        # Ejecutar todas las actualizaciones en una sola operación
        if update_queries:
            query_update = f"""
                UPDATE {schema}.dim_position_type
                SET name = %s,  last_modified = %s, is_disabled = %s
                WHERE code = %s
            """
            conn.execute(query_update, update_queries)

            print(f"{len(update_queries)} registros actualizados.")

        # Ejecutar todos los inserts en una sola operación
        if insert_queries:
            query_insert = f"""
                INSERT INTO {schema}.dim_position_type (code, name, last_modified, is_disabled)
                VALUES (%s, %s, %s, %s)
            """
            conn.execute(query_insert, insert_queries)

            print(f"{len(insert_queries)} registros insertados.")
    
    print("Proceso de UPSERT para la dimension POSITION TYPE completado.")