import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()

def load_jobs_redshift(df: pd.DataFrame, batch_size: int = 50):
    """
    Carga los datos en Redshift con la lógica de UPSERT
    (actualizar si existe, insertar si no).
    Tabla: JOBS

    Arguments:
    df : pd.DataFrame : El DataFrame con los datos a cargar.
    """

    logger.info("Se inicia Proceso de UPSERT para la tabla JOBS")
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    engine = create_engine(redshift_conn_string,  isolation_level='READ COMMITTED')

    # Iniciar la conexión y transacción
    with engine.connect() as conn:
        total_rows = len(df)
        logger.info(f'Total de registros a tratar: {total_rows}')
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            df_batch = df.iloc[start:end]

            update_queries = []
            insert_queries = []
            for _, row in df_batch.iterrows():
                object_id = row['object_id']
                position_id = row['position_id']
                position_title = row['position_title']
                location_description = row['location_description']
                organization_code = row['organization_code']
                department_code = row['department_code']
                job_category_code = row['job_category_code']
                position_start_date = row['position_start_date']
                position_end_date = row['position_end_date']
                publication_start_date = row['publication_start_date']
                application_close_date = row['application_close_date']
                minimum_salary = row['minimum_salary']
                maximum_salary = row['maximum_salary']
                rate_interval_description = row['rate_interval_description']
                position_type_code = row['position_type_code']
                detail_position_type = row['detail_position_type']
                version_date = row['version_date']
                is_reposted = 0
                duration = row['duration']

                # Verificar si el registro ya existe en la tabla Jobs
                query_check = f"""
                    SELECT COUNT(*) FROM {schema}.jobs WHERE object_id = %s
                """
                
                try:
                    result = conn.execute(query_check, (object_id)).scalar()
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query jobs REDSHIFT: {e}")
                    raise

                if result > 0:
                    # Si el registro existe, agregar un UPDATE a la lista
                    is_reposted = 1
                    update_queries.append((
                        position_id, position_title, location_description,
                        organization_code, department_code, job_category_code,
                        position_start_date, position_end_date,
                        publication_start_date, application_close_date,
                        minimum_salary, maximum_salary, rate_interval_description,
                        position_type_code, detail_position_type, version_date,
                        is_reposted, duration, object_id))
                else:
                    # Si el registro no existe, agregar un INSERT a la lista
                    insert_queries.append((
                        object_id, position_id, position_title,
                        location_description, organization_code, department_code,
                        job_category_code, position_start_date, position_end_date,
                        publication_start_date, application_close_date,
                        minimum_salary, maximum_salary, rate_interval_description,
                        position_type_code, detail_position_type, version_date,
                        is_reposted, duration))

            # Ejecutar todas las actualizaciones en una sola operación
            if update_queries:
                query_update = f"""
                    UPDATE {schema}.jobs
                    SET position_id  = %s, position_title = %s,
                        location_description = %s, organization_code = %s,
                        department_code = %s, job_category_code = %s,
                        position_start_date = %s, position_end_date = %s,
                        publication_start_date = %s, application_close_date = %s,
                        minimum_salary = %s, maximum_salary = %s,
                        rate_interval_description = %s, position_type_code = %s,
                        detail_position_type = %s, version_date = %s,
                        is_reposted = %s, duration = %s
                    WHERE object_id = %s
                """
                
                
                try:
                    conn.execute(query_update, update_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query UPDATE jobs REDSHIFT: {e}")
                    raise

                logger.info(f"{len(update_queries)} registros actualizados en el lote {start // batch_size + 1}.")

            # Ejecutar todos los inserts en una sola operación
            if insert_queries:
                query_insert = f"""
                    INSERT INTO {schema}.jobs (object_id, position_id,
                        position_title, location_description, organization_code,
                        department_code, job_category_code, position_start_date,
                        position_end_date, publication_start_date,
                        application_close_date, minimum_salary, maximum_salary,
                        rate_interval_description, position_type_code,
                        detail_position_type, version_date, is_reposted, duration)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s)
                """
                
                try:
                    conn.execute(query_insert, insert_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query INSERT jobs REDSHIFT: {e}")
                    raise

                logger.info(f"{len(insert_queries)} registros insertados en el lote {start // batch_size + 1}.")
    logger.info("Proceso de UPSERT para la tabla JOBS completado.")


def load_organization_redshift(df: pd.DataFrame, batch_size: int = 50):
    """
    Carga los datos en Redshift con la lógica de UPSERT
    (actualizar si existe, insertar si no).
    Dimension: ORGANIZATION

    Arguments:
    df : pd.DataFrame : El DataFrame con los datos a cargar.
    """

    logger.info("Se inicia Proceso de UPSERT para la dimension ORGANIZATION")
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    engine = create_engine(redshift_conn_string)

    # # Listas para almacenar los datos de actualización e inserción
    # update_queries = []
    # insert_queries = []

    # Iniciar la conexión y transacción
    with engine.connect() as conn:
        total_rows = len(df)
        logger.info(f'Total de registros a tratar: {total_rows}')
        #for _, row in df.iterrows():
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            df_batch = df.iloc[start:end]
            
            
            # Listas para almacenar los datos de actualización e inserción
            update_queries = []
            insert_queries = []
            
            for _, row in df_batch.iterrows():
                code = row['code']
                name = row['name']
                parent_code = row['parent_code']
                last_modified = row['last_modified']
                is_disabled = row['is_disabled']

                # Verificar si el registro ya existe en la tabla DimOrganization
                query_check = f"""
                        SELECT COUNT(*) FROM {schema}.dim_organization
                                WHERE code = %s
                        """
                
                
                try:
                    result = conn.execute(query_check, (code)).scalar()
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query dim_organization REDSHIFT: {e}")
                    raise

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

            # Ejecutar todas las actualizaciones del lote en una sola operación
            if update_queries:
                query_update = f"""
                    UPDATE {schema}.dim_organization
                    SET name = %s, parent_code = %s, last_modified = %s,
                                is_disabled = %s
                    WHERE code = %s
                """
                
                try:
                    conn.execute(query_update, update_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query UPDATE dim_organization REDSHIFT: {e}")
                    raise

                print(f"{len(update_queries)} registros actualizados en el lote {start // batch_size + 1}.")

            # Ejecutar todos los inserts del lote en una sola operación
            if insert_queries:
                query_insert = f"""
                    INSERT INTO {schema}.dim_organization (code, name, parent_code,
                                                        last_modified, is_disabled)
                    VALUES (%s, %s, %s, %s, %s)
                """
                
                try:
                    conn.execute(query_insert, insert_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query INSERT dim_organization REDSHIFT: {e}")
                    raise

                logger.info(f"{len(insert_queries)} registros insertados en el lote {start // batch_size + 1}.")

    logger.info("Proceso de UPSERT para la dimension ORGANIZATION completado.")


def load_categories_redshift(df: pd.DataFrame, batch_size: int = 50):
    """
    Carga los datos en Redshift con la lógica de UPSERT
    (actualizar si existe, insertar si no).
    Dimension: JOB CATEGORY

    Arguments:
    df : pd.DataFrame : El DataFrame con los datos a cargar.
    """

    logger.info("Se inicia Proceso de UPSERT para la dimension JOB CATEGORY")
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    engine = create_engine(redshift_conn_string)

    # Iniciar la conexión y transacción
    with engine.connect() as conn:
        total_rows = len(df)
        logger.info(f'Total de registros a tratar: {total_rows}')
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            df_batch = df.iloc[start:end]

             # Listas para almacenar los datos de actualización e inserción
            update_queries = []
            insert_queries = []
        
            for _, row in df_batch.iterrows():
                code = row['code']
                name = row['name']
                job_family = row['job_family']
                last_modified = row['last_modified']
                is_disabled = row['is_disabled']

                # Verificar si el registro ya existe en la tabla DimJobCategory
                query_check = f"""
                    SELECT COUNT(*) FROM {schema}.dim_job_category
                    WHERE code = %s
                """
                
                try:
                    result = conn.execute(query_check, (code)).scalar()
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query dim_job_category REDSHIFT: {e}")
                    raise

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
                    SET name = %s, job_family = %s, last_modified = %s,
                        is_disabled = %s
                    WHERE code = %s
                """
                
                try:
                    conn.execute(query_update, update_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query UPDATE dim_job_category REDSHIFT: {e}")
                    raise

                logger.info(f"{len(update_queries)} registros actualizados en el lote {start // batch_size + 1}.")

            # Ejecutar todos los inserts en una sola operación
            if insert_queries:
                query_insert = f"""
                    INSERT INTO {schema}.dim_job_category (code, name, job_family,
                                                        last_modified, is_disabled)
                    VALUES (%s, %s, %s, %s, %s)
                """
                
                try:
                    conn.execute(query_insert, insert_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query INSERT dim_job_category REDSHIFT: {e}")
                    raise

                logger.info(f"{len(insert_queries)} registros insertados en el lote {start // batch_size + 1}.")
    logger.info("Proceso de UPSERT para la dimension JOB CATEGORY completado.")


def load_position_types_redshift(df: pd.DataFrame, batch_size: int = 50):
    """
    Carga los datos en Redshift con la lógica de UPSERT
    (actualizar si existe, insertar si no).
    Dimension: POSITION TYPE

    Arguments:
    df : pd.DataFrame : El DataFrame con los datos a cargar.
    """

    logger.info("Se inicia Proceso de UPSERT para la dimension POSITION TYPE")
    # Crear la conexión a Redshift
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    engine = create_engine(redshift_conn_string)

    # # Listas para almacenar los datos de actualización e inserción
    # update_queries = []
    # insert_queries = []

    # Iniciar la conexión y transacción
    with engine.connect() as conn:
        total_rows = len(df)
        logger.info(f'Total de registros a tratar: {total_rows}')
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            df_batch = df.iloc[start:end]
        
        
            # Listas para almacenar los datos de actualización e inserción
            update_queries = []
            insert_queries = []
            for _, row in df_batch.iterrows():
                code = row['code']
                name = row['name']
                last_modified = row['last_modified']
                is_disabled = row['is_disabled']

                # Verificar si el registro ya existe en la tabla DimJobCategory
                query_check = f"""
                    SELECT COUNT(*) FROM {schema}.dim_position_type
                    WHERE code = %s
                """
                
                try:
                    result = conn.execute(query_check, (code)).scalar()
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query dim_position_type REDSHIFT: {e}")
                    raise

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
                
                try:
                    conn.execute(query_update, update_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query UPDATE dim_position_type REDSHIFT: {e}")
                    raise

                logger.info(f"{len(update_queries)} registros actualizados en el lote {start // batch_size + 1}.")

            # Ejecutar todos los inserts en una sola operación
            if insert_queries:
                query_insert = f"""
                    INSERT INTO {schema}.dim_position_type (code, name,
                                            last_modified, is_disabled)
                    VALUES (%s, %s, %s, %s)
                """
                
                try:
                    conn.execute(query_insert, insert_queries)
                except Exception as e:
                    # Captura errores inesperados y los propaga
                    logging.error(f"Error inesperado en query INSERT dim_position_type REDSHIFT: {e}")
                    raise

                logger.info(f"{len(insert_queries)} registros insertados en el lote {start // batch_size + 1}.")
    logger.info("Proceso de UPSERT para la dimension POSITION TYPE completado.")
