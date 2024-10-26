import logging
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()


def get_redshift_connection():
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    if not redshift_conn_string:
        logger.error("'REDSHIFT_CONN_STRING' no está definida o está vacía.")
        raise ValueError("'REDSHIFT_CONN_STRING' no está definida o está vacía.")
    
    engine = create_engine(redshift_conn_string)
    conn = engine.connect()
    logger.info("Conexión exitosa a Redshift.")
    return conn

def create_table(table_name, create_table_sql):
    """Función genérica para crear una tabla."""
    schema = f'"{os.getenv("REDSHIFT_SCHEMA")}"'
    create_table_sql = create_table_sql.format(schema=schema)
    conn = get_redshift_connection()
    
    conn.execute(create_table_sql)
    logger.info(f"Tabla '{table_name}' creada o ya existía.")
    conn.close()


def create_all_tables():
    """Crear todas las tablas del esquema."""
    tables = {
        'jobs': """
            CREATE TABLE IF NOT EXISTS {schema}.jobs (
                object_id VARCHAR(255) PRIMARY KEY,
                position_id VARCHAR(50),
                position_title VARCHAR(255),
                location_description VARCHAR(255),
                organization_code VARCHAR(10),
                department_code VARCHAR(10),
                job_category_code VARCHAR(10),
                position_start_date TIMESTAMP,
                position_end_date TIMESTAMP,
                publication_start_date TIMESTAMP,
                application_close_date TIMESTAMP,
                minimum_salary NUMERIC,
                maximum_salary NUMERIC,
                rate_interval_description VARCHAR(50),
                position_type_code VARCHAR(10),
                detail_position_type VARCHAR(255),
                version_date TIMESTAMP,
                is_reposted NUMERIC,
                duration NUMERIC
            );
        """,
        'dim_organization': """
            CREATE TABLE IF NOT EXISTS {schema}.dim_organization (
                code VARCHAR(10) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                parent_code VARCHAR(10),
                last_modified TIMESTAMP,
                is_disabled VARCHAR(3)
            );
        """,
        'dim_job_category': """
            CREATE TABLE IF NOT EXISTS {schema}.dim_job_category (
                code VARCHAR(10) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                job_family VARCHAR(10),
                last_modified TIMESTAMP,
                is_disabled VARCHAR(3)
            );
        """,
        'dim_position_type': """
            CREATE TABLE IF NOT EXISTS {schema}.dim_position_type (
                code VARCHAR(10) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                last_modified TIMESTAMP,
                is_disabled VARCHAR(3)
            );
        """
    }

    for table_name, create_sql in tables.items():
        create_table(table_name, create_sql)


def drop_schema():
    """
    Se eliminan toda las tablas del schema.
    Arguments:- 
    Returns: -
    """
    schema = f'{os.getenv("REDSHIFT_SCHEMA")}'
    get_tables_sql = f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}';
    """
    conn = get_redshift_connection()
    
    result = conn.execute(get_tables_sql)
    tables = result.fetchall()

    if not tables:
        logger.info(f"No hay tablas en el esquema '{schema}'.")
        return

    # Eliminar cada tabla
    for table in tables:
        drop_table_sql = f"DROP TABLE IF EXISTS \"{schema}\".{table[0]} CASCADE;"
        logger.info(f"Ejecutando: {drop_table_sql}")
        conn.execute(drop_table_sql)
        logger.info(f"Tabla '{table[0]}' eliminada exitosamente.")
    conn.close()


def get_organization_codes_by_names(names):
    """
    Obtiene los códigos de organización basados en una lista de nombres.
    Arguments:
    names : list : Lista de nombres de organizaciones a buscar.
    Returns:
    dict : Un diccionario con los nombres como claves y
    sus respectivos códigos como valores.
    """
    conn = get_redshift_connection()
    schema = f'"{os.getenv("REDSHIFT_SCHEMA")}"'
    # construimos query
    names_placeholder = ', '.join([f"'{name}'" for name in names])
    query = f"""
        SELECT code, name
        FROM {schema}.dim_organization
        WHERE name IN ({names_placeholder})
    """
    # Ejecutamos la consulta
    result = conn.execute(query)
    organization_map = {row['name']: row['code'] for row in result}
    logger.info(f"Códigos de organización obtenidos para los nombres: {names}")
    
    conn.close()
    
    return organization_map
