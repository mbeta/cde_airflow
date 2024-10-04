from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()

def get_redshift_connection():
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    if not redshift_conn_string:
            raise ValueError("La variable de entorno 'REDSHIFT_CONN_STRING' no está definida o está vacía.")
        
    engine = create_engine(redshift_conn_string)
    return engine.connect()

def create_schema_jobs():
    redshift_conn_string = os.getenv('REDSHIFT_CONN_STRING')
    engine = create_engine(redshift_conn_string)
    
    create_table_query = """
    CREATE TABLE IF NOT EXISTS jobs (
        position_id VARCHAR(255),
        title VARCHAR(255),
        location VARCHAR(255),
        organization_name VARCHAR(255),
        salary_min NUMERIC,
        salary_max NUMERIC,
        start_date TIMESTAMP,
        end_date TIMESTAMP,
        PRIMARY KEY (position_id)
    );
    """
    
    with engine.connect() as connection:
        connection.execute(create_table_query)
        
    print("Esquema de la base de datos creado con éxito.")

def create_schema_dim_organization():
    # SQL para crear la tabla DimOrganization
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.DimOrganization (
        Code VARCHAR(10) PRIMARY KEY,
        Name VARCHAR(255) NOT NULL,
        ParentCode VARCHAR(10),
        LastModified TIMESTAMP,
        IsDisabled VARCHAR(3)
    );
    """
    
    # Conexión a Redshift
    conn = get_redshift_connection()
    
    # Ejecutar el SQL de creación del esquema
    conn.execute(create_table_sql)
    
    print("Tabla 'DimOrganization' creada o ya existía.")
    
    # Cerrar la conexión
    conn.close()
    
    
def create_schema_dim_category():
    # SQL para crear la tabla DimOrganization
    schema = f'"{os.getenv('REDSHIFT_SCHEMA')}"'
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.DimJobCategory (
        Code VARCHAR(10) PRIMARY KEY,
        Name VARCHAR(255) NOT NULL,
        JobFamily VARCHAR(10),
        LastModified TIMESTAMP,
        IsDisabled VARCHAR(3)
    );
    """
    
    # Conexión a Redshift
    conn = get_redshift_connection()
    
    # Ejecutar el SQL de creación del esquema
    conn.execute(create_table_sql)
    
    print("Tabla 'DimJobCategory' creada o ya existía.")
    
    # Cerrar la conexión
    conn.close()    
    
if __name__ == "__main__":
    #create_schema_dim_organization()
    create_schema_dim_category()