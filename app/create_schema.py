from sqlalchemy import create_engine
import os

def create_schema():
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
