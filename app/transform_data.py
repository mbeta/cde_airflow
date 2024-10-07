import pandas as pd
from datetime import datetime
from db_services import get_organization_codes_by_names

def transform_data_jobs(parquet_file: str):
    """
    ETAPA TRANSFORM
    Transforma los datos de un archivo Parquet a un formato adecuado para el modelo de datos.
    Tabla de Hechos: FactJobPostings
    
    Arguments:
    parquet_file : str : Ruta del archivo Parquet a transformar.
    
    Returns:
    dict : Un diccionario con DataFrames para la tabla de hechos y las dimensiones.
    """
    
    # Leer el archivo Parquet
    df = pd.read_parquet(parquet_file)
    print("Columnas en el DataFrame:", df.columns.tolist())
    
    #Buscamos los codigos de organization que necesitaremos
    unique_organization_names = df['OrganizationName'].unique().tolist()
    unique_department_names = df['DepartmentName'].unique().tolist()
    
    # Buscar los códigos en la tabla dim_organization
    all_names = unique_organization_names + unique_department_names
    organization_map = get_organization_codes_by_names(all_names)
    
   # Preparar la tabla de hechos: FactJobPostings
    fact_job_postings = pd.DataFrame({
        'object_id': df['MatchedObjectId'],
        'position_id': df['PositionID'],
        'position_title': df['PositionTitle'],
        'location_description': df['PositionLocationDisplay'],
        'organization_code': df['OrganizationName'].map(organization_map),  # Buscamos code segun OrganizationName
        'department_code': df['DepartmentName'].map(organization_map), # Buscamos code segun DepartmentName
        'job_category_code':df['JobCategoryCode'],
        'position_start_date': pd.to_datetime(df['PositionStartDate'], errors='coerce'),
        'position_end_date': pd.to_datetime(df['PositionEndDate'], errors='coerce'),
        'publication_start_date': pd.to_datetime(df['PublicationStartDate'], errors='coerce'),
        'application_close_date': pd.to_datetime(df['ApplicationCloseDate'], errors='coerce'),
        'minimum_salary': df['MinSalary'].astype(float),
        'maximum_salary': df['MaxSalary'].astype(float),
        'rate_interval_description': df['RateIntervalDescription'],
        'position_type_code': df['PositionTypeCode'],
        'detail_position_type': df['DetailPositionType'],
        'version_date': datetime.now(),
        'duration': (pd.to_datetime(df['PositionEndDate'], errors='coerce') - 
                     pd.to_datetime(df['PositionStartDate'], errors='coerce')).dt.days
    })

   
    return fact_job_postings
    
def transform_data_organization(parquet_file: str):
    """
    ETAPA TRANSFORM - Dimension ORGANIZATION
    Transforma los datos de un archivo Parquet a un formato adecuado para el modelo de datos.
    Tabla de Dimension: DimOrganization
     
    Arguments:
    parquet_file : str : Ruta del archivo Parquet a transformar.
    
    Returns:
    dict : Un diccionario con DataFrames para la tabla de dimension.
    """
    
    # Leer el archivo Parquet
    df = pd.read_parquet(parquet_file)
    print("Columnas en el DataFrame:", df.columns.tolist())
    
   # Preparar la tabla de hechos: FactJobPostings
    dim_organization_postings = pd.DataFrame({
        'code': df['Code'],
        'name': df['Value'],
        'parent_code': df['ParentCode'],
        'last_modified': df['LastModified'],  
        'is_disabled': df['IsDisabled'],  
    })

    return  dim_organization_postings
        
def transform_data_category(parquet_file: str):
    """
    ETAPA TRANSFORM - Dimension JOB CATEGORY
    Transforma los datos de un archivo Parquet a un formato adecuado para el modelo de datos.
    Tabla de Dimension: DimJobCategory
     
    Arguments:
    parquet_file : str : Ruta del archivo Parquet a transformar.
    
    Returns:
    dict : Un diccionario con DataFrames para la tabla de dimension.
    """
    
    # Leer el archivo Parquet
    df = pd.read_parquet(parquet_file)
    print("Columnas en el DataFrame:", df.columns.tolist())
    
   # Preparar la tabla de hechos: FactJobPostings
    dim_job_category_postings = pd.DataFrame({
        'code': df['Code'],
        'name': df['Value'],
        'job_family': df['JobFamily'],
        'last_modified': df['LastModified'],  
        'is_disabled': df['IsDisabled'],  
    })

    return  dim_job_category_postings    

def transform_data_position_types(parquet_file: str):
    """
    ETAPA TRANSFORM - Dimension POSITION TYPE
    Transforma los datos de un archivo Parquet a un formato adecuado para el modelo de datos.
    Tabla de Dimension: DimPositionType
     
    Arguments:
    parquet_file : str : Ruta del archivo Parquet a transformar.
    
    Returns:
    dict : Un diccionario con DataFrames para la tabla de dimension.
    """
    
    # Leer el archivo Parquet
    df = pd.read_parquet(parquet_file)
    print("Columnas en el DataFrame:", df.columns.tolist())
    
   # Preparar la tabla de hechos: FactJobPostings
    dim_job_position_types_postings = pd.DataFrame({
        'code': df['Code'],
        'name': df['Value'],
        'last_modified': df['LastModified'],  
        'is_disabled': df['IsDisabled'],  
    })

    return  dim_job_position_types_postings    


if __name__ == "__main__":
    # Probar el script con un archivo Parquet
    #parquet_file = 'data_temp/2024-09-30_19-53-35_jobs_data.parquet'
    #transformed_data = transform_jobs_data(parquet_file)
    
    # Visualizar las primeras filas de cada DataFrame
    #for table_name, df in transformed_data.items():
    #    print(f"\nTabla {table_name} (primeras 5 filas):\n", df.head())
        
    #Pruebas con dimension organization
    #parquet_file = 'data_temp/2024-10-02_10-27-53_organizations_data.parquet'
    #dim_organization_df = transform_data_organization(parquet_file)
    
    #Pruebas con dimension job category
    parquet_file = 'data_temp/2024-10-02_11-01-11_job_categories_data.parquet'
    dim_organization_df = transform_data_category(parquet_file)
    
    # Imprimir el DataFrame transformado para verificar los resultados
    print(dim_organization_df.head())