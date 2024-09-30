import pandas as pd
from datetime import datetime

def transform_jobs_data(parquet_file: str):
    """
    ETAPA TRANSFORM
    Transforma los datos de un archivo Parquet a un formato adecuado para el modelo de datos.
    Tabla de Hechos: FactJobPostings
    Dimensiones: 
    - DimLocation
    - DimOrganization
    - DimJobCategory
    
    Arguments:
    parquet_file : str : Ruta del archivo Parquet a transformar.
    
    Returns:
    dict : Un diccionario con DataFrames para la tabla de hechos y las dimensiones.
    """
    
    # Leer el archivo Parquet
    df = pd.read_parquet(parquet_file)
    print("Columnas en el DataFrame:", df.columns.tolist())
    
   # Preparar la tabla de hechos: FactJobPostings
    fact_job_postings = pd.DataFrame({
        'JobPostingID': df['MatchedObjectId'],
        'PositionID': df['PositionID'],
        'MatchedObjectId': df['MatchedObjectId'],
        'LocationID': None,  # Placeholder para el ID de ubicación
        'OrganizationID': None,  # Placeholder para el ID de organización
        'JobCategoryID': None,  # Placeholder para el ID de categoría de trabajo
        'PositionStartDate': pd.to_datetime(df['PositionStartDate']),
        'PositionEndDate': pd.to_datetime(df['PositionEndDate']),
        'PublicationStartDate': pd.to_datetime(df['PublicationStartDate']),
        'ApplicationCloseDate': pd.to_datetime(df['ApplicationCloseDate']),
        'MinimumRange': df['MinSalary'],
        'MaximumRange': df['MaxSalary'],
        'RateIntervalCode': df['RateIntervalCode'],
        'TotalOpenings': df['TotalOpenings'],
        'VersionDate': datetime.now(),
        'IsReposted': 0,  # Asignar 0 o 1 según la lógica de reposteo
        'Duration': (pd.to_datetime(df['PositionEndDate']) - pd.to_datetime(df['PositionStartDate'])).dt.days
    })

    # Preparar la dimensión: DimLocation
    dim_location = df[['LocationName', 'CountryCode', 'CityName', 'Longitude', 'Latitude']].drop_duplicates().reset_index(drop=True)
    dim_location['LocationID'] = dim_location.index + 1  # Generar ID para ubicación
    dim_location = dim_location[['LocationID', 'LocationName', 'CountryCode', 'CityName', 'Longitude', 'Latitude']]

    # Preparar la dimensión: DimOrganization
    dim_organization = df[['OrganizationName', 'DepartmentName']].drop_duplicates().reset_index(drop=True)
    dim_organization['OrganizationID'] = dim_organization.index + 1  # Generar ID para organización
    dim_organization = dim_organization[['OrganizationID', 'OrganizationName', 'DepartmentName']]

    # Preparar la dimensión: DimJobCategory
    dim_job_category = df[['JobCategoryName', 'JobCategoryCode']].drop_duplicates().reset_index(drop=True)
    dim_job_category['JobCategoryID'] = dim_job_category.index + 1  # Generar ID para categoría de trabajo
    dim_job_category = dim_job_category[['JobCategoryID', 'JobCategoryName', 'JobCategoryCode']]

    # Mapear IDs en la tabla de hechos
    fact_job_postings['LocationID'] = fact_job_postings['LocationID'].map(dim_location.set_index('LocationName')['LocationID'])
    fact_job_postings['OrganizationID'] = fact_job_postings['OrganizationID'].map(dim_organization.set_index('OrganizationName')['OrganizationID'])
    fact_job_postings['JobCategoryID'] = fact_job_postings['JobCategoryID'].map(dim_job_category.set_index('JobCategoryName')['JobCategoryID'])

    return {
        'fact_job_postings': fact_job_postings,
        'dim_location': dim_location,
        'dim_organization': dim_organization,
        'dim_job_category': dim_job_category
    }


if __name__ == "__main__":
    # Probar el script con un archivo Parquet
    parquet_file = 'data_temp/2024-09-30_19-53-35_jobs_data.parquet'
    transformed_data = transform_jobs_data(parquet_file)
    
    # Visualizar las primeras filas de cada DataFrame
    for table_name, df in transformed_data.items():
        print(f"\nTabla {table_name} (primeras 5 filas):\n", df.head())