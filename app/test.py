from transform_data import transform_data_jobs, transform_data_organization, transform_data_category, transform_data_position_types
from load_to_redshift import load_jobs_redshift, load_organization_redshift, load_categories_redshift, load_position_types_redshift


if __name__ == "__main__":
        
        
    # #Pruebas con dimension categories
    # parquet_file = 'data_temp/2024-10-06_11-08-38_job_categories_data.parquet'   
    # #Transformar los datos
    # dim_job_categories_df = transform_data_category(parquet_file)
    # print(dim_job_categories_df.head())
    # #Cargar los datos en Redshift
    # load_categories_redshift(dim_job_categories_df)
    
    
    # #Pruebas con dimension position types
    # parquet_file = 'data_temp/2024-10-06_11-08-38_position_types_data.parquet'   
    # # Transformar los datos
    # dim_position_type_df = transform_data_position_types(parquet_file)
    # print(dim_position_type_df.head())
    # # Cargar los datos en Redshift
    # load_position_types_redshift(dim_position_type_df)
    
    
    #  #Pruebas con dimension organizations
    # parquet_file = 'data_temp/2024-10-06_11-08-37_organizations_data.parquet'   
    # # Transformar los datos
    # dim_organizations_df = transform_data_organization(parquet_file)
    # print(dim_organizations_df.head())
    # # Cargar los datos en Redshift
    # load_organization_redshift(dim_organizations_df)
    
     #Pruebas jobs
    parquet_file = 'data_temp/2024-10-06_11-34-00_jobs_data.parquet'   
    # Transformar los datos
    jobs_df = transform_data_jobs(parquet_file)
    print(jobs_df.head())
    # Cargar los datos en Redshift
    load_jobs_redshift(jobs_df.head())