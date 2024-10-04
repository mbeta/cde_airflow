from transform_data import transform_data_organization, transform_data_category
from load_to_redshift import load_organization_redshift, load_categories_redshift


if __name__ == "__main__":
        
    #Pruebas con dimension organization
    #parquet_file = 'data_temp/2024-10-02_10-27-53_organizations_data.parquet'   
    # Transformar los datos
    #dim_organization_df = transform_data_organization(parquet_file)
    #print(dim_organization_df.head())
    # Cargar los datos en Redshift
    #load_organization_redshift(dim_organization_df)
    
    #Pruebas con dimension categories
    parquet_file = 'data_temp/2024-10-02_11-01-11_job_categories_data.parquet'   
    # Transformar los datos
    dim_job_category_df = transform_data_category(parquet_file)
    print(dim_job_category_df.head())
    # Cargar los datos en Redshift
    load_categories_redshift(dim_job_category_df)