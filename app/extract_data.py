import os
import pandas as pd
from request_api import fetch_all_pages, fetch_organizations, fetch_job_categories
from datetime import datetime

def extract_data_jobs(keyword: str, output_parquet: str, date_posted: int = 0):
    """
    ETAPA EXTRACT JOBS
    Extrae datos de la API de USAJobs y los guarda en formato Parquet.

    Arguments:
    keyword : str : Palabra clave para la búsqueda de trabajos.
    output_parquet : str : Directorio donde se guardará el archivo Parquet.
    date_posted : int : Días desde que se publicó la oferta (0 para hoy, 60 para el máximo histórico).
    """
    # Llamar a la función fetch_all_pages para obtener los datos
    data = fetch_all_pages(keyword, date_posted)

    if data:
        # Crear una lista para almacenar los datos de los trabajos
        jobs = []
        
        for item in data:
            matched_descriptor = item['MatchedObjectDescriptor']
            matched_object_id = item['MatchedObjectId']
            
            # Extraemos data de la descripción del item
            position_id = matched_descriptor['PositionID']
            position_title = matched_descriptor['PositionTitle']
            organization_name = matched_descriptor['OrganizationName']
            department_name = matched_descriptor['DepartmentName']
            location = matched_descriptor['PositionLocation'][0] if matched_descriptor.get('PositionLocation') else {}
            location_name = location.get('LocationName')
            country_code = location.get('CountryCode')
            city_name = location.get('CityName')
            longitude = location.get('Longitude')
            latitude = location.get('Latitude')
            

            publication_start_date = matched_descriptor.get('PublicationStartDate', None)
            application_close_date = matched_descriptor.get('ApplicationCloseDate', None)
            position_start_date = matched_descriptor.get('PositionStartDate', None)
            position_end_date = matched_descriptor.get('PositionEndDate', None)
            
            position_remuneration = matched_descriptor['PositionRemuneration'][0] if matched_descriptor.get('PositionRemuneration') else {}
            min_salary = position_remuneration.get('MinimumRange', None)
            max_salary = position_remuneration.get('MaximumRange', None)
            rate_interval_code = position_remuneration.get('RateIntervalCode', None)
            
            user_area = matched_descriptor['UserArea']
            total_openings = user_area.get('Details', None).get('TotalOpenings', None)
            

            job_category = matched_descriptor['JobCategory'][0] if matched_descriptor.get('JobCategory') else {}
            job_category_name = job_category.get('Name')
            job_category_code = job_category.get('Code')

            jobs.append({
                'MatchedObjectId': matched_object_id,
                'PositionID': position_id,
                'PositionTitle': position_title,
                'OrganizationName': organization_name,
                'DepartmentName' : department_name,
                'LocationName': location_name,
                'CountryCode': country_code,
                'CityName': city_name,
                'Longitude': longitude,
                'Latitude': latitude,
                'MinSalary': min_salary,
                'MaxSalary': max_salary,
                'RateIntervalCode' : rate_interval_code,
                'PublicationStartDate' : publication_start_date,
                'ApplicationCloseDate' : application_close_date,
                'PositionStartDate': position_start_date,
                'PositionEndDate': position_end_date,
                'TotalOpenings' : total_openings,
                'JobCategoryName': job_category_name,
                'JobCategoryCode': job_category_code
            })

        # Convertir la lista de resultados a un DataFrame de pandas
        df = pd.DataFrame(jobs)

        # Verificar si el directorio existe, si no, crearlo
        if not os.path.exists(output_parquet):
            os.makedirs(output_parquet)
            
        # Obtener la fecha actual y formatearla como YYYY-MM-DD
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Guardar los datos en un archivo Parquet con la fecha y hora en el nombre
        path = os.path.join(output_parquet, f'{timestamp}_jobs_data.parquet')
        df.to_parquet(path, index=False)
        print(f"Datos extraídos y guardados en {path}")
        return path
    else:
        print("No se han recibido datos.")
        return None
    
    
def extract_data_organization(lastmodified: str, output_parquet: str):
    """
    ETAPA EXTRACT
    Extrae datos de Orgaizaciones de la API de USAJobs y los guarda en formato Parquet.

    Arguments:
    lastmodified : str : Filtro de fecha formato YYYY-MM-DD para ultimas modificaciones.
    """
    # Llamar a la función fetch_organizations para obtener los datos
    data = fetch_organizations(lastmodified)
    print(f"Cantidad de Registros: {data}")

    if data:
        # Crear una lista para almacenar los datos de la dimension de organizaciones
        organizations = []
        
        for item in data:
            code = item.get('Code',None)
            value = item.get('Value',None)
            parent_code = item.get('ParentCode',None)
            last_modified = item.get('LastModified',None)
            id_disabled = item.get('IsDisabled',None)
            
                      
            organizations.append({
                'Code': code,
                'Value': value,
                'ParentCode': parent_code,
                'LastModified' : last_modified,
                'IsDisabled': id_disabled
            })

        # Convertir la lista de resultados a un DataFrame de pandas
        df = pd.DataFrame(organizations)

        # Verificar si el directorio existe, si no, crearlo
        if not os.path.exists(output_parquet):
            os.makedirs(output_parquet)
            
        # Obtener la fecha actual y formatearla como YYYY-MM-DD
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Guardar los datos en un archivo Parquet con la fecha y hora en el nombre
        path = os.path.join(output_parquet, f'{timestamp}_organizations_data.parquet')
        df.to_parquet(path, index=False)
        print(f"Datos extraídos y guardados en {path}")
        return path
    else:
        print("No se han recibido datos.")
        return None    

def extract_data_job_categories(lastmodified: str, output_parquet: str):
    """
    ETAPA EXTRACT
    Extrae datos de Categorias de la API de USAJobs y los guarda en formato Parquet.

    Arguments:
    lastmodified : str : Filtro de fecha formato YYYY-MM-DD para ultimas modificaciones.
    """
    # Llamar a la función fetch_job_categories para obtener los datos
    data = fetch_job_categories(lastmodified)
    
    if data:
        # Crear una lista para almacenar los datos de la dimension de categorias
        job_categories = []
        
        for item in data:
            code = item.get('Code',None)
            value = item.get('Value',None)
            job_family = item.get('JobFamily',None)
            last_modified = item.get('LastModified',None)
            id_disabled = item.get('IsDisabled',None)
            
                      
            job_categories.append({
                'Code': code,
                'Value': value,
                'JobFamily': job_family,
                'LastModified' : last_modified,
                'IsDisabled': id_disabled
            })

        # Convertir la lista de resultados a un DataFrame de pandas
        df = pd.DataFrame(job_categories)

        # Verificar si el directorio existe, si no, crearlo
        if not os.path.exists(output_parquet):
            os.makedirs(output_parquet)
            
        # Obtener la fecha actual y formatearla como YYYY-MM-DD
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Guardar los datos en un archivo Parquet con la fecha y hora en el nombre
        path = os.path.join(output_parquet, f'{timestamp}_job_categories_data.parquet')
        df.to_parquet(path, index=False)
        print(f"Datos extraídos y guardados en {path}")
        return path
    else:
        print("No se han recibido datos.")
        return None    

if __name__ == "__main__":
    # Prueba de uso
    output_directory = 'data_temp'
    #extract_data_jobs('Software', output_directory, date_posted=60)
    #extract_data_organization('', output_directory)
    extract_data_job_categories('', output_directory)
