import os
import pandas as pd
from request_api import fetch_all_pages, fetch_organizations, fetch_job_categories, fetch_position_types
from datetime import datetime


def extract_data_jobs(keyword: str, output_parquet: str,
                      date_posted: int = 0, date_control: str = None):
    """
    ETAPA EXTRACT JOBS
    Extrae datos de la API de USAJobs y los guarda en formato Parquet.

    Arguments:
    keyword : str : Palabra clave para la búsqueda de trabajos.
    output_parquet : str : Directorio donde se guardará el archivo Parquet.
    date_posted : int : Días desde que se publicó la oferta
                (0 para hoy, 60 para el máximo histórico).
                
    date_control: Fecha limite de control, para evitar procesar
                    dias incompletos. Se listan registros con fecha de publicación
                    < a la fecha de control. (YYYY-MM-DD)
                    
    Returns:
    str : Ruta completa del archivo Parquet generado.
    """
    # Llamar a la función fetch_all_pages para obtener los datos
    data = fetch_all_pages(keyword, date_posted)

    
    if data:
        # Crear una lista para almacenar los datos de los trabajos
        jobs = []
        print(f"Cantidad de registros a procesar: {len(data)}")
        if date_control:
            date_control = datetime.strptime(date_control, '%Y-%m-%d').date()
        for item in data:
            matched_descriptor = item['MatchedObjectDescriptor']
            # Control de fecha
            publication_start_date = matched_descriptor.get(
                'PublicationStartDate', None)
            # Verificar que la fecha de publicación exista y
            # convertirla a un objeto datetime
            if publication_start_date:
                publication_start_date_control = datetime.strptime(
                    publication_start_date.split('T')[0], '%Y-%m-%d').date()
                # Si la publication_start_date_control es mayor a la de date_control,
                # ignorar este registro
                if date_control and \
                   publication_start_date_control > date_control:
                        continue

            # Extraemos data de la descripción del item
            position_id = matched_descriptor['PositionID']
            matched_object_id = item['MatchedObjectId']
            position_title = matched_descriptor['PositionTitle']

            # Posterior relacion con dimension Organizations
            organization_name = matched_descriptor['OrganizationName']
            department_name = matched_descriptor['DepartmentName']
            location_display = matched_descriptor['PositionLocationDisplay']

            # Informacion de fechas de la publicación
            publication_start_date = matched_descriptor.get(
                'PublicationStartDate', None)
            application_close_date = matched_descriptor.get(
                'ApplicationCloseDate', None)
            position_start_date = matched_descriptor.get(
                'PositionStartDate', None)
            position_end_date = matched_descriptor.get(
                'PositionEndDate', None)

            position_remuneration = matched_descriptor[
                'PositionRemuneration'][0] if matched_descriptor.get(
                    'PositionRemuneration') else {}
            min_salary = position_remuneration.get(
                'MinimumRange', None)
            max_salary = position_remuneration.get(
                'MaximumRange', None)
            rate_interval_description = position_remuneration.get(
                'Description', None)

            # Pueden presentarse mas de una categoria por posteo
            job_categories = matched_descriptor.get('JobCategory')

            # Creo un registro por cada categoria encontrada
            for job_category in job_categories:
                job_category_code = job_category.get('Code')

                position_types = matched_descriptor['PositionOfferingType']
                # Creo registros por cada combinacion de tipos
                for position_type in position_types:
                    position_type_code = position_type.get('Code', None)
                    position_type_detail = position_type.get('Name', None)
                    jobs.append({
                        'MatchedObjectId': matched_object_id,
                        'PositionID': position_id,
                        'PositionTitle': position_title,
                        'PositionLocationDisplay': location_display,
                        'OrganizationName': organization_name,
                        'DepartmentName': department_name,
                        'JobCategoryCode': job_category_code,
                        'PositionStartDate': position_start_date,
                        'PositionEndDate': position_end_date,
                        'PublicationStartDate': publication_start_date,
                        'ApplicationCloseDate': application_close_date,
                        'MinSalary': min_salary,
                        'MaxSalary': max_salary,
                        'RateIntervalDescription': rate_interval_description,
                        'PositionTypeCode': position_type_code,
                        'DetailPositionType': position_type_detail
                    })

        if len(jobs) == 0:
            print(f'No se encontraron registros a transformar')
            return None
        else:
            print(f'Se toman {len(jobs)} registros a transformar')
            # Convertir la lista de resultados a un DataFrame de pandas
            df = pd.DataFrame(jobs)

            # Verificar si el directorio existe, si no, crearlo
            if not os.path.exists(output_parquet):
                os.makedirs(output_parquet)

            # Obtener la fecha actual y formatearla como YYYY-MM-DD
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

            # Guardar los datos en un archivo Parquet con la fecha
            # y hora en el nombre
            path = os.path.join(output_parquet, f'{timestamp}_jobs_data.parquet')
            df.to_parquet(path, index=False)
            print(f"Datos extraídos y guardados en {path}")
            return path
    else:
        print("No se han recibido datos.")
        return None


def extract_data_organization(lastmodified: str, output_parquet: str):
    """
    ETAPA EXTRACT - Dimensión ORGANIZATION
    Extrae datos de Orgaizaciones de la API de USAJobs y los
    guarda en formato Parquet.

    Arguments:
    lastmodified : str : Filtro de fecha formato YYYY-MM-DD
    para ultimas modificaciones.
    output_parquet : str : Ruta del directorio donde se guardará el archivo Parquet.
    
    Returns:
    str : Ruta completa del archivo Parquet generado.
    """
    # Llamar a la función fetch_organizations para obtener los datos
    
    data = fetch_organizations(lastmodified)
    print(f"Cantidad de Registros: {len(data)}")

    if data:
        # Crear una lista para almacenar los datos de la
        # dimension de organizaciones
        organizations = []

        for item in data:
            code = item.get('Code', None)
            value = item.get('Value', None)
            parent_code = item.get('ParentCode', None)
            last_modified = item.get('LastModified', None)
            id_disabled = item.get('IsDisabled', None)
            organizations.append({
                'Code': code,
                'Value': value,
                'ParentCode': parent_code,
                'LastModified': last_modified,
                'IsDisabled': id_disabled
            })

        # Convertir la lista de resultados a un DataFrame de pandas
        df = pd.DataFrame(organizations)

        # Verificar si el directorio existe, si no, crearlo
        if not os.path.exists(output_parquet):
            os.makedirs(output_parquet)

        # Obtener la fecha actual y formatearla como YYYY-MM-DD
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Guardar los datos en un archivo Parquet con la fecha y
        # hora en el nombre
        path = os.path.join(
            output_parquet, f'{timestamp}_organizations_data.parquet')
        df.to_parquet(path, index=False)
        print(f"Datos extraídos y guardados en {path}")
        return path
    else:
        print("No se han recibido datos.")
        return None


def extract_data_job_categories(lastmodified: str, output_parquet: str):
    """
    ETAPA EXTRACT - Dimensión JOB CATEGORY
    Extrae datos de Categorias de la API de USAJobs y los
    guarda en formato Parquet.

    Arguments:
    lastmodified : str : Filtro de fecha formato YYYY-MM-DD
    para ultimas modificaciones.
    output_parquet : str : Ruta del directorio donde se guardará el archivo Parquet.
    
    Returns:
    str : Ruta completa del archivo Parquet generado.
    """
    # Llamar a la función fetch_job_categories para obtener los datos
    data = fetch_job_categories(lastmodified)
    if data:
        # Crear una lista para almacenar los datos de la
        # dimension de categorias
        job_categories = []
        for item in data:
            code = item.get('Code', None)
            value = item.get('Value', None)
            job_family = item.get('JobFamily', None)
            last_modified = item.get('LastModified', None)
            id_disabled = item.get('IsDisabled', None)
            job_categories.append({
                'Code': code,
                'Value': value,
                'JobFamily': job_family,
                'LastModified': last_modified,
                'IsDisabled': id_disabled
            })

        # Convertir la lista de resultados a un DataFrame de pandas
        df = pd.DataFrame(job_categories)

        # Verificar si el directorio existe, si no, crearlo
        if not os.path.exists(output_parquet):
            os.makedirs(output_parquet)

        # Obtener la fecha actual y formatearla como YYYY-MM-DD
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Guardar los datos en un archivo Parquet con la fecha y
        # hora en el nombre
        path = os.path.join(
            output_parquet, f'{timestamp}_job_categories_data.parquet')
        df.to_parquet(path, index=False)
        print(f"Datos extraídos y guardados en {path}")
        return path
    else:
        print("No se han recibido datos.")
        return None


def extract_data_position_type(lastmodified: str, output_parquet: str):
    """
    ETAPA EXTRACT - Dimensión POSITION TYPE
    Extrae datos de los tipos de posiciones de la API de USAJobs y los
    guarda en formato Parquet.

    Arguments:
    lastmodified : str : Filtro de fecha formato YYYY-MM-DD para
    ultimas modificaciones.
    output_parquet : str : Ruta del directorio donde se guardará el archivo Parquet.
    
    Returns:
    str : Ruta completa del archivo Parquet generado.
    """
    # Llamar a la función fetch_position_types para obtener los datos
    data = fetch_position_types(lastmodified)

    if data:
        # Crear una lista para almacenar los datos de la dimension
        # de tipos de posiciones
        position_types = []
        for item in data:
            code = item.get('Code', None)
            value = item.get('Value', None)
            last_modified = item.get('LastModified', None)
            id_disabled = item.get('IsDisabled', None)
            position_types.append({
                'Code': code,
                'Value': value,
                'LastModified': last_modified,
                'IsDisabled': id_disabled
            })

        # Convertir la lista de resultados a un DataFrame de pandas
        df = pd.DataFrame(position_types)

        # Verificar si el directorio existe, si no, crearlo
        if not os.path.exists(output_parquet):
            os.makedirs(output_parquet)

        # Obtener la fecha actual y formatearla como YYYY-MM-DD
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

        # Guardar los datos en un archivo Parquet con la fecha y
        # hora en el nombre
        path = os.path.join(
            output_parquet, f'{timestamp}_position_types_data.parquet')
        df.to_parquet(path, index=False)
        print(f"Datos extraídos y guardados en {path}")
        return path
    else:
        print("No se han recibido datos.")
        return None


if __name__ == '__main__':
    parquet_file = 'data_temp'
    exract_data = extract_data_job_categories('',parquet_file)