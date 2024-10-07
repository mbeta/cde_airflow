import requests
import os
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

def build_headers():
    """
    Construye los headers de la solicitud a partir de las variables de entorno.
    """
    headers = {
        'User-Agent': os.getenv('USER_AGENT'),
        'Authorization-Key': os.getenv('AUTHORIZATION_KEY'),
        'Host': os.getenv('HOST')
    }
    return headers

def get_data_jobs(keyword: str, page: int, date_posted: int = 0) -> dict:
    """
    Realiza una solicitud GET a la API de USAJobs según los parámetros especificados.

    Arguments:
    keyword : str : Palabra clave para la búsqueda de trabajos.
    page: int : Número de página de resultados (1 a n).
    date_posted: int : Días desde que se publicó la oferta (0 para hoy, 60 para el máximo histórico).

    Returns:
    dict : Respuesta en formato JSON o None si hay un error.
    """
    url = f'https://data.usajobs.gov/api/search?Keyword={keyword}&DatePosted={date_posted}&Page={page}&ResultsPerPage=500'
    headers = build_headers()
    
    print(f"Consultando la página {page} de resultados...")  # Debug
    print(f"URL: {url}")  # Debug

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Lanza una excepción si el status_code no es 200
        return response.json()
    except requests.exceptions.Timeout: # Captura exceptions tipo Timeout
        print("Error: La solicitud ha superado el tiempo de espera.")
    except requests.exceptions.RequestException as e: # Captura exceptions de Request
        print(f"Error en la solicitud: {e}")
    
    return None

def fetch_all_pages(keyword: str, date_posted: int = 0):
    """
    Obtiene todos los resultados de la API, manejando la paginación automáticamente.

    Arguments:
    keyword : str : Palabra clave para la búsqueda de trabajos.
    date_posted: int : Días desde que se publicó la oferta (0 para hoy, 60 para el máximo histórico).

    Returns:
    list : Lista con todas las respuestas JSON de cada página.
    """
    all_results = []
    current_page = 1
    
    response = get_data_jobs(keyword, current_page, date_posted)
    
    # Recupera el numero total de paginas
    total_pages = int(response['SearchResult']['UserArea']['NumberOfPages'])
    print(f"Paginas totales a consultar: {total_pages}")
    
    # Se acumula resultado de primera pagina
    all_results.extend(response['SearchResult']['SearchResultItems'])
    
    # Recupera paginas faltantes
    while current_page < total_pages:
        current_page += 1
        response = get_data_jobs(keyword, current_page, date_posted)
        all_results.extend(response['SearchResult']['SearchResultItems'])
    
    # Retorna resultado de todas las paginas
    return all_results


def get_data_jobs_categories(lastmodified: str) -> dict:
    """
    Realiza una solicitud GET a la API de USAJobs para obtener los diferentes JobCategoryCode.
    https://developer.usajobs.gov/api-reference/get-codelist-occupationalseries

    Arguments:
    lastmodified : str : Fecha de ultima modificacion, con la que solo devuelve registros que hayan sido modificados en esta fecha o posterior

    Returns:
    dict : Respuesta en formato JSON o None si hay un error.
    """
    url = f'https://data.usajobs.gov/api/codelist/occupationalseries?lastmodified={lastmodified}'
    headers = build_headers()
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Lanza una excepción si el status_code no es 200
        return response.json()
    except requests.exceptions.Timeout: # Captura exceptions tipo Timeout
        print("Error: La solicitud de JobCategory ha superado el tiempo de espera.")
    except requests.exceptions.RequestException as e: # Captura exceptions de Request
        print(f"Error en la solicitud: {e}")
    
    return None

def get_data_organization(lastmodified: str) -> dict:
    """
    Realiza una solicitud GET a la API de USAJobs para obtener Organizations.
    https://developer.usajobs.gov/api-reference/get-codelist-agencysubelements
    
    Arguments:
    lastmodified : str : Fecha de ultima modificacion, con la que solo devuelve registros que hayan sido modificados en esta fecha o posterior

    Returns:
    dict : Respuesta en formato JSON o None si hay un error.
    """
    url = f'https://data.usajobs.gov/api/codelist/agencysubelements?lastmodified={lastmodified}'
    headers = build_headers()
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Lanza una excepción si el status_code no es 200
        return response.json()
    except requests.exceptions.Timeout: # Captura exceptions tipo Timeout
        print("Error: La solicitud de JobCategory ha superado el tiempo de espera.")
    except requests.exceptions.RequestException as e: # Captura exceptions de Request
        print(f"Error en la solicitud: {e}")
    
    return None


def fetch_organizations(lastmodified: str):
    """
    Obtiene todos los resultados de la API de Organizations.

    Arguments:
    lastmodified : str : Fecha de ultima modificacion, con la que solo devuelve registros que hayan sido modificados en esta fecha o posterior

    Returns:
    list : Lista con todas las Organizations en JSON.
    """
      
    response = get_data_organization(lastmodified)
       
    # Retorna resultado de solo  listado de organizaciones.
    return response['CodeList'][0]['ValidValue']

def fetch_job_categories(lastmodified: str):
    """
    Obtiene todos los resultados de la API de Jobs Category.
    
    Arguments:
    lastmodified : str : Fecha de ultima modificacion, con la que solo devuelve registros que hayan sido modificados en esta fecha o posterior

    Returns:
    list : Lista con todas las Categorias  resultantes en JSON.
    """
      
    response = get_data_jobs_categories(lastmodified)
       
    # Retorna resultado de solo  listado de categorias.
    return response['CodeList'][0]['ValidValue']


if __name__ == "__main__":
    # Ejemplo de consulta diaria
    jobs_today = fetch_all_pages('Software', date_posted=0)
    if jobs_today:
        print(f"Se encontraron {len(jobs_today)} trabajos publicados hoy.")

    # Ejemplo de consulta histórica (últimos 60 días)
    jobs_history = fetch_all_pages('Software', date_posted=60)
    if jobs_history:
        print(f"Se encontraron {len(jobs_history)} trabajos publicados en los últimos 60 días.")
