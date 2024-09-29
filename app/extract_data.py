import os
import pandas as pd
from request_api import fetch_all_pages
from datetime import datetime

def extract_data(keyword: str, output_parquet: str, date_posted: int = 0):
    """
    Extrae datos de la API de USAJobs y los guarda en formato Parquet.

    Arguments:
    keyword : str : Palabra clave para la búsqueda de trabajos.
    output_parquet : str : Directorio donde se guardará el archivo Parquet.
    date_posted : int : Días desde que se publicó la oferta (0 para hoy, 60 para el máximo histórico).
    """
    # Llamar a la función fetch_all_pages para obtener los datos
    data = fetch_all_pages(keyword, date_posted)

    if data:
        # Convertir la lista de resultados a un DataFrame de pandas
        df = pd.json_normalize(data)

        # Verificar si el directorio existe, si no, crearlo
        if not os.path.exists(output_parquet):
            os.makedirs(output_parquet)
            
        # Obtener la fecha actual y formatearla como YYYY-MM-DD
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')

         # Guardar los datos en un archivo Parquet con la fecha y hora en el nombre
        path = os.path.join(output_parquet, f'{timestamp}_jobs_data.parquet')
        df.to_parquet(path, index=False)
        print(f"Datos extraidos y guardados en {path}")
        return path
    else:
        print("No se han recibido datos.")
        return None

if __name__ == "__main__":
    # Prueba de uso
    output_directory = 'data_temp'
    extract_data('Software', output_directory, date_posted=60)
