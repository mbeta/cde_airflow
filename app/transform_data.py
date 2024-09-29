import pandas as pd

def transform_jobs_data(raw_data: list):
    # Crear un DataFrame de los datos en bruto
    df = pd.DataFrame(raw_data)
    
    # Extraer campos relevantes
    df_normalized = pd.json_normalize(df['MatchedObjectDescriptor'])
    
    # Limpiar columnas, eliminar duplicados, etc.
    df_normalized['PositionStartDate'] = pd.to_datetime(df_normalized['PositionStartDate'])
    df_normalized['PositionEndDate'] = pd.to_datetime(df_normalized['PositionEndDate'])
    df_normalized['SalaryMin'] = pd.to_numeric(df_normalized['PositionRemuneration'].apply(lambda x: x[0]['MinimumRange']))
    df_normalized['SalaryMax'] = pd.to_numeric(df_normalized['PositionRemuneration'].apply(lambda x: x[0]['MaximumRange']))
    
    # Otras transformaciones necesarias
    
    return df_normalized
