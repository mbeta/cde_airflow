from airflow.exceptions import AirflowException
from unittest.mock import patch
import pandas as pd
import os
import json
from plugins.etl.extract_data import extract_data_jobs, extract_data_organization, extract_data_job_categories, extract_data_position_type


@patch('plugins.etl.extract_data.fetch_all_pages')
def test_extract_data_jobs(mock_fetch_all_pages):
    """
    Test Unitario funcion extract_data_jobs

    Args:
        mock_fetch_all_pages (_type_): Mock de respusta de servicio API,
        utiliza archivo JSON en el directorio
    """
    # Cargar el JSON de ejemplo desde el archivo
    json_path = os.path.join(
        os.path.dirname(__file__), 'response-jobs-example.json')
    with open(json_path, 'r') as file:
        mock_data = json.load(file)

    # Configura el retorno del mock
    mock_fetch_all_pages.return_value = mock_data[
        'SearchResult']['SearchResultItems']

    # Llama a la función que se está probando
    # A fines de test no se controla fecha de publicación
    result_path = extract_data_jobs("test", "./tests/output", None)

    # Verificar que el archivo guardado tenga el nombre esperado
    assert result_path.endswith('_jobs_data.parquet')

    # Leer el archivo Parquet y validar las columnas
    df = pd.read_parquet(result_path)

    # Validar que las columnas esperadas están presentes
    expected_columns = ["MatchedObjectId", "PositionID", "PositionTitle",
                        "PositionLocationDisplay", "OrganizationName",
                        "DepartmentName", "JobCategoryCode",
                        "PositionStartDate", "PositionEndDate",
                        "PublicationStartDate", "ApplicationCloseDate",
                        "MinSalary", "MaxSalary", "RateIntervalDescription",
                        "PositionTypeCode", "DetailPositionType"]

    assert list(df.columns) == expected_columns, f"""
        Se esperaban estas columnas: {expected_columns},
        pero se obtuvieron estas: {list(df.columns)}"""

    # Eliminar el archivo después de la prueba
    if os.path.exists(result_path):
        os.remove(result_path)


@patch('plugins.etl.extract_data.fetch_organizations')
def test_extract_data_organization(mock_fetch_organizations):
    # Cargar el JSON de ejemplo desde el archivo
    json_path = os.path.join(
        os.path.dirname(__file__), 'response-organization-example.json')
    with open(json_path, 'r') as file:
        mock_data = json.load(file)

    # Configura el retorno del mock
    mock_fetch_organizations.return_value = mock_data[
        'CodeList'][0]['ValidValue']

    # Llama a la función que se está probando
    result_path = extract_data_organization("", "./tests/output")

    # Verificar que el archivo guardado tenga el nombre esperado
    assert result_path.endswith('_organizations_data.parquet')

    # Leer el archivo Parquet y validar las columnas
    df = pd.read_parquet(result_path)

    # Validar que las columnas esperadas están presentes
    expected_columns = ["Code", "Value", "ParentCode",
                        "LastModified", "IsDisabled"]
    assert list(df.columns) == expected_columns, f"""
        Se esperaban estas columnas: {expected_columns},
        pero se obtuvieron estas: {list(df.columns)}"""

    # Eliminar el archivo después de la prueba
    if os.path.exists(result_path):
        os.remove(result_path)


@patch('plugins.etl.extract_data.fetch_job_categories')
def test_extract_data_job_categories(mock_fetch_job_categories):
    # Cargar el JSON de ejemplo desde el archivo
    json_path = os.path.join(
        os.path.dirname(__file__), 'response-job-category-example.json')
    with open(json_path, 'r') as file:
        mock_data = json.load(file)

    # Configura el retorno del mock
    mock_fetch_job_categories.return_value = mock_data[
        'CodeList'][0]['ValidValue']

    # Llama a la función que se está probando
    result_path = extract_data_job_categories("", "./tests/output")

    # Verificar que el archivo guardado tenga el nombre esperado
    assert result_path.endswith('_job_categories_data.parquet')

    # Leer el archivo Parquet y validar las columnas
    df = pd.read_parquet(result_path)

    # Validar que las columnas esperadas están presentes
    expected_columns = ["Code", "Value", "JobFamily",
                        "LastModified", "IsDisabled"]
    assert list(df.columns) == expected_columns, f"""
        Se esperaban estas columnas: {expected_columns},
        pero se obtuvieron estas: {list(df.columns)}"""

    # Eliminar el archivo después de la prueba
    if os.path.exists(result_path):
        os.remove(result_path)


@patch('plugins.etl.extract_data.fetch_position_types')
def test_extract_data_position_type(mock_fetch_position_types):
    # Cargar el JSON de ejemplo desde el archivo
    json_path = os.path.join(
        os.path.dirname(__file__), 'response-position-types-example.json')
    with open(json_path, 'r') as file:
        mock_data = json.load(file)

    # Configura el retorno del mock
    mock_fetch_position_types.return_value = mock_data[
        'CodeList'][0]['ValidValue']

    # Llama a la función que se está probando
    result_path = extract_data_position_type("", "./tests/output")

    # Leer el archivo Parquet y validar las columnas
    df = pd.read_parquet(result_path)

    # Verificar que el archivo guardado tenga el nombre esperado
    assert result_path.endswith('_position_types_data.parquet')

    # Leer el archivo Parquet y validar las columnas
    df = pd.read_parquet(result_path)

    # Validar que las columnas esperadas están presentes
    expected_columns = ["Code", "Value", "LastModified", "IsDisabled"]
    assert list(df.columns) == expected_columns, f"""
        Se esperaban estas columnas: {expected_columns},
        pero se obtuvieron estas: {list(df.columns)}"""

    # Eliminar el archivo después de la prueba
    if os.path.exists(result_path):
        os.remove(result_path)
