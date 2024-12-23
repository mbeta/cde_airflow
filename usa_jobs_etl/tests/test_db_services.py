from unittest.mock import MagicMock
import os
from plugins.etl.db_services import get_redshift_connection, create_table, get_organization_codes_by_names


def test_get_redshift_connection(monkeypatch):
    """
    Test Unitario funcion get_redshift_connection,
    sin exponer las credenciales reales.

    Args:
        mocker (_type_): Mock
    """
     # Mockeamos os.environ
    mock_environ = {
        'REDSHIFT_USER': 'user',
        'REDSHIFT_PASSWORD': 'password',
        'REDSHIFT_SERVER': 'host',
        'REDSHIFT_DB': 'dbname',
        'REDSHIFT_PORT': '5439'
    }
    monkeypatch.setattr(os, 'environ', mock_environ)
    
    mock_create_engine = MagicMock()
    monkeypatch.setattr('plugins.etl.db_services.create_engine', mock_create_engine)

    # Simulamos la conexión
    mock_connection = MagicMock()
    mock_create_engine.return_value.connect.return_value = mock_connection

    # Llamamos a la función
    connection = get_redshift_connection()

    mock_create_engine.assert_called_once_with(
        'redshift+psycopg2://user:password@host:5439/dbname', isolation_level='READ COMMITTED')
    mock_create_engine.return_value.connect.assert_called_once()
    assert connection == mock_connection


def test_create_table(mocker):
    """
    Test Unitario funcion create_table

    Args:
        mocker (_type_): Mock
    """
    mock_get_redshift_connection = mocker.patch('plugins.etl.db_services.get_redshift_connection')

    mock_conn = MagicMock()
    mock_get_redshift_connection.return_value = mock_conn
    create_table("test_table", "CREATE TABLE {schema}.test_table (id INT);")

    mock_conn.execute.assert_called_once()


def test_get_organization_codes_by_names(mocker):
    """
    Test Unitario funcion get_organization_codes_by_names

    Args:
        mocker (_type_): Mock
    """
    mock_get_redshift_connection = mocker.patch(
        'plugins.etl.db_services.get_redshift_connection')

    mock_conn = MagicMock()
    mock_get_redshift_connection.return_value = mock_conn
    mock_conn.execute.return_value = [
        {"code": "AF00", "name": "Department of the Air Force - Agency Wide"},
        {"code": "AG00", "name": "Department of Agriculture - Agency Wide"}]

    names = ["Department of Agriculture - Agency Wide",
             "Department of the Air Force - Agency Wide"]
    result = get_organization_codes_by_names(names)

    assert result == {"Department of the Air Force - Agency Wide": "AF00",
                      "Department of Agriculture - Agency Wide": "AG00"}

    mock_conn.execute.assert_called_once()
