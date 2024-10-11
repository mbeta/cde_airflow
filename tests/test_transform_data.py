import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from etl.app.transform_data import (
    transform_data_jobs,
    transform_data_organization,
    transform_data_category,
    transform_data_position_types,
)

# Datos de entrada de ejemplo para los tests
test_data_jobs = {
    'MatchedObjectId': ['811721900', '812494700', '811631200'],
    'PositionID': ['MCGT244059969885HW',
                   'AFMC-12566475-584347-9V-MLL',
                   'GSFC-24-IMP-12552581-AS'],
    'PositionTitle': [
        'Information Technology Specialist (Application Software)',
        'SOFTWARE MANAGEMENT SPECIALIST',
        'Supervisory Computer Engineer, AST, Software Systems'],
    'PositionLocationDisplay': ['Scott AFB, Illinois',
                                'Tinker AFB, Oklahoma',
                                'Wallops Island, Virginia'],
    'OrganizationName': [
        'U.S. Army Military Surface Deployment and Distribution Command',
        'Air Force Materiel Command',
        'Goddard Space Flight Center'],
    'DepartmentName': ['Department of the Army',
                       'Department of the Air Force',
                       'National Aeronautics and Space Administration'],
    'JobCategoryCode': ['2210', '0301', '0854'],
    'PositionStartDate': ['2024-09-27T08:38:51.0170',
                          '2024-10-03T00:00:00.0000',
                          '2024-10-03T00:00:00.0000'],
    'PositionEndDate': ['2024-10-07T23:59:59.9970',
                        '2024-10-09T23:59:59.9970',
                        '2024-10-09T23:59:59.9970'],
    'PublicationStartDate': ['2024-09-27T08:38:51.0170',
                             '2024-10-03T00:00:00.0000',
                             '2024-10-03T00:00:00.0000'],
    'ApplicationCloseDate': ['2024-10-07T23:59:59.9970',
                             '2024-10-09T23:59:59.9970',
                             '2024-10-09T23:59:59.9970'],
    'MinSalary': [89054.0, 39576.0, 122198.0],
    'MaxSalary': [115766.0, 94317.0, 158860.0],
    'RateIntervalDescription': ['Per Year', 'Per Year', 'Per Year'],
    'PositionTypeCode': ['15317', '15317', '15317'],
    'DetailPositionType': ['', '', ''],
}

test_data_organization = {
    'Code': ['AR', 'AF', 'NN',
             'ARXT', 'AF1M', 'NN51'],
    'Value': ['Department of the Army', 'Department of the Air Force',
              'National Aeronautics and Space Administration',
              'U.S. Army Military Surface Deployment and Distribution Command',
              'Air Force Materiel Command', 'Goddard Space Flight Center'],
    'ParentCode': ['DD', 'DD', None,
                   'AR', 'AF', 'NN'],
    'LastModified': ['2021-04-30T11:38:44.897',
                     '2021-05-14T11:04:18.77',
                     '2021-04-30T11:38:54.68',
                     '2021-04-30T11:38:48.777',
                     '2021-04-30T11:38:46.72',
                     '2021-04-30T11:38:54.737'],
    'IsDisabled': ['No', 'No', 'No', 'No', 'No', 'No'],
}

test_data_category = {
    'Code': ['2210', '0301', '0854'],
    'Value': ['Information Technology Management',
              'Miscellaneous Administration And Program',
              'Computer Engineering'],
    'JobFamily': ['2200', '0300', '0800'],
    'LastModified': ['2016-10-21T06:13:47.827',
                     '2011-07-06T21:35:26.23',
                     '2019-08-26T06:18:32.53'],
    'IsDisabled': ['No', 'No', 'No'],
}

test_data_position_types = {
    'Code': ['15317', '15318', '15319'],
    'Value': ['Permanent', 'Temporary', 'Term'],
    'LastModified': ['2018-07-27T06:13:07.677',
                     '2018-07-27T06:13:07.677',
                     '2018-07-27T06:13:07.68'],
    'IsDisabled': ['No', 'No', 'No'],
}


@pytest.fixture
def mock_parquet_read():
    with patch('pandas.read_parquet') as mock:
        yield mock


def test_transform_data_jobs(mock_parquet_read):
    mock_parquet_read.return_value = pd.DataFrame(test_data_jobs)
   
    # Creamos un mock para la conexión
    mock_connection = MagicMock()
    mock_connection.execute.return_value = [
        {'code': 'ARXT', 'name': 'U.S. Army Military Surface Deployment and Distribution Command'},
        {'code': 'AF1M', 'name': 'Air Force Materiel Command'},
        {'code': 'NN51', 'name': 'Goddard Space Flight Center'},
        {'code': 'AR', 'name': 'Department of the Army'},
        {'code': 'AF', 'name': 'Department of the Air Force'},
        {'code': 'NN', 'name': 'National Aeronautics and Space Administration'},
    ]
    
    # Definimos los mocks
    with patch('app.db_services.get_redshift_connection', return_value=mock_connection):
        result = transform_data_jobs('dummy_path')
        
    # with patch('app.db_services.get_organization_codes_by_names', return_value=mock_connection):
    #     result = transform_data_jobs('dummy_path')

    expected_data = {
        'object_id': ['811721900', '812494700', '811631200'],
        'position_id': ['MCGT244059969885HW',
                        'AFMC-12566475-584347-9V-MLL',
                        'GSFC-24-IMP-12552581-AS'],
        'position_title': [
            'Information Technology Specialist (Application Software)',
            'SOFTWARE MANAGEMENT SPECIALIST',
            'Supervisory Computer Engineer, AST, Software Systems'],
        'location_description': ['Scott AFB, Illinois',
                                 'Tinker AFB, Oklahoma',
                                 'Wallops Island, Virginia'],
        'organization_code': ['ARXT', 'AF1M', 'NN51'],
        'department_code': ['AR', 'AF', 'NN'],
        'job_category_code': ['2210', '0301', '0854'],
        'position_start_date': pd.to_datetime(['2024-09-27T08:38:51.0170',
                                               '2024-10-03T00:00:00.0000',
                                               '2024-10-03T00:00:00.0000']),
        'position_end_date': pd.to_datetime(['2024-10-07T23:59:59.9970',
                                             '2024-10-09T23:59:59.9970',
                                             '2024-10-09T23:59:59.9970']),
        'publication_start_date': pd.to_datetime(['2024-09-27T08:38:51.0170',
                                                  '2024-10-03T00:00:00.0000',
                                                  '2024-10-03T00:00:00.0000']),
        'application_close_date': pd.to_datetime(['2024-10-07T23:59:59.9970',
                                                  '2024-10-09T23:59:59.9970',
                                                  '2024-10-09T23:59:59.9970']),
        'minimum_salary': [89054.0, 39576.0, 122198.0],
        'maximum_salary': [115766.0, 94317.0, 158860.0],
        'rate_interval_description': ['Per Year', 'Per Year', 'Per Year'],
        'position_type_code': ['15317', '15317', '15317'],
        'detail_position_type': ['', '', ''],
        'duration': [10, 6, 6],
    }

    expected_df = pd.DataFrame(expected_data)
    # Eliminamos la columna 'version_date' del resultado,
    # es un dato que nunca coincidiran
    result = result.drop(columns=['version_date'])
    pd.testing.assert_frame_equal(result, expected_df)


def test_transform_data_organization(mock_parquet_read):
    mock_parquet_read.return_value = pd.DataFrame(test_data_organization)

    result = transform_data_organization('dummy_path')

    expected_data = {
         'code': ['AR', 'AF', 'NN', 'ARXT', 'AF1M', 'NN51'],
         'name': [
             'Department of the Army',
             'Department of the Air Force',
             'National Aeronautics and Space Administration',
             'U.S. Army Military Surface Deployment and Distribution Command',
             'Air Force Materiel Command', 'Goddard Space Flight Center'],
         'parent_code': ['DD', 'DD', None, 'AR', 'AF', 'NN'],
         'last_modified': ['2021-04-30T11:38:44.897',
                           '2021-05-14T11:04:18.77',
                           '2021-04-30T11:38:54.68',
                           '2021-04-30T11:38:48.777',
                           '2021-04-30T11:38:46.72',
                           '2021-04-30T11:38:54.737'],
         'is_disabled': ['No', 'No', 'No', 'No', 'No', 'No'],
    }

    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)


def test_transform_data_category(mock_parquet_read):
    mock_parquet_read.return_value = pd.DataFrame(test_data_category)
    result = transform_data_category('dummy_path')
    expected_data = {
            'code': ['2210', '0301', '0854'],
            'name': ['Information Technology Management',
                     'Miscellaneous Administration And Program',
                     'Computer Engineering'],
            'job_family': ['2200', '0300', '0800'],
            'last_modified': ['2016-10-21T06:13:47.827',
                              '2011-07-06T21:35:26.23',
                              '2019-08-26T06:18:32.53'],
            'is_disabled': ['No', 'No', 'No'],
    }

    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)


def test_transform_data_position_types(mock_parquet_read):
    mock_parquet_read.return_value = pd.DataFrame(test_data_position_types)
    result = transform_data_position_types('dummy_path')
    expected_data = {
        'code': ['15317', '15318', '15319'],
        'name': ['Permanent', 'Temporary', 'Term'],
        'last_modified': ['2018-07-27T06:13:07.677',
                          '2018-07-27T06:13:07.677',
                          '2018-07-27T06:13:07.68'],
        'is_disabled': ['No', 'No', 'No'],
    }
    expected_df = pd.DataFrame(expected_data)
    pd.testing.assert_frame_equal(result, expected_df)
