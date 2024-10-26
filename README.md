# CDE AIRFLOW
---
Proyecto Airflow 
(Airflow, python, Docker, unittest, mock, GitHub Actions)

## Origen de datos
***
Se utilizara una API provista por USAJOBS, que brinda informacion de puestos de trabajo ofrecidos por la Oficina de Personal de los Estados Unidos  (https://www.opm.gov/).
Con dicho servicio se podra registrar informacion sobre ofertas laborales publicadas a diario con ciertos parametros de interes para las estadísticas que se demandan.

### Api de búsqueda
https://developer.usajobs.gov/api-reference/get-api-search

### Seguridad
Authorization: API Key Authentication
Debera ser gestionada a traves de la web USAJobs

### Configuracion 
Para que las consultas a la Rest-API funcione se debe proveer un archivo .env en la raiz del directorio con la siguiente información:

    USER_AGENT=[USER]
    AUTHORIZATION_KEY=[API_KEY]
    HOST=data.usajobs.gov

## Modelización
---
![Diagrama ER](/img/ER.jpg)


#### jobs
Se trabaja como tabla de HECHO las publicaciones de puestos de trabajo
Para mas información: [API-Reference](https://developer.usajobs.gov/api-reference/get-api-search)
    
> MatchedObjectId (PK)
PositionID (Identifica cada posición en la organización)
PositionTitle
LocationDescription 
OrganizationCode (FK a la Dimensión Organization)
DepartmentCode (FK a la Dimensión Organization)
JobCategoryCode (FK a la Dimensión JobCategory)
PositionStartDate
PositionEndDate
PublicationStartDate
ApplicationCloseDate
MinimumSalary
MaximumSalary
RateIntervalDescription
PositionTypeCode (FK a la Dimensión PositionType)
DetailPositionType
VersionDate (fecha en que se registró la oferta o actualización)
IsReposted (para marcar si la oferta es repetida de una anterior)
Duration (calculada con el tiempo que la oferta estuvo abierta)


#### dim_organization
Se trabaja como tabla de Dimension las sub-agencias que pueden ser Organizaciones y Departamentos.
Para mas información: [api-reference](https://developer.usajobs.gov/api-reference/get-codelist-agencysubelements)
    
>Code (PK)
Name
ParentCode
LastModified
IsDisabled

#### dim_job_category
Se trabaja como tabla de Dimension las categorias de trabajos publicados.
Para mas información: [api-reference](https://developer.usajobs.gov/api-reference/get-codelist-occupationalseries)
    
>Code (PK)
Name
LastModified
IsDisabled

#### dim_position_type
Se trabaja como tabla de Dimension las tipos de posiciones ofrecidas.
Para mas información: [api-reference](https://developer.usajobs.gov/api-reference/get-codelist-positionofferingtypes)
    
>Code (PK)
Name
LastModified
IsDisabled



#### Paginación
La páginacion de resultados se realiza mediante un parametro de tamaño de página esperado en la respuesta (ResultsPerPage) y el numero de página solicitado. El Servicio de acuerdo a estos parametros informara el numero de páginas a consultar (NumberOfPages)
Los servicios utilizados solo ofrecen paginacion en la consulta de JOBS

Query Params:
- Page
- ResultsPerPage

Response: 
- NumberOfPages

En las APIs utilizadas para las dimensiones, se utilizara como parametro de consulta lastmodified, para solo traer las ultimas modificaciones y poder impactar sus actualizaciones.

#### Consideraciones 
Todas las consultas a los servicios se realiza con una palabra clave (keyword) 'Software' para filtrar los empleados relacionados con el area de interes elegido.
Se permite una carga inicial de historico de hasta 60 dias previos a la fecha de consulta (DatePosted = 60), posteriormente se consultara las ofertas del día anterior (DatePosted = 1), para evitar el procesamiento de un dia incompleto.
Debido a que la API de JOBS no permite filtro por fecha, se calculara el dia inmediato anterior a la fecha que se consulta, para filtar manualmente las publicaciones con fecha posterior y evitar procesar publicaciones de un dia incompleto.


## Como iniciar el proyecto
---
#### Variables de entorno 
Se debera crear un archivo .env en la raiz del proyecto (dentro de /usa_jobs_etl)
```
    USER_AGENT=(email con el que re registro en USAJobs)
    AUTHORIZATION_KEY=(Api Key otorgado por USAJobs)
    HOST=data.usajobs.go
    REDSHIFT_SCHEMA=(Schema de la DB Redshift)
    REDSHIFT_CONN_STRING=(String de conexion de la DB Redshift)
    AIRFLOW_UID=(UID de su SO o 50000 por defecto)
    AIRFLOW_GID=0

```

#### Iniciar Airflow
Requerimientos previos:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Docker-compose](https://docs.docker.com/compose/install/)

Pasos:
1. Posicionarse en /usa_jobs_etl
2. copiar .env en raiz de directorio 
3. ejecutar: docker-compose up airflow-init
4. docker-compose up
5. Abrir navegador en http://localhost:8080/home
6. Login a AirFlow:
    ***User***: airflow
    ***Contraseña***: airflow

![Diagrama ER](/img/Login-Airflow.jpg)

#### Deterner Airflow
Debera ejecutar:
```
docker-compose down
```
#### DAGs
![Diagrama ER](/img/DAGs.jpg)

Detalle:
- **drop_schema_dag**: Dag con funcionalidad para borrar schema de la base de datos. Su ejecucion es A DEMANDA.
- **etl_dimensions_dag**: Dag con la funcionalidad para actualizar las dimensiones del modelo, realizara la consulta a los servicios con fechas del ultima actualización el dia anterior la fecha de contexto. Fecruencia de ejecución DIARIA.
Si es necesario el mismo DAG creará las tablas en la base de datos.
- **etl_usa_job_software_dag**: Dag con la funcionalidad para actualizar la tabla de hechos (Jobs) con las novedades de publicacion de trabajos con fecha de publicación el dia anterior de la fecha de contexto. Frecuencia de ejecucion DIARIA.
Si es necesario el mismo DAG creará las tablas en la base de datos.
Como condicion de ejecuciónn controlará que el DAG ***etl_dimensions_dag*** se haya ejecutado exitosamente en las ultimas 24 horas, con el fin de garantizar que cuente con la informaciónn necesaria actualizada.
- **history_dimensions_dag**: Dag con la funcionalidad para poblar las tablas dimensiones con todas los registros obtenidos por la API, sin considerar la fecha de ultima actualizacion.
Su ejecucion es A DEMANDA, si es necesario el mismo DAG creará las tablas en la base de datos.
- **history_usa_job_software_dag**: Dag con la funcionalidad para poblar las tablas hechos (jobs) con todos los registros obtenidos por la API de los ultimos 60 días publicaciones de trabajos (máximo posible de consulta).
Su ejecucion es A DEMANDA, si es necesario el mismo DAG creará la tabla jobs en la base de datos.


#### Cambios en el proyecto
Si realizo cambios en el proyecto y desea desplegarlos:
1. docker-compose down
2. docker-compose build
3. docker-compose up -d

#### Test Unitarios
Tener en cuenta que se encuentra configurado Test Unitarios que se validan mediante Github Actions.
Configuracion en .github/workflows/run-tests.yaml
Test unitarios en /usa_jobs_etl/tests

Si desea chequear los test:
1. ir al directorio /usa_jobs_etl
2. ejecutar: poetry install (la primera vez)
3. ejecutar: poetry run pytest tests

Lo test incluidos en el proyecto son los siguientes:

- **test_get_redshift_connection**: Evalúa la función get_redshift_connection asegurándose de que cree una conexión a Redshift utilizando un string de conexión simulado, sin exponer credenciales reales. Verifica que la función llame a create_engine con los argumentos correctos y establezca la conexión.

- **test_create_table**: Prueba la función create_table, la cual ejecuta un comando SQL para crear una tabla en Redshift. Simula la conexión y confirma que 'execute' se llame correctamente para ejecutar el comando de creación de tabla.

- **test_get_organization_codes_by_names**: Valida que get_organization_codes_by_names consulte correctamente los códigos de organización basándose en nombres de organización específicos. Simula la conexión y define un resultado esperado, verificando que el retorno de la función sea un diccionario de nombres y códigos correctos.

- **test_extract_data_jobs**: Verifica que extract_data_jobs extrae y guarda correctamente los datos de puestos de trabajo en formato Parquet. Utiliza un JSON de ejemplo y mockea la función fetch_all_pages para simular la respuesta de la API. Luego, comprueba que el archivo Parquet generado contiene las columnas esperadas.

- **test_extract_data_organization**: Evalúa que extract_data_organization convierte los datos de organizaciones en un archivo Parquet. Mockea fetch_organizations con un JSON simulado y revisa que las columnas del archivo Parquet correspondan a las especificadas.

- **test_extract_data_job_categories**: Prueba extract_data_job_categories, que guarda los datos de categorías de trabajo en un archivo Parquet. Mockea fetch_job_categories y verifica que el archivo resultante contenga las columnas correctas.

- **test_extract_data_position_type**: Confirma que extract_data_position_type procesa los tipos de posición y genera un archivo Parquet con las columnas adecuadas, usando un JSON de ejemplo y mockeando fetch_position_types.

- **test_transform_data_organization**:  Valida que la función transform_data_organization transforme correctamente los datos de la organización, utilizando datos simulados y la salida generada por la función se compara con el DataFrame esperado que es definido (expected_df) para verificar que los datos estén formateados y estructurados correctamente.

- ***test_transform_data_category***: Verifica que la función transform_data_category transforme correctamente los datos de categorías de trabajo. Utiliza datos de entrada simulados que representan códigos de categorías, nombres, familias de trabajo, fechas de modificación y estatus.
La salida de la función debe coincidir con el DataFrame esperado, asegurando que los datos de categorías se procesen adecuadamente.

- ***test_transform_data_position_types***: Verifica que la función transform_data_position_types transforme correctamente los tipos de posición.
Los datos de entrada simludados incluyen códigos de tipo de posición, nombres, fechas de modificación y estatus.
Compara la salida con el DataFrame esperado para validar que la transformación mantenga los datos en el formato adecuado.