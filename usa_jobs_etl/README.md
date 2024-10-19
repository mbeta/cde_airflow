# cde_airflow
Proyecto Airflow 
(Airflow, python, Docker, unittest, mock, GitHub Actions)

## Origen de datos
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

### Modelización
#### jobs
Se trabaja como tabla de HECHO las publicaciones de puestos de trabajo
Para mas información: [api-reference](https://developer.usajobs.gov/api-reference/get-api-search)
    
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

#### Consioderaciones 
Todas las consultas a los servicios se realiza con una palabra clave (keyword) 'Software' para filtrar los empleados relacionados con el area de interes elegido.
Se permite una carga inicial de historico de hasta 60 dias previos a la fecha de consulta (DatePosted = 60), posteriormente se consultara las ofertas del día anterior (DatePosted = 1), para evitar el procesamiento de un dia incompleto.
Debido a que la API de JOBS no permite filtro por fecha, se calculara el dia inmediato anterior a la fecha que se consulta, para filtar manualmente las publicaciones con fecha posterior y evitar procesar publicaciones de un dia incompleto.

## Como iniciar el proyecto

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

#### Base de Datos
Si no cuenta con el schema creado con las tablas, puede utilizar la funcionalidad dentro de db_services.py:
- drop_schema()
- create_all_tables()

#### Iniciar Airflow
Requerimientos previos:
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) o similiar
- [Docker-compose](https://docs.docker.com/compose/install/)

Pasos:
1. Posicionarse en /usa_jobs_etl
2. copiar .env en raiz de directorio 
3. ejecutar: docker-compose up airflow-init
4. docker-compose up
5. Abrir navegador en http://localhost:8080/home

#### Deterner Airflow
Debera ejecutar:
```
docker-compose down
```

#### Cambios en el proyecto
Si realizo cambios en el proyecto y desea desplegarlos:
1. docker-compose down
2. docker-compose build
3. docker-compose up


