# cde_airflow
Proyecto Airflow 

## Origen de datos
Se utilizara una API provista por USAJOBS, que brinda informacion de puestos de trabajo ofrecidos por la Oficina de Personal de los Estados Unidos  (https://www.opm.gov/).
Con dicho servicio se podra registrar informacion sobre ofertas laborales publicadas a diario con ciertos parametros de interes para las estadísticas que se demandan.

### Api de búsqueda
https://developer.usajobs.gov/api-reference/get-api-search

### Seguridad
Authorization: API Key Authentication

### Configuracion 
Para que las consultas a la Rest-API funcione se debe proveer un archivo .env en la raiz del directorio con la siguiente información:

    USER_AGENT=[USER]
    AUTHORIZATION_KEY=[API_KEY]
    HOST=data.usajobs.gov

(Por razones de seguridad no se subira al repositorio)

### Paginación
La páginacion de resultados se realiza mediante un parametro de tamaño de página esperado en la respuesta (ResultsPerPage) y el numero de página solicitado. El Servicio de acuerdo a estos parametros informara el numero de páginas a consultar (NumberOfPages)

Query Params:
- Page
- ResultsPerPage

Response: 
- NumberOfPages

### Consioderaciones 
Todas las consultas a los servicios se realiza con una palabra clave (keyword) 'Software' para filtrar los empleados relacionados con el area de interes elegido.
Se permite una carga inicial de historico de hasta 60 dias previos a la fecha de consulta (DatePosted = 60), posteriormente se consultara las ofertas del día (DatePosted = 0).