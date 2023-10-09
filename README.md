# Distribuidos TP1: Flights Optimizer

Flights Optimizer es un sistema distribuido preparado para realizar consultas sobre los datos que provengan de registros de vuelos
que cumplan con ciertas condiciones. Los resultados de estas consultas son devueltas al cliente para que el mismo
pueda tomar las decisiones que considere oportunas.

El objetivo del presente trabajo práctico es realizar un sistema distribuido escalable capaz de 
analizar 6 meses de registros de precios de vuelos de avión para proponer mejoras en la oferta a clientes.

Este sistema debe de responder a las siguientes consultas:
* ID, trayecto, precio y escalas de vuelos de 3 escalas o más.
* ID, trayecto y distancia total de vuelos cuya distancia total sea mayor a cuatro veces la distancia directa entre puntos origen-destino.
* ID, trayecto, escalas y duración de los 2 vuelos más rápidos para todo trayecto con algún vuelo de 3 escalas o más.
* El precio avg y max por trayecto de los vuelos con precio mayor a la media general de precios.


## Ejecución
Se provee un `docker-compose-dev.yaml` que tiene todas las configuraciones requeridas para ejecutar el sistema
y un `Makefile` para simplificar la ejecución de los comandos. Se puede ejecutar como `make <target>`

El `Makefile` tiene las acciones:
* `docker-image`: Buildea las imágenes a ser utilizadas tanto en el servidor como en el cliente. Este target es utilizado por `docker-compose-up`
* `docker-compose-up`: Inicializa el ambiente de desarrollo (buildear docker images del servidor y cliente, inicializar la red a utilizar por docker, etc.) y arranca los containers de las aplicaciones que componen el proyecto.
* `docker-compose-down`: Realiza un `docker-compose stop` para detener los containers asociados al compose y luego realiza un `docker-compose down` para destruir todos los recursos asociados al proyecto que fueron inicializados
* `docker-compose-logs`: Permite ver los logs actuales del proyecto.

Una cuestión a tener en cuenta, es que se deberán de configurar los archivos a utilizar por el cliente. 
Por defecto el docker-compose los busca de la carpeta `/data`, 
pero es posible modificar el `docker-compose` para que los busque en otro directorio.

## Informe

Para ver los detalles de implementación, diagramas, y explicaciones de las decisiones tomadas referirse al informe en el repositorio.