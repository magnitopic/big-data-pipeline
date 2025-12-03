## ALEX

-   Montrar entorno docker.

    -   Crear entorno con todos los contenedores y estrucutra de Docker.
    -   Configurar servicios
    -   Entorno y secretos

-   Installar pyspark
    -   Crear entorno Python
    -   Crear script de python que lea datos de la api

## Hugo

-   APIs y tokens

    -   Crear cuenta y registrase en los diferentes servicios que vamos a usar
    -   Guardar tokes y otra información en el fichero `.env`

-   Crear diagrama de arquitectura
    -   Crear diagrama de arquitectura Kappa. Mostrar:
        -   Fuentes de datos (archivos CSV + APIs)
            -   Capa de ingestión en NiFi
            -   Tópicos de Kafka (registro unificado)
            -   Procesamiento con Flink
            -   Capa de almacenamiento (HDFS, MySQL, Cassandra/MongoDB)
            -   Conexión con Power BI

## Nacho

-   Base de datos

    -   Para MySQL, Cassandra y MongoDB
    -   Crear eschemas para cada tipo de dato
    -   Crear tablas para cada tipo de dato

-   NiFi
    -   Acceder a NiFi (puerto 8443)
    -   Crear primer flow: Leer CSV -> parsear -> print
    -   Segundo flow: usar api de OpenSky -> parsear JSON -> print
