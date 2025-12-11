🟩 HITO 1 — 11/12
👉 Objetivo: Arquitectura + ingesta básica + primeros jobs funcionando

BLOQUE 1 — ARQUITECTURA
Tareas
Definir arquitectura Kappa completa (componentes y flujo general).


Definir flujo batch: NiFi → HDFS → PySpark → SQL.


Definir flujo streaming: NiFi → Kafka → Flink → Mongo/Cassandra.


Establecer naming conventions:


Topics Kafka


Capas HDFS (raw, curated, analytics)


Definir esquemas base de mensajes (JSON para flights, JSON para weather).


Crear el diagrama de arquitectura (Draw.io / Miro).


Crear el catálogo inicial de datasets (columnas + diccionario básico).


BLOQUE 2 — INGESTA (NiFi)
Tareas
Configurar Flow NiFi para leer delays_history_agg.csv.


Crear proceso NiFi para cargar CSV en HDFS (zona raw).


Convertir CSV → JSON para Kafka (opcional Hito 1).


Preparar pipeline NiFi vacío para futuras llamadas a OpenSky.


Preparar pipeline NiFi vacío para futuras llamadas a OpenWeather.


Exportar template NiFi del flujo actual.


BLOQUE 3 — KAFKA (Infraestructura base)
Tareas
Crear los topics:


flights_all


weather_all


Verificar creación con kafka-topics.sh.


Enviar mensajes de prueba a los topics.


Consumir mensajes de prueba para validar conexión.


BLOQUE 4 — PROCESAMIENTO INICIAL (Flink + PySpark)
Tareas
Crear proyecto base de Flink/Beam.


Implementar job inicial:


Leer mensajes de Kafka


Loguearlos en consola


Crear proyecto PySpark.


PySpark: leer delays_history_agg.csv desde HDFS.


Calcular agregaciones simples:


Media de retrasos


Conteo de registros


Generar primer script batch funcional.


Guardar scripts en estructura del repositorio.


BLOQUE 5 — GESTIÓN DEL PROYECTO
Tareas
Configurar repositorio Git con estructura base.


Crear ramas:


main, dev, feature/…


Crear README inicial del proyecto.


Configurar tablero (Trello/Jira/GitHub Projects).


Crear tarjetas para tareas del Hito 1.


Subir primeras evidencias (capturas NiFi, comandos Kafka, etc.).


🟦 HITO 2 — 18/12
👉 Objetivo: Datos limpios + correlaciones + Power BI con 3 páginas

BLOQUE 1 — BATCH (PySpark completo)
Tareas
Limpiar dataset delays_history_agg.csv:


nulos


tipos


rangos


Limpiar weather_history.csv.


Normalizar fechas y aeropuertos.


Unir delays + weather por aeropuerto + fecha.


Calcular métricas agregadas:


retraso medio


retrasos > 15 min


minutos de retraso por causa


medias meteorológicas


Crear tablas finales:


dim_airport


dim_airline


fact_delays_weather


Cargar estas tablas en MySQL/SQL Server.


Generar dataset preparado para Power BI.


BLOQUE 2 — POWER BI (3 páginas obligatorias)
Tareas
Página 1:


Vista global por aeropuerto


Mapa / tabla / KPIs


Página 2:


Relación meteorología–retrasos


Scatterplot / líneas combinadas


Página 3:


Comparativa por aerolínea o ruta


Crear medidas DAX básicas (si aplica).


Documentar filtros, navegación, estructura.


BLOQUE 3 — CALIDAD DEL DATO (Primera versión)
Tareas
Definir 5 reglas de calidad del dato:


nulos obligatorios


rangos retrasos


coherencia temporal


rangos meteo válidos


consistencia delay_cause


Implementar validaciones en NiFi (ValidateRecord).


Implementar validaciones en PySpark (filtros + conteos).


Generar reportes de % válido / % erróneo.


Documentar estrategia de calidad.


🟧 HITO 3 — 08/01
👉 Objetivo: API real-time + KPIs streaming + demo + informe final

BLOQUE 1 — INGESTA APIs en NiFi
Tareas
Crear flujo NiFi para OpenSky (vuelos).


Crear flujo NiFi para OpenWeather (meteo).


Normalizar JSON (atributos clave).


Publicar eventos en Kafka:


flights_all


weather_all


Implementar manejo de errores y backpressure.


Documentar endpoints, límites, parámetros.

BLOQUE 2 — STREAMING (Flink completo)
Tareas
Leer flights_all + weather_all en Flink.


Implementar join por aeropuerto + ventana temporal.


Calcular KPIs en tiempo real:


retraso estimado medio


vuelos con retraso significativo


riesgo por aeropuerto


Escribir KPIs en MongoDB/Cassandra.


Publicar opcionalmente KPIs en un topic kpi_rt.


Implementar reprocesado Kappa (offset 0).


Documentar el pipeline (latencias, throughput, checkpoints).


BLOQUE 3 — POWER BI FINAL (historico + real-time)
Tareas
Conectar Power BI a:


MySQL (histórico)


Mongo/Cassandra o topic procesado (real-time)


Crear visual de KPIs en tiempo casi real.


Ajustar navegación final del dashboard.


Exportar versión final del PBIX.


BLOQUE 4 — MONITORIZACIÓN + GOBERNANZA
Tareas
Definir métricas:


eventos procesados


lag de Kafka


registros inválidos


Capturar métricas desde UIs (NiFi, Flink, Kafka, Spark).


Crear tabla/colección de “métricas de plataforma”.


Crear catálogo final de datos (dataset → proceso → destino).


Crear diagrama de linaje completo.


Definir SLAs:


latencia real-time


refresco histórico


Documentar todo en el informe.

BLOQUE 5 — INFORME + PRESENTACIÓN
Tareas
Escribir informe técnico (máx. 5 páginas).


Incluir evidencias de:


NiFi


Kafka


Spark


Flink


SQL/NoSQL


Power BI


Preparar presentación (10–15 min).


Ensayar demo end-to-end.


