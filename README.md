# RETO BIG DATA: METEOROLOGÍA Y RETRASOS DE VUELOS  
**Competición por equipos – Arquitecturas Lambda vs Kappa con ingesta en tiempo real desde APIs**

## 1. Contexto y objetivo

Se plantea analizar la relación entre las condiciones meteorológicas y los retrasos de vuelos comerciales en aeropuertos de Estados Unidos, utilizando datos históricos y flujos de datos en tiempo casi real.

Objetivos del reto:

- Diseñar, implementar y demostrar una arquitectura de datos que permita:
  - Correlacionar retrasos con variables meteorológicas.
  - Calcular indicadores clave (KPIs) por aeropuerto, aerolínea, ruta y franja temporal.
  - Visualizar los resultados en un cuadro de mando en Power BI.
- Preparar datos para entrenar y testear un modelo de machine learning que prediga retrasos aéreos por causas meteorológicas.
- Comparar en la práctica las arquitecturas **Lambda** y **Kappa**.

Organización:

- **Equipos A y B**: implementarán una **arquitectura Kappa**.  
- **Equipos C y D**: implementarán una **arquitectura Lambda**.

Cada equipo trabajará de forma independiente y presentará sus resultados al resto de la clase.

Incentivos:

- El primer equipo en presentar un **MVP funcional** tendrá, si la necesitase, una bonificación de **hasta un 12 %**.
- La mejor propuesta/ejecución técnica recibirá una bonificación adicional de **hasta un 8 %**.

Gestión del proyecto:

- Cada grupo utilizará un **gestor de versiones** compatible con Git (GitLab, GitHub, Bitbucket, etc.) para el mantenimiento de código y documentación.
- Cada grupo utilizará una **herramienta de gestión de proyectos** estándar (por ejemplo, Trello, Jira, GitHub Projects, MS Project o equivalente) para:
  - Planificar tareas (Gantt o metodologías ágiles: Scrum, Kanban).
  - Repartir la carga de trabajo de forma equitativa.
  - Evidenciar la dedicación e implicación de cada miembro.
  - Permitir al Product Owner (el docente) intervenir en caso de bloqueos graves para reasignar tareas.

---

## 2. Datos disponibles

### 2.1. Histórico agregado de retrasos de vuelos

Fichero: `Airline_Delay_Cause.csv` (origen: Bureau of Transportation Statistics, formato ya preprocesado).

Campos relevantes (entre otros):

- `year`, `month`
- `airport`, `airport_name`
- `carrier`, `carrier_name`
- `arr_flights`, `arr_del15`, `arr_cancelled`, `arr_diverted`
- `arr_delay` (minutos totales de retraso)
- `carrier_delay`, `weather_delay`, `nas_delay`, `security_delay`, `late_aircraft_delay`

### 2.2. Ficheros derivados para el ejercicio

Se proporcionarán ficheros derivados ya preparados o generados mediante scripts:

- **`delays_history_agg.csv`**  
  Datos agregados por año, mes, aeropuerto y aerolínea, con:
  - Número de vuelos totales, vuelos con retraso > 15 minutos, cancelaciones y desvíos.
  - Retraso medio.
  - Minutos de retraso por causa (`carrier_delay`, `weather_delay`, etc.).

- **`delays_history_sample.csv`**  
  Muestra sintética a nivel vuelo, con columnas tipo:
  - `flight_id`, `airline`
  - `origin_airport`, `dest_airport`
  - `flight_date`
  - `scheduled_departure`, `actual_departure`
  - `scheduled_arrival`, `actual_arrival`
  - `departure_delay_min`, `arrival_delay_min`
  - `delay_cause` ∈ {`WEATHER`, `CARRIER`, `NAS`, `SECURITY`, `LATE_AIRCRAFT`, `OTHER`}

### 2.3. Histórico de meteorología

Fichero: `weather_history.csv`

- Se generará a partir de datos de **NOAA (National Oceanic and Atmospheric Administration)** o fuentes equivalentes.
- Contendrá, como mínimo, para todos los aeropuertos presentes en `Airline_Delay_Cause.csv` para los años **2019–2023**:
  - `airport_code`, `date` (resolución diaria recomendada)
  - `temperature_c`
  - `wind_speed_kt`, `wind_direction_deg`
  - `visibility_km`
  - `precip_mm`
  - `storm_flag` (0/1)
  - `conditions` (código o descripción breve)

Estos ficheros servirán como **base batch** y como referencia para validar los resultados que se obtengan de las APIs en tiempo real.

---

## 3. APIs externas a utilizar

Para la capa de streaming se trabajará con, al menos:

- Una **API de tráfico aéreo** (OpenSky Network).
- Una **API meteorológica** (OpenWeather u otra equivalente).

En este enunciado se describen los servicios de alto nivel.  
Los detalles de conexión (endpoints, ejemplos de URLs y parámetros) se recogen en el apartado final **“Referencias sobre uso de las APIs”**.

### 3.1. API de tráfico aéreo – OpenSky Network

- Sitio web: <https://opensky-network.org>  
- Documentación API: <https://opensky-network.org/data/api>  

Uso en el ejercicio:

- Obtención de estados de aeronaves o vuelos por aeropuerto en ventanas temporales acotadas.
- Generación de eventos en tiempo casi real sobre:
  - vuelos en curso,
  - vuelos llegados/salidos recientemente de determinados aeropuertos.

### 3.2. API meteorológica – OpenWeather (One Call API 3.0)

- Portal de APIs: <https://openweathermap.org/api>  

Uso en el ejercicio:

- Obtención de:
  - tiempo actual,
  - previsiones a corto plazo,
  - histórico meteorológico para coordenadas asociadas a aeropuertos.

### 3.3. Alternativa para históricos masivos – Meteostat

- Web: <https://meteostat.net>  

Uso recomendado:

- Generación de `weather_history.csv` para todos los aeropuertos y años definidos, a través de su API o librería Python, como soporte batch.

---

## 4. Stack tecnológico mínimo

Cada equipo utilizará al menos los siguientes componentes:

- **Ingesta y orquestación**:  
  - Apache **NiFi** (consumo de APIs, lectura de ficheros históricos, enrutado y transformación de flujos batch/stream).
- **Bus de eventos**:  
  - Apache **Kafka**.
- **Procesamiento**:
  - **Apache Flink** o **Apache Beam** (ejecutándose sobre Flink u otro runner distribuido) como motor de **stream processing** principal.
  - **PySpark / Apache Spark** como motor principal de **batch** (procesos históricos, preparación de datos para ML, análisis exploratorio).
- **Almacenamiento**:
  - Sistema distribuido: **HDFS**.
  - Base de datos relacional: **MySQL** o **SQL Server**.
  - Base de datos NoSQL:
    - **Cassandra** (series temporales) o
    - **MongoDB** (JSON semiestructurado).
- **Visualización / BI**:
  - **Power BI** como herramienta mínima de explotación.
- **Opcional**:
  - **Apache Druid** como base analítica OLAP de baja latencia para consultas intensivas.

---

## 5. Fases del proyecto y uso de herramientas

### 5.1. Ingesta (NiFi)

- Lectura de ficheros históricos:
  - `Airline_Delay_Cause.csv`, `delays_history_agg.csv`, `delays_history_sample.csv`, `weather_history.csv`.
  - Carga en HDFS (zona *raw* y *curated*).
- Ingesta de streaming desde APIs:
  - Consumir periódicamente OpenSky y OpenWeather.
  - Normalizar respuestas a eventos homogéneos de vuelos y meteorología.
  - Publicar eventos en Kafka (`flights_rt_api`, `weather_rt_api` o `flights_all`, `weather_all`).

### 5.2. Procesamiento batch (PySpark / Spark)

- Procesar datos históricos en HDFS:
  - Limpieza, validación y enriquecimiento de `delays_history_agg`, `delays_history_sample` y `weather_history`.
  - Cálculo de métricas agregadas:
    - retrasos medios,
    - distribuciones por aeropuerto, aerolínea, ruta, franja temporal y condiciones meteorológicas.
  - Generación de tablas de hechos y dimensiones para MySQL/SQL Server.
- Preparación de datasets de entrenamiento y test para el modelo de ML.

### 5.3. Procesamiento streaming (Flink / Beam)

- Lectura desde Kafka:
  - Arquitectura Lambda:
    - `flights_rt_api`, `weather_rt_api`.
  - Arquitectura Kappa:
    - `flights_all`, `weather_all` (histórico + tiempo real).
- Operaciones:
  - Joins entre flujos de vuelos y meteorología por aeropuerto y ventana temporal.
  - Cálculo de KPIs en tiempo casi real:
    - retraso estimado medio,
    - número de vuelos con retraso significativo,
    - niveles de riesgo por aeropuerto.
- Escritura:
  - KPIs en Cassandra/MongoDB.
  - Opcionalmente, publicación de KPIs en Kafka (`kpi_rt`).

### 5.4. Serving y visualización (SQL/NoSQL + Power BI)

- Exposición de:
  - Tablas históricas (MySQL/SQL Server).
  - KPIs en tiempo real (Cassandra/MongoDB o vistas derivadas).
- Construcción de cuadros de mando en Power BI que combinen:
  - análisis histórico,
  - análisis en tiempo casi real.

---

## 6. Monitorización, calidad del dato y gobierno del dato

### 6.1. Monitorización

Cada equipo deberá:

1. Definir y documentar la monitorización de:
   - Flujos de NiFi (colas, backpressure, errores).
   - Topics de Kafka (volumen de mensajes, lag de consumidores).
   - Jobs de Flink/Beam (latencias, throughput, checkpoints).
   - Jobs batch de PySpark (tiempos de ejecución, etapas críticas).

2. Establecer al menos **tres métricas funcionales**, por ejemplo:
   - Número de eventos procesados por minuto y por topic.
   - Número de registros erróneos o rechazados.
   - Retraso medio global y por aeropuerto en las últimas ventanas.

3. Exponer estas métricas mediante:
   - UIs nativas (NiFi, Flink, Spark).
   - Una tabla o vista de “métricas de plataforma” en la base de datos relacional o en una colección NoSQL.

### 6.2. Calidad del dato (Data Quality)

Cada equipo definirá y aplicará una estrategia mínima de calidad del dato que incluya:

1. Reglas de calidad (mínimo 5 reglas), por ejemplo:
   - Campos obligatorios no nulos (`flight_id`, `airport_code`, `flight_date`, etc.).
   - Rangos válidos para retrasos (`arrival_delay_min`, `departure_delay_min`).
   - Rangos razonables para variables meteorológicas (temperatura, viento, visibilidad).
   - Consistencia temporal (`actual_arrival` ≥ `scheduled_departure`, etc.).
   - Coherencia entre variables (por ejemplo, `storm_flag` coherente con `conditions` y nivel de precipitación).

2. Implementación técnica:
   - Validaciones en NiFi (`ValidateRecord`, `RouteOnAttribute`) con rutas de error para registros inválidos.
   - Validaciones adicionales en PySpark sobre datos históricos (expresiones SQL, UDFs o librerías de calidad de datos).
   - Filtrado y conteo de eventos inválidos en Flink/Beam para la parte streaming.

3. Resultados:
   - Porcentajes de registros válidos/erróneos por dataset.
   - Resumen de errores detectados.
   - Un apartado específico de “Calidad del dato” en el informe técnico.

### 6.3. Gobierno del dato (Data Governance)

Cada equipo deberá aportar:

1. **Catálogo mínimo de datos**, con:
   - Datasets de entrada:
     - `Airline_Delay_Cause.csv`,
     - `delays_history_agg.csv`,
     - `delays_history_sample.csv`,
     - `weather_history.csv`.
   - Topics de Kafka:
     - `flights_rt_api`, `weather_rt_api`, `flights_all`, `weather_all`, `kpi_rt`, etc.
   - Tablas y colecciones de salida en MySQL/SQL Server, Cassandra y/o MongoDB.
   - Para cada elemento: nombre, descripción, esquema (columnas y tipos), fuente y frecuencia de actualización.

2. Convenciones y políticas básicas:
   - Patrón de nombres para topics Kafka.
   - Prefijos para capas de datos:
     - `raw_`, `curated_`, `analytics_`, u otros que se definan.
   - Identificación de los datasets considerados “fuente de verdad” para el análisis de retrasos.

3. Trazabilidad (lineage) simplificada:
   - Diagrama de flujo de datos donde se muestre:
     - Orígenes (APIs, ficheros históricos).
     - Procesos intermedios (NiFi, PySpark, Flink/Beam).
     - Destinos (HDFS, bases de datos, dashboards).
   - Puede representarse con diagramas de bloques, BPMN simple o mapas de flujo de datos.

4. SLAs y expectativas:
   - Definir objetivos de latencia para KPIs en tiempo casi real.
   - Definir frecuencia de actualización de datos históricos.
   - Explicar el impacto de incumplir estos SLAs.

---

## 7. Requisitos por tipo de arquitectura

### 7.2. Equipos A y B – Arquitectura Kappa

**Log de eventos unificado**:

- NiFi enviará tanto:
  - Datos históricos (`delays_history_sample.csv`, `weather_history.csv`) como
  - Eventos en tiempo real (OpenSky + OpenWeather)
- A topics unificados, por ejemplo:
  - `flights_all`, `weather_all` (histórico + real).

**Procesamiento unificado**:

- Un único pipeline Flink/Beam en modo streaming que:
  - Lea de `flights_all` y `weather_all`.
  - Realice joins por aeropuerto, ruta y ventanas temporales.
  - Calcule KPIs equivalentes a los de Lambda.
  - Escriba resultados:
    - En Cassandra/MongoDB (KPIs en tiempo real).
    - En MySQL/SQL Server y/o HDFS como batch derivado.

**Reprocesado**:

- Documentar (y, si es posible, demostrar parcialmente) cómo se relanza el pipeline desde el offset 0 de Kafka para recalcular históricos ante cambios en la lógica de negocio.

**Serving y Power BI**:

- Igual que en Lambda, haciendo explícito que todas las vistas batch proceden de reprocesado del log de Kafka.

---

## 8. Entregables, presentación y plazos

### 8.1. Entregables

Cada equipo entregará:

1. **Diagrama de arquitectura** (Lambda o Kappa según corresponda), incluyendo:
   - NiFi, Kafka, Flink/Beam, PySpark/Spark, HDFS, BBDD, Power BI.
   - Flujos de datos batch/stream.
   - Medidas de gobernanza y calidad del dato (filtrado, validaciones, transformaciones).
   - Topics de Kafka y su finalidad.

2. **Implementación técnica**:
   - Export de flujos NiFi (incluyendo llamadas a las APIs).
   - Código de jobs de Flink/Beam.
   - Código de procesos batch en PySpark.
   - Scripts de creación de topics y tablas/colecciones.
   - Evidencias de ejecución (capturas de pantalla o video y consultas de ejemplo).

3. **Cuadro de mando en Power BI**:
   - Mínimo 3 páginas:
     - Visión global por aeropuerto.
     - Relación meteorología–retrasos.
     - Comparativa por aerolínea o ruta.
   - Integración de datos históricos y KPIs en tiempo casi real.

4. **Informe técnico breve** (máx. 5 páginas):
   - Descripción de la arquitectura implementada.
   - Diseño de la ingesta desde las APIs:
     - URLs de referencia,
     - frecuencia de llamadas,
     - parámetros y gestión de API Keys.
   - Estrategia de monitorización, calidad del dato y gobierno del dato.
   - Principales decisiones, problemas encontrados, limitaciones y posibles mejoras.

5. **Presentación oral** (10–15 minutos):
   - Explicación de la solución y defensa de la arquitectura elegida.
   - Demostración del cuadro de mando y del flujo extremo a extremo:
     - desde las APIs y fuentes históricas,
     - hasta los dashboards en Power BI.

### 8.2. Plazos de entrega (1.ª convocatoria)

- **Primera entrega – Jueves 11/12/2025**  
  - Diagrama de arquitectura (Lambda o Kappa).  
  - Primer avance de implementación técnica (NiFi + Kafka + estructura básica de Flink/PySpark).

- **Segunda entrega – Jueves 18/12/2025**  
  - Cuadro de mando en Power BI con datos de prueba suficientemente representativos.

- **Tercera entrega – Jueves 08/01/2025**  
  - Informe técnico breve.  
  - Presentación oral y demostración final.

---

## 9. Referencias sobre uso de las APIs

### 9.1. OpenSky Network

- Web: <https://opensky-network.org>  
- Documentación API: <https://opensky-network.org/data/api>  

Puntos clave:

- API REST con URL base: `https://opensky-network.org/api`
- Endpoints típicos:
  - `GET /states/all`
  - `GET /flights/arrival`
  - `GET /flights/departure`
- Permite uso anónimo con limitaciones de frecuencia; con cuenta, admite autenticación básica HTTP (usuario/contraseña).
- No proporciona directamente un campo de “retraso declarado”; los retrasos se derivan combinando horarios programados y horarios observados.

### 9.2. OpenWeather – One Call API 3.0

- Portal: <https://openweathermap.org/api>  
- One Call 3.0: <https://openweathermap.org/api/one-call-3>  
- Historical Weather: <https://openweathermap.org/history>  

Puntos clave:

- Requiere registro y obtención de una **API Key** (`appid`).
- API REST con parámetros típicos:
  - `lat`, `lon`, `units`, `exclude`, `appid`.
- Permite:
  - tiempo actual,
  - previsiones,
  - histórico por timestamp y coordenadas (según producto contratado).
- La cuenta gratuita tiene límites de peticiones por minuto y por día.

### 9.3. Meteostat

- Web: <https://meteostat.net>  
- Documentación: <https://dev.meteostat.net>  
- Librería Python: <https://dev.meteostat.net/python>  

Puntos clave:

- Proporciona datos meteorológicos históricos diarios y horarios a partir de múltiples fuentes (incluida NOAA).
- Permite construir de forma automatizada `weather_history.csv` para los aeropuertos y años elegidos.
- Útil como fuente batch para el ejercicio y como referencia para validar datos procedentes de otras APIs.


