Proyecto PSet 2 - Pipeline ELT NYC Taxi (Mage + PostgreSQL)

Autor
Tais Rodriguez
USFQ - Maestría en Ciencia de Datos

I. Descripción general
Este proyecto implementa un pipeline ELT de extremo a extremo para la ingesta, procesamiento y modelado de datos de taxis de Nueva York utilizando Mage AI, PostgreSQL y Docker.
La arquitectura sigue un enfoque por capas:
    Capa raw: ingesta de datos con transformaciones mínimas
    Capa clean: transformación y modelado analítico

II. Arquitectura

Fuente de datos NYC TLC
        ↓
Mage - raw_ingestion_pipeline
        ↓
PostgreSQL (schema: raw)
        ↓
Mage - clean_transformation_pipeline
        ↓
PostgreSQL (schema: clean)
        ↓
pgAdmin (validación)

III. Pipelines
    1. Pipeline de ingesta (raw)
Descarga datos en formato Parquet
Estandariza columnas
Carga datos en `raw.taxi_trips_raw`
Implementa carga **idempotente**
Registra logs en `raw.load_log`

    2. Pipeline de transformación (clean)
Aplica reglas de calidad de datos:
    - elimina valores negativos
    - valida fechas
    - elimina viajes con distancia 0
Construye un modelo dimensional (star schema)

IV. Tabla de hechos
`clean.fact_trips`

V. Tablas de dimensión
`clean.dim_vendor`
`clean.dim_payment_type`
`clean.dim_pickup_location`
`clean.dim_dropoff_location`

VI. Validación de datos
Registros en raw: ~1.25 millones
Registros en fact: ~1.07 millones
La reducción se debe a los filtros de calidad y a la eliminación de registros inválidos. 

VII. Orquestación
Se configuraron triggers programados con freuencia diaria para raw_ingestion_pipeline a las 8:00 y clean_transformation_pipeline a las 8:30.
De esta forma nos aseguramos que primero se crague el raw y luego se transforme el clean. 

VII. Infraestructura
Docker (contenedores)
PostgreSQL (data warehouse)
Mage (orquestación)
pgAdmin (consulta y validación)

VIII. Evidencia
Se incluyen capturas de:
    - contenedores Docker activos
    - pipelines en Mage
    - ejecuciones exitosas
    - schemas raw y clean
    - resultados de queries


