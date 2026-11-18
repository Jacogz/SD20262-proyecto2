-- =============================================================================
-- Punto 5 - Catalogación con EMR / Hive.
--
-- Opción A (recomendada): el clúster EMR usa el Glue Data Catalog como metastore
--   (emr-configuracion-glue-catalog.json). Las tablas que crearon los crawlers ya
--   aparecen en Hive; basta con:
--       SHOW DATABASES;
--       USE saber11;
--       SHOW TABLES;
--       DESCRIBE FORMATTED trusted_colegio_periodo;
--
-- Opción B: crear las tablas directamente desde Hive con este DDL, en una base aparte
--   (saber11_hive) para demostrar la catalogación hecha por Hive.
--   Reemplace TU_BUCKET por el nombre real del bucket antes de ejecutar:
--       sed -i 's/TU_BUCKET/mi-bucket/g' hive_ddl.sql && hive -f hive_ddl.sql
-- Hive asocia las columnas de Parquet por NOMBRE, así que el orden no importa.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS saber11_hive
COMMENT 'Tablas Saber 11 catalogadas desde Hive (EMR)';

USE saber11_hive;

-- Trusted: resultados por colegio-jornada y periodo, con zona de la sede
CREATE EXTERNAL TABLE IF NOT EXISTS trusted_colegio_periodo (
  anio INT, semestre INT, es_sintetico BOOLEAN,
  cod_colegio STRING, cod_dane_sede STRING, cod_dane_sede_ref STRING, nombre_colegio STRING,
  cod_mpio STRING, municipio STRING, cod_depto STRING, departamento STRING, region STRING,
  calendario STRING, naturaleza STRING, jornada STRING,
  zona STRING, metodo_zona STRING, estado_sede STRING, latitud DOUBLE, longitud DOUBLE,
  evaluados INT,
  prom_lectura DOUBLE, prom_matematicas DOUBLE, prom_sociales DOUBLE, prom_naturales DOUBLE,
  prom_ingles DOUBLE, prom_global DOUBLE,
  desv_lectura DOUBLE, desv_matematicas DOUBLE, desv_sociales DOUBLE, desv_naturales DOUBLE,
  desv_ingles DOUBLE, archivo_origen STRING
)
PARTITIONED BY (periodo STRING)
STORED AS PARQUET
LOCATION 's3://TU_BUCKET/trusted/colegio_periodo/';

MSCK REPAIR TABLE trusted_colegio_periodo;

-- Trusted: directorio de sedes educativas (une las 7 tablas de RDS MariaDB)
CREATE EXTERNAL TABLE IF NOT EXISTS trusted_sede (
  cod_dane_sede STRING, id_sede INT, nombre_sede STRING, cod_mpio STRING, municipio STRING,
  cod_depto STRING, departamento STRING, region STRING, naturaleza STRING, zona STRING,
  estado STRING, calendario STRING, latitud DOUBLE, longitud DOUBLE
)
STORED AS PARQUET
LOCATION 's3://TU_BUCKET/trusted/sede/';

-- Refined: P1
CREATE EXTERNAL TABLE IF NOT EXISTS p1_puntaje_departamento (
  periodo STRING, anio INT, es_sintetico BOOLEAN, cod_depto STRING, departamento STRING, region STRING,
  colegios BIGINT, evaluados BIGINT, prom_global DOUBLE, prom_lectura DOUBLE, prom_matematicas DOUBLE,
  prom_sociales DOUBLE, prom_naturales DOUBLE, prom_ingles DOUBLE, ranking_nacional INT
)
STORED AS PARQUET
LOCATION 's3://TU_BUCKET/refined/p1_puntaje_departamento/';

-- Refined: P7
CREATE EXTERNAL TABLE IF NOT EXISTS p7_brecha_ciudades_principales (
  periodo STRING, es_sintetico BOOLEAN, grupo STRING, evaluados BIGINT, prom_global DOUBLE,
  diferencia_vs_resto DOUBLE
)
STORED AS PARQUET
LOCATION 's3://TU_BUCKET/refined/p7_brecha_ciudades_principales/';

SHOW TABLES;
SHOW PARTITIONS trusted_colegio_periodo;
SELECT periodo, es_sintetico, COUNT(*) AS filas FROM trusted_colegio_periodo GROUP BY periodo, es_sintetico;
