-- Generado a partir de consultas.sql para ejecutarse en Hive (EMR): hive -f consultas_hive.sql
USE saber11;

-- =============================================================================
-- Punto 6 - Consultas analíticas descriptivas de las 7 preguntas de negocio.
--
-- El MISMO archivo corre sin cambios en los tres motores:
--   * AWS Athena   : base de datos "saber11" seleccionada en el editor
--   * EMR / Hive   : hive -f consultas_hive.sql   (igual a este con "USE saber11;" al inicio)
--   * SparkSQL     : python sparksql_consultas.py  (lo lee y ejecuta consulta por consulta)
--
-- Tablas (creadas por los crawlers de Glue, punto 5):
--   trusted_colegio_periodo  resultados por colegio-jornada y periodo (ICFES), con zona de la sede
--   trusted_sede             directorio de sedes educativas (RDS MariaDB)
--   trusted_estudiante       resultados por estudiante (solo si se carga esa fuente; ver P5)
--
-- Los promedios territoriales se ponderan por número de evaluados.
-- "Último periodo" = último periodo de CALENDARIO A (semestre 2 o 4). Los periodos de calendario B
-- (semestre 1) evalúan solo ~340 colegios, casi todos privados bilingües: mezclarlos con el
-- calendario A distorsiona rankings y tendencias.
-- es_sintetico = true marca los periodos cuyo archivo del ICFES llegó corrupto y se reemplazó
-- por datos sintéticos (ver src/scripts/generate_synthetic_icfes.py).
-- Cada consulta empieza con "-- @Pnn: título", que usan sparksql_consultas.py y athena_ejecutar.py.
-- Las consultas de P5 van al final: requieren trusted_estudiante y Hive se detiene en el primer error.
-- =============================================================================


-- @P1a: Departamentos ordenados por puntaje global promedio (último periodo)
SELECT c.departamento,
       c.region,
       SUM(c.evaluados)                                              AS evaluados,
       ROUND(SUM(c.prom_global * c.evaluados) / SUM(c.evaluados), 2) AS prom_global
FROM trusted_colegio_periodo c
JOIN (SELECT MAX(periodo) AS periodo FROM trusted_colegio_periodo WHERE semestre <> 1) u ON c.periodo = u.periodo
GROUP BY c.departamento, c.region
ORDER BY prom_global DESC;


-- @P1b: Los 10 municipios con mayor y menor puntaje (mínimo 100 evaluados, último periodo)
SELECT *
FROM (
    SELECT departamento, municipio, evaluados, prom_global,
           RANK() OVER (ORDER BY prom_global DESC) AS puesto_mejor,
           RANK() OVER (ORDER BY prom_global ASC)  AS puesto_peor
    FROM (
        SELECT c.departamento, c.municipio,
               SUM(c.evaluados)                                              AS evaluados,
               ROUND(SUM(c.prom_global * c.evaluados) / SUM(c.evaluados), 2) AS prom_global
        FROM trusted_colegio_periodo c
        JOIN (SELECT MAX(periodo) AS periodo FROM trusted_colegio_periodo WHERE semestre <> 1) u ON c.periodo = u.periodo
        GROUP BY c.departamento, c.municipio
        HAVING SUM(c.evaluados) >= 100
    ) m
) r
WHERE puesto_mejor <= 10 OR puesto_peor <= 10
ORDER BY prom_global DESC;


-- @P2: Brecha oficial vs no oficial según la zona de la sede (urbana / rural), por periodo
SELECT periodo,
       es_sintetico,
       zona,
       ROUND(SUM(CASE WHEN naturaleza = 'OFICIAL'    THEN prom_global * evaluados END)
           / SUM(CASE WHEN naturaleza = 'OFICIAL'    THEN evaluados END), 2)             AS prom_oficial,
       ROUND(SUM(CASE WHEN naturaleza = 'NO OFICIAL' THEN prom_global * evaluados END)
           / SUM(CASE WHEN naturaleza = 'NO OFICIAL' THEN evaluados END), 2)             AS prom_no_oficial,
       ROUND(SUM(CASE WHEN naturaleza = 'NO OFICIAL' THEN prom_global * evaluados END)
           / SUM(CASE WHEN naturaleza = 'NO OFICIAL' THEN evaluados END)
           - SUM(CASE WHEN naturaleza = 'OFICIAL'    THEN prom_global * evaluados END)
           / SUM(CASE WHEN naturaleza = 'OFICIAL'    THEN evaluados END), 2)             AS brecha,
       SUM(evaluados)                                                                    AS evaluados
FROM trusted_colegio_periodo
WHERE zona IS NOT NULL AND naturaleza IS NOT NULL
GROUP BY periodo, es_sintetico, zona
ORDER BY periodo, zona;


-- @P3: Área con peor desempeño en cada región (último periodo)
SELECT region,
       prom_lectura, prom_matematicas, prom_sociales, prom_naturales, prom_ingles,
       CASE LEAST(prom_lectura, prom_matematicas, prom_sociales, prom_naturales, prom_ingles)
            WHEN prom_lectura     THEN 'LECTURA CRITICA'
            WHEN prom_matematicas THEN 'MATEMATICAS'
            WHEN prom_sociales    THEN 'SOCIALES Y CIUDADANAS'
            WHEN prom_naturales   THEN 'CIENCIAS NATURALES'
            ELSE 'INGLES'
       END AS area_mas_debil
FROM (
    SELECT c.region,
           ROUND(SUM(c.prom_lectura     * c.evaluados) / SUM(c.evaluados), 2) AS prom_lectura,
           ROUND(SUM(c.prom_matematicas * c.evaluados) / SUM(c.evaluados), 2) AS prom_matematicas,
           ROUND(SUM(c.prom_sociales    * c.evaluados) / SUM(c.evaluados), 2) AS prom_sociales,
           ROUND(SUM(c.prom_naturales   * c.evaluados) / SUM(c.evaluados), 2) AS prom_naturales,
           ROUND(SUM(c.prom_ingles      * c.evaluados) / SUM(c.evaluados), 2) AS prom_ingles
    FROM trusted_colegio_periodo c
    JOIN (SELECT MAX(periodo) AS periodo FROM trusted_colegio_periodo WHERE semestre <> 1) u ON c.periodo = u.periodo
    GROUP BY c.region
) r
ORDER BY region;


-- @P4: Evolución anual del puntaje global por departamento y variación frente al año anterior
SELECT departamento,
       anio,
       prom_global,
       ROUND(prom_global - LAG(prom_global) OVER (PARTITION BY departamento ORDER BY anio), 2) AS variacion,
       incluye_sintetico
FROM (
    SELECT departamento, anio,
           ROUND(SUM(prom_global * evaluados) / SUM(evaluados), 2) AS prom_global,
           MAX(CASE WHEN es_sintetico THEN 1 ELSE 0 END)           AS incluye_sintetico
    FROM trusted_colegio_periodo
    WHERE semestre <> 1          -- solo calendario A (ver nota arriba)
    GROUP BY departamento, anio
) a
ORDER BY departamento, anio;


-- @P6a: Top 10 colegios por departamento (último periodo, mínimo 10 evaluados)
SELECT departamento, puesto, nombre_colegio, municipio, naturaleza, evaluados, prom_global
FROM (
    SELECT departamento, municipio, cod_colegio, nombre_colegio, naturaleza, evaluados, prom_global,
           ROW_NUMBER() OVER (PARTITION BY departamento ORDER BY prom_global DESC, evaluados DESC) AS puesto
    FROM (
        SELECT c.departamento, c.municipio, c.cod_colegio, c.naturaleza,
               MAX(c.nombre_colegio)                                         AS nombre_colegio,
               SUM(c.evaluados)                                              AS evaluados,
               ROUND(SUM(c.prom_global * c.evaluados) / SUM(c.evaluados), 2) AS prom_global
        FROM trusted_colegio_periodo c
        JOIN (SELECT MAX(periodo) AS periodo FROM trusted_colegio_periodo WHERE semestre <> 1) u ON c.periodo = u.periodo
        GROUP BY c.departamento, c.municipio, c.cod_colegio, c.naturaleza
        HAVING SUM(c.evaluados) >= 10
    ) col
) t
WHERE puesto <= 10
ORDER BY departamento, puesto;


-- @P6b: Cuántos colegios del top 10 de cada departamento están en el mismo municipio
SELECT departamento, municipio, COUNT(*) AS colegios_en_top10
FROM (
    SELECT departamento, municipio,
           ROW_NUMBER() OVER (PARTITION BY departamento ORDER BY prom_global DESC, evaluados DESC) AS puesto
    FROM (
        SELECT c.departamento, c.municipio, c.cod_colegio,
               SUM(c.evaluados)                                              AS evaluados,
               ROUND(SUM(c.prom_global * c.evaluados) / SUM(c.evaluados), 2) AS prom_global
        FROM trusted_colegio_periodo c
        JOIN (SELECT MAX(periodo) AS periodo FROM trusted_colegio_periodo WHERE semestre <> 1) u ON c.periodo = u.periodo
        GROUP BY c.departamento, c.municipio, c.cod_colegio
        HAVING SUM(c.evaluados) >= 10
    ) col
) t
WHERE puesto <= 10
GROUP BY departamento, municipio
ORDER BY departamento, colegios_en_top10 DESC;


-- @P7: Brecha entre Bogotá, Medellín, Cali y el resto del país, por periodo
SELECT periodo,
       es_sintetico,
       grupo,
       SUM(evaluados)                                          AS evaluados,
       ROUND(SUM(prom_global * evaluados) / SUM(evaluados), 2) AS prom_global
FROM (
    SELECT periodo, es_sintetico, evaluados, prom_global,
           CASE cod_mpio
                WHEN '11001' THEN 'BOGOTA D.C.'
                WHEN '05001' THEN 'MEDELLIN'
                WHEN '76001' THEN 'CALI'
                ELSE 'RESTO DEL PAIS'
           END AS grupo
    FROM trusted_colegio_periodo
) g
GROUP BY periodo, es_sintetico, grupo
ORDER BY periodo, prom_global DESC;


-- ---------------------------------------------------------------------------
-- P5 requiere datos POR ESTUDIANTE (estrato y educación de los padres no están en los
-- archivos agregados por colegio). Solo funcionan si se carga trusted_estudiante.
-- ---------------------------------------------------------------------------

-- @P5a: Puntaje global promedio por estrato
SELECT estrato,
       COUNT(*)                   AS estudiantes,
       ROUND(AVG(punt_global), 2) AS prom_global
FROM trusted_estudiante
WHERE estrato IS NOT NULL
GROUP BY estrato
ORDER BY estrato;


-- @P5b: Puntaje global según el máximo nivel educativo de los padres
SELECT nivel_educ_padres_txt,
       COUNT(*)                   AS estudiantes,
       ROUND(AVG(punt_global), 2) AS prom_global
FROM trusted_estudiante
WHERE nivel_educ_padres IS NOT NULL
GROUP BY nivel_educ_padres_txt
ORDER BY nivel_educ_padres_txt;


-- @P5c: Correlación de Pearson entre estrato, educación de los padres y puntaje global
SELECT ROUND(CORR(CAST(estrato AS DOUBLE), punt_global), 3)           AS corr_estrato,
       ROUND(CORR(CAST(nivel_educ_padres AS DOUBLE), punt_global), 3) AS corr_educ_padres
FROM trusted_estudiante;
