# Punto 1 · Caso de estudio: resultados Saber 11 (ICFES)

## Contexto

La prueba **Saber 11** del ICFES evalúa a los estudiantes que terminan la educación media en Colombia en cinco
áreas: lectura crítica, matemáticas, sociales y ciudadanas, ciencias naturales e inglés. El ICFES publica
cada periodo los **resultados agregados por institución y jornada** (puntaje promedio y desviación por área,
número de evaluados, municipio, naturaleza oficial/no oficial, calendario).

El proyecto construye un data lake en AWS que integra esos resultados con el **directorio de sedes
educativas (SISE)** para responder preguntas sobre brechas territoriales, por tipo de colegio y en el tiempo.

## Fuentes de datos

| Fuente | Tipo (requisito del proyecto) | Contenido | Dónde está |
|---|---|---|---|
| Resultados agregados Saber 11 (ICFES) | Archivos descargados de URLs | Un Excel por periodo, 2012 a 2025-2, por institución y jornada | Lista de URLs en [`../data/matches.txt`](../data/matches.txt) |
| Directorio de sedes educativas (SISE) | Base de datos relacional (RDS MariaDB) | 84.413 sedes con código DANE, municipio, sector, zona urbana/rural, calendario y coordenadas, normalizadas en 7 tablas | Excel por departamento en [`../data/SISE_departamentos/`](../data/SISE_departamentos/), cargados a RDS por [`../src/scripts/setup_rds.py`](../src/scripts/setup_rds.py) |
| Resultados agregados **sintéticos** | Archivos en EC2 | Reemplazo de los periodos cuyo Excel publicado por el ICFES está corrupto: 2012-2, 2016-2, 2017-2, 2018-1, 2019-1, 2019-2, 2020-4, 2021-4 | [`../data/ec2_files/icfes_saber11_sinteticos/`](../data/ec2_files/icfes_saber11_sinteticos/), generados por [`../src/scripts/generate_synthetic_icfes.py`](../src/scripts/generate_synthetic_icfes.py) |
| Resultados por estudiante (datos.gov.co) | Archivos en EC2 (opcional) | Estrato y educación de los padres, necesarios para P5 | No cargada todavía (ver pendiente abajo) |

Detalle de cada fuente en [`../02-fuentes/`](../02-fuentes/README.md).

## Preguntas de negocio

Tomadas de las consultas del punto 6 ([`consultas.sql`](../06-consultas-sql/consultas.sql)) y del análisis del punto 7
([`analisis_pyspark.ipynb`](../07-pyspark/analisis_pyspark.ipynb)):

1. **P1.** ¿Qué departamentos y municipios tienen el mayor y el menor puntaje global promedio?
2. **P2.** ¿Qué diferencia hay entre colegios oficiales y no oficiales, y cómo cambia según la zona (urbana / rural) de la sede?
3. **P3.** ¿Cuál es el área con peor desempeño en cada región del país?
4. **P4.** ¿Cómo ha evolucionado el puntaje global promedio por departamento año a año?
5. **P5.** ¿Cómo se relacionan el estrato y el nivel educativo de los padres con el puntaje global?
6. **P6.** ¿Cuáles son los 10 mejores colegios de cada departamento y qué tan concentrados están en un mismo municipio?
7. **P7.** ¿Qué tan grande es la brecha entre Bogotá, Medellín, Cali y el resto del país?

Convenciones de cálculo (detalle en [`../README_puntos_4_a_7.md`](../README_puntos_4_a_7.md)):
- Puntaje global = `(3·Lectura + 3·Matemáticas + 3·Sociales + 3·Naturales + 1·Inglés) / 13 × 5` (escala 0-500).
- Promedios territoriales ponderados por número de evaluados.
- "Último periodo" y evolución anual usan solo calendario A.
- Las filas que vienen de archivos sintéticos se marcan con `es_sintetico`.

## Pendientes del equipo

- **TODO (equipo):** decidir P5. Necesita datos por estudiante que no están en los Excel agregados. Opciones:
  cargar el CSV *Resultados únicos Saber 11* de datos.gov.co en `raw/ec2/saber11_individual/`, o cambiar P5 por
  una pregunta que se responda con datos por colegio. El punto 8 hoy no incluye P5.
- **TODO (equipo):** si existe un documento entregado del punto 1 (justificación del caso, enunciado original de
  las preguntas), enlazarlo o copiarlo aquí.
