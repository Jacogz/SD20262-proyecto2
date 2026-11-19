# Punto 4 · Preparación de datos (raw → trusted → refined)

Dos jobs de PySpark que corren **sin cambios** en AWS Glue, Amazon EMR y en local.

| Script | Qué hace | Entrada | Salida |
|---|---|---|---|
| `raw_to_trusted.py` | Limpia, estandariza y une las fuentes | `raw/` | `trusted/` (Parquet) |
| `trusted_to_refined.py` | Calcula una tabla por pregunta de negocio | `trusted/` | `refined/` (Parquet) |
| `crear_roles.sh` | Crea el rol de Glue y los roles de EMR (solo cuenta normal, no Academy) | — | — |
| `crear_glue_jobs.sh` | Sube los scripts, crea los 2 Glue Jobs y el Workflow automático | — | — |
| `emr_ejecutar.sh` | Crea el clúster EMR, lanza los steps y lo termina | — | — |

## Qué hace `raw_to_trusted.py`

**Excel agregados** (`raw/url/icfes_saber11_agregados/` reales + `raw/ec2/icfes_saber11_sinteticos/`)
- Busca la fila de encabezado (el ICFES pone títulos arriba; el encabezado está en la fila 2, 5, 6 o 7 según la época).
- Reconoce los nombres reales (`PROMLECTURACRITICA`, `PROMMATEMATICA`, `PROMINGLES`, `EVALUADO`/`EVALUADOS`, `CODIGODANE_SEDE`…). Si un año trae nombres nuevos, se agregan en `ALIAS_AGREGADOS`.
- Lee `.xlsx` y `.xls`. Un archivo corrupto en el origen o con estructura anterior a 2014 se **omite con aviso** y el log lista cuáles.
- Periodo desde la columna `PERIODO` o, si falta, desde el nombre del archivo (`2023-4-VF` → `20234`, `20181` → `20181`).
- Marca `es_sintetico` según el nombre del archivo. Si un periodo existe real y sintético, gana el real.
- Códigos: `CODINST` (código ICFES, 6 dígitos) se deja como viene; `CODIGODANE_SEDE` a 12 dígitos; municipio DIVIPOLA a 5 dígitos (Excel guarda `5001` y pierde el cero de Antioquia).
- Departamento y región desde el código DIVIPOLA, así **Bogotá D.C. (11) queda separada de Cundinamarca (25)**.
- `prom_global` con la ponderación oficial del ICFES `(3·LC + 3·MAT + 3·SOC + 3·CN + ING) / 13 × 5`, porque los Excel no lo traen.
- Descarta filas sin evaluados o sin puntaje publicado (instituciones con muy pocos evaluados).

**Directorio de sedes** (`raw/rds/sedes_educativas/<tabla>/ingest_date=.../`)
- Lee solo la **`ingest_date` más reciente** de cada tabla (cada corrida de la ingesta deja una copia).
- Une las 7 tablas (sede, municipio, departamento, sector, zona, calendario, estado) en `trusted/sede`.

**Zona de cada fila de resultados** (para P2)
- 2023 en adelante: unión directa por `CODIGODANE_SEDE`.
- Años anteriores: se usa la sede que tuvo la misma institución (`CODINST`) en los periodos que sí traen el código DANE.
- `metodo_zona` dice cuál se usó. El log muestra, por periodo, el porcentaje de filas con zona.

**CSV por estudiante** (`raw/ec2/saber11_individual/`, opcional, necesario para P5): elimina duplicados del portal, convierte estrato y educación de los padres, y **borra el identificador del estudiante**.

## Tablas de `refined/`

| Pregunta | Tabla(s) |
|---|---|
| P1 | `p1_puntaje_departamento`, `p1_puntaje_municipio` |
| P2 | `p2_puntaje_naturaleza_zona`, `p2_brecha_naturaleza_zona` |
| P3 | `p3_area_mas_debil_region` |
| P4 | `p4_evolucion_departamento` (solo calendario A) |
| P5 | `p5_puntaje_estrato`, `p5_puntaje_educ_padres`, `p5_puntaje_estrato_educ_padres`, `p5_correlaciones` (solo si hay datos por estudiante) |
| P6 | `p6_top10_colegios_departamento`, `p6_concentracion_top10_municipio` |
| P7 | `p7_brecha_ciudades_principales` |

Promedios territoriales **ponderados por evaluados**. Todas las tablas por periodo llevan `es_sintetico`; P4 lleva `incluye_sintetico`. Con `--excluir_sinteticos` se calculan solo con datos reales.

## Ejecutar en AWS Glue (exigido)

```bash
export BUCKET=saber11-datalake-XXXXXXXX
bash crear_roles.sh            # cuenta normal. En Academy: export GLUE_ROLE=LabRole y saltar este paso
bash crear_glue_jobs.sh        # sube scripts, crea jobs y el workflow automático
aws glue start-workflow-run --name saber11-pipeline
```

El workflow `saber11-pipeline` corre a diario y encadena raw→trusted → trusted→refined → crawlers (punto 5). `crear_catalogo.sh` debe existir antes de la primera corrida, porque el workflow invoca los crawlers.

## Ejecutar en Amazon EMR (Spark)

```bash
export BUCKET=saber11-datalake-XXXXXXXX
export KEY_NAME=mi-llave        # opcional, para SSH al nodo maestro (en Academy: vockey)
bash emr_ejecutar.sh crear              # devuelve el id j-XXXX
bash emr_ejecutar.sh pasos j-XXXX       # P4 en Spark + P6 en SparkSQL y Hive
bash emr_ejecutar.sh terminar j-XXXX    # SIEMPRE al terminar: cuesta ~US$0,7/hora en cuenta personal
```

## Probar en local
Ver `../pruebas-locales/README.md`.

## Capturas para el documento
1. Glue › Jobs: los dos jobs con su última ejecución en *Succeeded*.
2. Glue › Workflows › `saber11-pipeline`: el grafo y una ejecución exitosa.
3. Log de CloudWatch del job: archivos procesados u omitidos, tabla de periodos real/sintético con `pct_con_zona`, y `Resumen de filas escritas`.
4. S3: carpetas `trusted/` y `refined/` con los Parquet y las particiones `periodo=`.
5. EMR › Steps en *Completed*.
