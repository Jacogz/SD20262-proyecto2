# Proyecto 2 · Saber 11 · Puntos 4 a 7 (Spark, catalogación y consultas)

Responsable: Juan José Álvarez Ocampo.

| Carpeta | Punto | Contenido |
|---|---|---|
| `04-preparacion/` | 4 | Jobs PySpark raw→trusted→refined, Glue Jobs + Workflow, ejecución en EMR, roles de IAM |
| `05-catalogacion/` | 5 | Glue Crawlers, configuración de EMR con Glue Catalog, DDL de Hive |
| `06-consultas-sql/` | 6 | 11 consultas de las 7 preguntas, en Athena, Hive y SparkSQL |
| `07-pyspark/` | 7 | Notebook PySpark (API de DataFrames) y gráficos |
| `pruebas-locales/` | — | Arma un raw local con el formato real a partir del repositorio del equipo |

## Fuentes que leen los jobs (las deja la ingesta, `src/scripts/ingest_to_s3.py`)

```
s3://BUCKET/raw/
├── url/icfes_saber11_agregados/          Excel del ICFES por colegio-jornada (2012 a 2025-2)
├── ec2/icfes_saber11_sinteticos/         Excel SINTÉTICOS de los periodos cuyo archivo del ICFES llega corrupto
├── rds/sedes_educativas/<tabla>/ingest_date=AAAA-MM-DD/<tabla>.csv    7 tablas del directorio de sedes (SISE)
├── ec2/saber11_individual/               (opcional) CSV por estudiante de datos.gov.co — necesario para P5
└── _manifests/                           registro de cada corrida de la ingesta (no lo usan los jobs)
```

Los jobs escriben `trusted/`, `refined/` y usan `scripts/`, `tmp/` y `athena-results/` en el mismo bucket.

## Decisiones de diseño

1. **Columnas reales del ICFES.** Los Excel usan `PROMLECTURACRITICA`, `PROMMATEMATICA`, `PROMINGLES`, `EVALUADO`/`EVALUADOS`, etc. (no los nombres del diccionario de la web). El encabezado está en la fila 2, 5, 6 o 7 según la época; el job busca la fila que contiene `CODINST`.
2. **`CODINST` es el código ICFES de la institución (6 dígitos), no el código DANE.** El código DANE de la sede solo viene como `CODIGODANE_SEDE` en los archivos de 2023 en adelante.
3. **Zona urbana/rural (P2).** Sale del directorio de sedes de RDS. En 2023+ se une por `CODIGODANE_SEDE`; para años anteriores se usa la sede que tuvo esa misma institución en los periodos que sí la traen. La columna `metodo_zona` dice cuál de los dos se usó.
4. **Datos sintéticos.** Los periodos cuyo Excel llega corrupto se reemplazan con los sintéticos de EC2. Todas las tablas llevan `es_sintetico` (o `incluye_sintetico` en P4) para filtrarlos en Athena o Power BI. Si un periodo existe real y sintético, gana el real. Con `--excluir_sinteticos` el job de refined los deja por fuera.
5. **Calendario A.** "Último periodo" y la evolución anual (P4) usan solo calendario A (semestre 2 o 4). Los periodos de calendario B evalúan unos 340 colegios, casi todos privados bilingües; mezclarlos dispara el promedio del año (en las pruebas, Antioquia saltaba de 255 a 327 en 2018).
6. **Puntaje global.** Los Excel no lo traen: se calcula con la ponderación oficial `(3·LC + 3·MAT + 3·SOC + 3·CN + ING) / 13 × 5`.
7. **Promedios territoriales ponderados por evaluados.**
8. **Bogotá D.C. separada de Cundinamarca**, y región natural asignada por código DIVIPOLA del departamento.
9. **Robustez:** un Excel corrupto o con estructura anterior a 2014 se omite con aviso (no detiene el job); de RDS se lee solo la `ingest_date` más reciente.

## Preguntas de negocio: numeración final

El equipo dejó **6 preguntas de negocio** en el punto 1 y descartó la de estrato y educación de los padres. El código la sigue soportando si se cargan datos por estudiante en `raw/ec2/saber11_individual/`; mientras tanto, refined omite P5 con un aviso y las consultas P5a-c de `consultas.sql` fallan de forma esperada y **no hacen parte de la entrega**.

Equivalencia entre el código y el documento: **P1-P4 del código = Preguntas 1-4 del documento; P6 del código = Pregunta 5 (top 10 colegios); P7 del código = Pregunta 6 (brecha ciudades).**

## Orden de ejecución en AWS

```bash
export BUCKET=saber11-datalake-XXXXXXXX
bash 04-preparacion/crear_roles.sh                # solo cuenta normal (no Academy)
bash 04-preparacion/crear_glue_jobs.sh            # punto 4: jobs + workflow
bash 05-catalogacion/crear_catalogo.sh            # punto 5: base saber11 + crawlers
aws glue start-workflow-run --name saber11-pipeline   # corre raw→trusted→refined→crawlers
python3 06-consultas-sql/athena_ejecutar.py --bucket $BUCKET   # punto 6: Athena
bash 04-preparacion/emr_ejecutar.sh crear         # EMR: Spark, Hive, JupyterHub
bash 04-preparacion/emr_ejecutar.sh pasos j-XXXX  # P4 en Spark + P6 en SparkSQL y Hive
# punto 7: notebook en JupyterHub del clúster
bash 04-preparacion/emr_ejecutar.sh terminar j-XXXX
```

**AWS Academy:** no se ejecuta `crear_roles.sh`; antes de los demás scripts: `export GLUE_ROLE=LabRole KEY_NAME=vockey`.

### Bucket en otra cuenta

Lo más simple es correr todo en la cuenta dueña del bucket. Si Glue o EMR corren en otra cuenta, el dueño del bucket debe permitir el acceso al rol que los ejecuta, con una política de bucket como esta (reemplazando la cuenta y el rol):

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"AWS": ["arn:aws:iam::CUENTA_QUE_EJECUTA:role/saber11-glue-role",
                          "arn:aws:iam::CUENTA_QUE_EJECUTA:role/EMR_EC2_DefaultRole"]},
    "Action": ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
    "Resource": ["arn:aws:s3:::BUCKET", "arn:aws:s3:::BUCKET/*"]
  }]
}
```

## Costos (cuenta personal)
- **EMR:** 3 × m5.xlarge, aproximadamente US$0,7 por hora. Se apaga solo tras 1 hora sin uso, pero **termínelo a mano al acabar**.
- **Glue:** 2 workers G.1X por job; con estos volúmenes (~30 MB) cada corrida tarda pocos minutos.
- **Athena:** cobra por datos escaneados; trusted y refined están en Parquet.
- El workflow corre a diario: si no lo necesitan, desactiven el trigger `saber11-inicio`.
- Recomendado: una alerta en **Billing → Budgets** antes de empezar.

## Pruebas realizadas
Se probó todo en local con PySpark 3.5.3, sobre un raw con el formato real (`pruebas-locales/preparar_raw_desde_repo.py`): los 8 Excel sintéticos del repositorio, las 7 tablas de RDS construidas con `setup_rds.build_tables()` (84.413 sedes del SISE, dos fechas de ingesta), un Excel moderno con `CODIGODANE_SEDE` y uno corrupto. Resultado: 9 Excel leídos y 1 omitido, 91.282 filas en `colegio_periodo`, zona asignada a ~94 % de las filas de calendario A, las 13 tablas de refined (sin P5), las 8 consultas de P1-P4, P6 y P7 en SparkSQL y el notebook completo. Glue, EMR, Athena y Hive no se pudieron probar porque requieren la cuenta de AWS.

## Para el punto 8 (visualización y API)

Las tablas listas para graficar quedan en `s3://BUCKET/refined/` (Parquet) y, después de los crawlers, en la base `saber11` de Athena:

| Tabla | Para qué sirve | Columnas clave |
|---|---|---|
| `p1_puntaje_departamento` | Ranking / mapa por departamento | `periodo`, `departamento`, `region`, `prom_global`, `ranking_nacional`, `evaluados` |
| `p1_puntaje_municipio` | Ranking por municipio | `periodo`, `departamento`, `municipio`, `prom_global`, `evaluados` |
| `p2_brecha_naturaleza_zona` | Oficial vs no oficial por zona | `periodo`, `zona`, `prom_oficial`, `prom_no_oficial`, `brecha_no_oficial_menos_oficial` |
| `p3_area_mas_debil_region` | Áreas por región | `periodo`, `region`, `prom_lectura`…`prom_ingles`, `area_mas_debil` |
| `p4_evolucion_departamento` | Línea de tiempo | `anio`, `departamento`, `prom_global`, `variacion`, `incluye_sintetico` |
| `p6_top10_colegios_departamento` | Tabla de top 10 | `periodo`, `departamento`, `puesto`, `nombre_colegio`, `municipio`, `prom_global` |
| `p7_brecha_ciudades_principales` | Ciudades vs resto | `periodo`, `grupo`, `prom_global`, `diferencia_vs_resto` |

- `trusted_colegio_periodo` trae `latitud` y `longitud` de la sede, útiles para un mapa.
- Filtren o señalen `es_sintetico = true` (periodos con datos sintéticos).
- Power BI se conecta a Athena con el conector **Amazon Athena** (ODBC); Streamlit puede leer las mismas tablas con `awswrangler` o `pyathena`.
