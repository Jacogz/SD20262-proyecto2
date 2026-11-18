# Proyecto 2 · Data lake de resultados Saber 11 (ICFES) en AWS

Big Data · Universidad EAFIT · 2026-2

## Equipo
- Jacobo Giraldo Zuluaga
- Paulina Velásquez Londoño
- Mariana García Zapata
- Juan José Álvarez Ocampo

## Arquitectura

```
 FUENTES                    INGESTA                 DATA LAKE (S3)                PROCESAMIENTO / CATÁLOGO        CONSUMO
 ───────                    ───────                 ──────────────                ────────────────────────        ───────
 RDS MariaDB (SISE)  ─┐                           ┌─ raw/      (CSV, Excel)  ──┐   Glue Jobs / EMR Spark         Athena
 URLs ICFES          ─┼─▶ EC2 + ingest_to_s3.py ─▶│                           ├─▶ (raw→trusted→refined)    ┌─▶ Hive (EMR)
 Archivos en EC2     ─┘   (timer diario)          ├─ trusted/  (Parquet)  ◀──┤   Glue Workflow diario     │   SparkSQL / PySpark
                                                  └─ refined/  (Parquet)  ◀──┘   Glue Crawlers ──▶ Glue Data Catalog
                                                                                  (base saber11, metastore de Hive)
 raw/ ──▶ build_refined.py ──▶ refined/api/*.json ──▶ Lambda ──▶ API Gateway ──▶ Streamlit (EC2)       (punto 8)
```

## Estructura del repositorio

| Punto | Carpeta | Contenido |
|---|---|---|
| 1 | [`01-caso-estudio/`](01-caso-estudio/README.md) | Caso de estudio, fuentes y las 7 preguntas de negocio |
| 2 | [`02-fuentes/`](02-fuentes/README.md) | RDS MariaDB desde el SISE, URLs del ICFES, archivos sintéticos en EC2 (código en `src/` y `data/`) |
| 3 | [`03-ingesta/`](03-ingesta/README.md) | Bucket, EC2 de ingesta y carga automática a `s3://BUCKET/raw/` (código en `src/`) |
| 4 | [`04-preparacion/`](04-preparacion/README.md) | Jobs PySpark raw→trusted→refined en Glue (con Workflow) y EMR |
| 5 | [`05-catalogacion/`](05-catalogacion/README.md) | Glue Crawlers, base `saber11`, EMR con Glue Catalog como metastore de Hive |
| 6 | [`06-consultas-sql/`](06-consultas-sql/README.md) | Consultas de las 7 preguntas en Athena, Hive y SparkSQL |
| 7 | [`07-pyspark/`](07-pyspark/README.md) | Notebook de análisis con la API de DataFrames de PySpark y gráficos |
| 8 | [`08-visualizacion-api/`](08-visualizacion-api/README.md) | Lambda + API Gateway y app Streamlit |

Otras carpetas:
- [`src/`](src/) y [`data/`](data/): código y datos de los puntos 2 y 3. **No se mueven**: los scripts dependen de esas rutas.
- [`pruebas-locales/`](pruebas-locales/README.md): arma un `raw/` local con el formato real para probar los puntos 4 a 7 sin AWS.
- [`README_puntos_4_a_7.md`](README_puntos_4_a_7.md): decisiones de diseño, tablas de refined, costos y pruebas de los puntos 4 a 7.

## Ejecución de punta a punta

Todos los comandos se corren desde la raíz del repositorio. En **AWS Academy** no se crean roles: antes de
empezar, `export GLUE_ROLE=LabRole KEY_NAME=vockey` y usar `LabRole` donde se pida un rol.

```bash
# --- Puntos 2 y 3: fuentes e ingesta (ver 02-fuentes/ y 03-ingesta/) ---
cp src/infra/config.env.example src/infra/config.env      # editar
# .env en la raíz con DATABASE_MASTER_ENDPOINT / USER / PASSWORD
python src/scripts/setup_rds.py                            # carga la RDS (ver nota de ruta en 02-fuentes/)
bash src/infra/01_create_datalake.sh                       # bucket y zonas
bash src/infra/02_launch_ec2.sh                            # EC2 de ingesta
bash src/infra/03_deploy_ec2.sh                            # despliega y ejecuta la ingesta → raw/

# --- Puntos 4 y 5: preparación y catalogación ---
export BUCKET=<nombre-del-bucket>
bash 04-preparacion/crear_roles.sh                         # solo cuenta normal (no Academy)
bash 04-preparacion/crear_glue_jobs.sh                     # 2 Glue Jobs + workflow saber11-pipeline
bash 05-catalogacion/crear_catalogo.sh                     # base saber11 + crawlers (antes de correr el workflow)
aws glue start-workflow-run --name saber11-pipeline        # raw→trusted→refined→crawlers

# --- Punto 6: consultas ---
python3 06-consultas-sql/athena_ejecutar.py --bucket $BUCKET
bash 04-preparacion/emr_ejecutar.sh crear                  # devuelve j-XXXX
bash 04-preparacion/emr_ejecutar.sh pasos j-XXXX           # Spark + SparkSQL + Hive

# --- Punto 7: notebook 07-pyspark/analisis_pyspark.ipynb en JupyterHub del clúster ---
bash 04-preparacion/emr_ejecutar.sh terminar j-XXXX        # SIEMPRE al terminar

# --- Punto 8: visualización y API ---
python 08-visualizacion-api/src/build_refined.py --bucket $BUCKET --out 08-visualizacion-api/src/out
BUCKET=$BUCKET ROLE_ARN=arn:aws:iam::<CUENTA>:role/LabRole bash 08-visualizacion-api/api/deploy_api.sh
API_URL=https://<api-id>.execute-api.us-east-1.amazonaws.com streamlit run 08-visualizacion-api/src/app.py
```

Cada carpeta explica sus requisitos, parámetros y las capturas de evidencia que se esperan.

## Pruebas sin AWS
- Puntos 4 a 7: [`pruebas-locales/README.md`](pruebas-locales/README.md).
- Punto 8: sección *Prueba local sin AWS* de [`08-visualizacion-api/README.md`](08-visualizacion-api/README.md).

## Seguridad
`.env`, `src/infra/config.env`, `src/infra/keys/` y `src/infra/.ec2_state` están en `.gitignore`: credenciales,
llaves y estado local no se suben al repositorio.
