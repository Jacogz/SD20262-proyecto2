# Punto 5 · Catalogación (AWS Glue Data Catalog + EMR/Hive)

## 1. Glue Crawlers

```bash
export BUCKET=su-bucket
bash crear_catalogo.sh
```

Crea la base `saber11` y dos crawlers con el rol de Glue (`saber11-glue-role`, creado por `04-preparacion/crear_roles.sh`; en Academy, `export GLUE_ROLE=LabRole`):

| Crawler | Lee | Tablas que crea |
|---|---|---|
| `saber11-trusted` | `s3://BUCKET/trusted/<tabla>/` | `trusted_colegio_periodo`, `trusted_sede`, `trusted_departamento` y `trusted_estudiante` (si se carga esa fuente) |
| `saber11-refined` | `s3://BUCKET/refined/<tabla>/` | `p1_...` a `p7_...` (13 tablas) |

Se declara un destino S3 **por tabla** para que Glue no fusione carpetas con esquemas parecidos en una sola tabla. `trusted_colegio_periodo` (y `trusted_estudiante`, si existe) quedan particionadas por `periodo`.

El workflow del punto 4 vuelve a ejecutar los crawlers cada vez que se actualizan los datos.

## 2. EMR / Hive

El clúster de `04-preparacion/emr_ejecutar.sh` se crea con `emr-configuracion-glue-catalog.json`, que hace que **Hive y Spark usen el Glue Data Catalog como metastore**. Por eso las tablas de los crawlers aparecen directamente en Hive:

```bash
# Conectarse al nodo maestro (EMR > clúster > "Connect to the Primary node using SSM", o SSH con KEY_NAME)
hive
SHOW DATABASES;
USE saber11;
SHOW TABLES;
DESCRIBE FORMATTED trusted_colegio_periodo;
SHOW PARTITIONS trusted_colegio_periodo;
```

Para demostrar además la catalogación hecha **desde Hive**, `hive_ddl.sql` crea tablas externas en una base aparte (`saber11_hive`):

```bash
sed -i 's/TU_BUCKET/su-bucket/g' hive_ddl.sql
hive -f hive_ddl.sql
```

## Capturas para el documento
1. Glue › Crawlers: ambos crawlers en *Ready* con la última ejecución *Succeeded*.
2. Glue › Databases › `saber11`: lista de tablas.
3. Esquema de `trusted_colegio_periodo` mostrando la partición `periodo`.
4. Terminal de Hive con `SHOW TABLES` en `saber11` y en `saber11_hive`.
