# Punto 6 · Consultas SQL con Athena, EMR/Hive y SparkSQL

`consultas.sql` tiene las consultas de las 7 preguntas (11 consultas: P1a, P1b, P2, P3, P4, P5a-c, P6a-b, P7). **El mismo SQL corre en los tres motores**, lo que permite comparar resultados.

| Archivo | Motor |
|---|---|
| `consultas.sql` | Athena (consola o `athena_ejecutar.py`) |
| `consultas_hive.sql` | Hive: es `consultas.sql` con `USE saber11;` al inicio |
| `sparksql_consultas.py` | SparkSQL: lee `consultas.sql` y ejecuta cada consulta |

Las consultas trabajan sobre las tablas **trusted**: el cálculo se hace en SQL, no se leen respuestas ya calculadas.

- "Último periodo" es el último de **calendario A**; P4 también usa solo calendario A.
- P2, P4 y P7 muestran `es_sintetico` / `incluye_sintetico` para distinguir periodos con datos sintéticos.
- **P5a-c requieren `trusted_estudiante`** (datos por estudiante). Están al final del archivo porque Hive se detiene en el primer error; si esa fuente no se carga, fallan sin afectar a las demás.

## Athena

1. Consola de Athena → *Settings* → ubicación de resultados: `s3://BUCKET/athena-results/`.
2. Base de datos: `saber11`.
3. Pegar cada consulta del archivo y ejecutar. O todas desde la terminal:

```bash
python athena_ejecutar.py --bucket su-bucket
python athena_ejecutar.py --bucket su-bucket --solo P5
```

## Hive (EMR)

```bash
# como step (lo hace emr_ejecutar.sh pasos):  Type=Hive, -f s3://BUCKET/scripts/consultas_hive.sql
# o en el nodo maestro:
hive -f consultas_hive.sql
```

## SparkSQL

```bash
# EMR, usando el Glue Data Catalog:
spark-submit --files consultas.sql sparksql_consultas.py --modo catalogo --sql consultas.sql \
  --salida s3://BUCKET/refined/consultas_sparksql

# Local, sobre la muestra:
python sparksql_consultas.py --modo parquet --trusted ../pruebas-locales/prueba/trusted --salida ./resultados
```

Guarda el resultado de cada consulta como CSV en `refined/consultas_sparksql/<id>/`.

## Capturas para el documento
1. Editor de Athena con una consulta y su resultado (mínimo P1a, P2, P4 y P7), mostrando los datos escaneados.
2. Salida de Hive (stdout del step o terminal) con al menos 2 consultas.
3. Salida del step de SparkSQL con al menos 2 consultas.
4. Una misma consulta con el mismo resultado en los tres motores.
