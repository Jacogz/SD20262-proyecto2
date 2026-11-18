"""
Punto 6 - Ejecuta con SparkSQL las mismas consultas de consultas.sql.

Dos modos:
  --modo catalogo  (EMR): usa las tablas del Glue Data Catalog (base saber11).
                   Requiere el clúster configurado con el Glue Catalog como metastore (punto 5).
  --modo parquet   (local o EMR sin catálogo): lee los Parquet de trusted y registra vistas temporales
                   con los mismos nombres de tabla.

Guarda el resultado de cada consulta como CSV en --salida (ej. s3://BUCKET/refined/consultas_sparksql).

Ejemplos:
  spark-submit sparksql_consultas.py --modo catalogo --base_datos saber11 --salida s3://BUCKET/refined/consultas_sparksql
  python sparksql_consultas.py --modo parquet --trusted ../pruebas-locales/trusted_prueba --salida ./resultados
"""
import argparse
import os
import re

from pyspark.sql import SparkSession

TABLAS_TRUSTED = {"trusted_colegio_periodo": "colegio_periodo",
                  "trusted_sede": "sede",
                  "trusted_estudiante": "estudiante"}


def leer_consultas(ruta_sql):
    """Separa consultas.sql en bloques [(id, titulo, sql)] usando las líneas '-- @Pxx: título'."""
    with open(ruta_sql, encoding="utf-8") as f:
        texto = f.read()
    bloques = re.split(r"^-- @(P\w+): (.*)$", texto, flags=re.M)
    consultas = []
    for i in range(1, len(bloques), 3):
        cuerpo = "\n".join(l for l in bloques[i + 2].splitlines() if not l.strip().startswith("--"))
        consultas.append((bloques[i], bloques[i + 1].strip(), cuerpo.strip().rstrip(";")))
    return consultas


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--modo", choices=["catalogo", "parquet"], default="catalogo")
    p.add_argument("--base_datos", default="saber11")
    p.add_argument("--trusted", help="Ruta de trusted (solo modo parquet)")
    p.add_argument("--salida", required=True)
    p.add_argument("--sql", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "consultas.sql"))
    args, _ = p.parse_known_args()

    builder = (SparkSession.builder.appName("saber11-sparksql")
               .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false"))
    if args.modo == "catalogo":
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if args.modo == "catalogo":
        spark.sql(f"USE {args.base_datos}")
    else:
        for vista, carpeta in TABLAS_TRUSTED.items():
            ruta = f"{args.trusted.rstrip('/')}/{carpeta}"
            try:
                spark.read.parquet(ruta).createOrReplaceTempView(vista)
                print(f"Vista {vista} <- {ruta}")
            except Exception:
                print(f"AVISO: no existe {ruta}; las consultas que usan {vista} fallarán")

    # El archivo SQL viaja junto al script; en EMR puede pasarse con --files o leerse de S3 copiado localmente.
    for id_consulta, titulo, sql in leer_consultas(args.sql):
        print(f"\n===== {id_consulta}: {titulo} =====")
        try:
            df = spark.sql(sql)
            df.show(20, truncate=False)
            (df.coalesce(1).write.mode("overwrite").option("header", True)
               .csv(f"{args.salida.rstrip('/')}/{id_consulta.lower()}"))
        except Exception as e:
            print(f"ERROR en {id_consulta}: {e}")

    spark.stop()


if __name__ == "__main__":
    main()
