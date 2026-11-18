"""
Punto 4 (continuación) - Zona TRUSTED -> zona REFINED (silver -> gold).

Construye una tabla agregada por cada pregunta de negocio. Estas tablas son las que
consultan Athena, Hive y Power BI.

Regla clave: los promedios por departamento, municipio o región se calculan PONDERADOS
por el número de evaluados de cada colegio. Promediar los promedios de los colegios sin
ponderar le daría el mismo peso a un colegio de 5 estudiantes que a uno de 500.

Uso:
  spark-submit trusted_to_refined.py --trusted s3://BUCKET/trusted --refined s3://BUCKET/refined
  Opcional: --min_evaluados_top 10    mínimo de evaluados para entrar al top 10 de colegios
            --excluir_sinteticos        deja por fuera los periodos sintéticos (archivos del ICFES corruptos)

Los periodos sintéticos se incluyen por defecto, pero todas las tablas llevan la columna
es_sintetico (o incluye_sintetico en P4) para poder filtrarlos en Athena o Power BI.
"""
import argparse

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

AREAS = ["lectura", "matematicas", "sociales", "naturales", "ingles"]
NOMBRE_AREA = {"lectura": "LECTURA CRITICA", "matematicas": "MATEMATICAS", "sociales": "SOCIALES Y CIUDADANAS",
               "naturales": "CIENCIAS NATURALES", "ingles": "INGLES"}
# Códigos DIVIPOLA de las ciudades principales (pregunta 7).
CIUDADES_PRINCIPALES = {"11001": "BOGOTA D.C.", "05001": "MEDELLIN", "76001": "CALI"}


def log(msg):
    print(f"[trusted_to_refined] {msg}", flush=True)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--trusted", required=True)
    p.add_argument("--refined", required=True)
    p.add_argument("--min_evaluados_top", type=int, default=10)
    p.add_argument("--excluir_sinteticos", action="store_true")
    args, _ = p.parse_known_args()
    return args


def ruta(base, nombre):
    return f"{base.rstrip('/')}/{nombre}"


def leer(spark, base, nombre):
    r = ruta(base, nombre)
    try:
        df = spark.read.parquet(r)
        log(f"Leída {r}")
        return df
    except Exception as e:  # la tabla puede no existir si esa fuente no se cargó
        log(f"AVISO: no se pudo leer {r} ({type(e).__name__}); se omiten las tablas que dependen de ella")
        return None


def escribir(df: DataFrame, base, nombre):
    df.coalesce(1).write.mode("overwrite").parquet(ruta(base, nombre))
    log(f"Escrita {ruta(base, nombre)} ({df.count()} filas)")


def ponderado(col, peso="evaluados"):
    return F.round(F.sum(F.col(col) * F.col(peso)) / F.sum(F.when(F.col(col).isNotNull(), F.col(peso))), 2)


def metricas_ponderadas():
    return [F.countDistinct("cod_colegio").alias("colegios"),
            F.sum("evaluados").alias("evaluados"),
            ponderado("prom_global").alias("prom_global"),
            *[ponderado(f"prom_{a}").alias(f"prom_{a}") for a in AREAS]]


# P1 -------------------------------------------------------------------------
def p1_puntaje_territorial(cp: DataFrame, refined):
    depto = cp.groupBy("periodo", "anio", "es_sintetico", "cod_depto", "departamento", "region").agg(*metricas_ponderadas())
    w = Window.partitionBy("periodo").orderBy(F.desc("prom_global"))
    depto = depto.withColumn("ranking_nacional", F.rank().over(w))
    escribir(depto, refined, "p1_puntaje_departamento")

    mpio = cp.groupBy("periodo", "anio", "es_sintetico", "cod_depto", "departamento", "cod_mpio", "municipio").agg(*metricas_ponderadas())
    w = Window.partitionBy("periodo").orderBy(F.desc("prom_global"))
    mpio = mpio.withColumn("ranking_nacional", F.rank().over(w))
    escribir(mpio, refined, "p1_puntaje_municipio")


# P2 -------------------------------------------------------------------------
def p2_brecha_naturaleza_zona(cp: DataFrame, refined):
    """Oficial vs no oficial según la zona de la sede (urbana/rural, del directorio de RDS)."""
    base = (cp.filter(F.col("zona").isNotNull() & F.col("naturaleza").isNotNull())
              .groupBy("periodo", "es_sintetico", "zona", "naturaleza")
              .agg(F.sum("evaluados").alias("evaluados"), ponderado("prom_global").alias("prom_global")))
    pivot = (base.groupBy("periodo", "es_sintetico", "zona")
                 .agg(F.max(F.when(F.col("naturaleza") == "OFICIAL", F.col("prom_global"))).alias("prom_oficial"),
                      F.max(F.when(F.col("naturaleza") == "NO OFICIAL", F.col("prom_global"))).alias("prom_no_oficial"),
                      F.sum(F.when(F.col("naturaleza") == "OFICIAL", F.col("evaluados"))).alias("evaluados_oficial"),
                      F.sum(F.when(F.col("naturaleza") == "NO OFICIAL", F.col("evaluados"))).alias("evaluados_no_oficial"))
                 .withColumn("brecha_no_oficial_menos_oficial",
                             F.round(F.col("prom_no_oficial") - F.col("prom_oficial"), 2)))
    escribir(base, refined, "p2_puntaje_naturaleza_zona")
    escribir(pivot, refined, "p2_brecha_naturaleza_zona")


# P3 -------------------------------------------------------------------------
def p3_area_mas_debil_region(cp: DataFrame, refined):
    reg = cp.groupBy("periodo", "es_sintetico", "region").agg(
        F.sum("evaluados").alias("evaluados"), *[ponderado(f"prom_{a}").alias(f"prom_{a}") for a in AREAS])
    # Las cinco áreas están en escala 0-100, así que se pueden comparar directamente.
    pares = F.array(*[F.struct(F.col(f"prom_{a}").alias("puntaje"), F.lit(NOMBRE_AREA[a]).alias("area"))
                      for a in AREAS])
    reg = (reg.withColumn("minimo", F.array_min(pares))
              .withColumn("area_mas_debil", F.col("minimo.area"))
              .withColumn("puntaje_area_mas_debil", F.col("minimo.puntaje"))
              .drop("minimo"))
    escribir(reg, refined, "p3_area_mas_debil_region")


# P4 -------------------------------------------------------------------------
def p4_evolucion_departamento(cp: DataFrame, refined):
    # Solo calendario A: los periodos de calendario B (semestre 1) evalúan ~340 colegios, casi todos
    # privados bilingües, y mezclarlos con el A dispara el promedio de ese año.
    anual = cp.filter(F.col("semestre") != 1).groupBy("anio", "cod_depto", "departamento", "region").agg(
        F.sum("evaluados").alias("evaluados"), ponderado("prom_global").alias("prom_global"),
        F.max("es_sintetico").alias("incluye_sintetico"))
    w = Window.partitionBy("cod_depto").orderBy("anio")
    anual = (anual.withColumn("prom_global_anio_anterior", F.lag("prom_global").over(w))
                  .withColumn("variacion", F.round(F.col("prom_global") - F.col("prom_global_anio_anterior"), 2))
                  .withColumn("primer_anio", F.first("prom_global").over(w.rowsBetween(Window.unboundedPreceding, 0)))
                  .withColumn("variacion_acumulada", F.round(F.col("prom_global") - F.col("primer_anio"), 2))
                  .drop("primer_anio"))
    escribir(anual, refined, "p4_evolucion_departamento")


# P5 -------------------------------------------------------------------------
def p5_estrato_padres(est: DataFrame, refined):
    estrato = (est.filter(F.col("estrato").isNotNull())
                  .groupBy("periodo", "estrato")
                  .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("prom_global")))
    padres = (est.filter(F.col("nivel_educ_padres").isNotNull())
                 .groupBy("periodo", "nivel_educ_padres", "nivel_educ_padres_txt")
                 .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("prom_global")))
    cruce = (est.filter(F.col("estrato").isNotNull() & F.col("nivel_educ_padres").isNotNull())
                .groupBy("periodo", "estrato", "nivel_educ_padres", "nivel_educ_padres_txt")
                .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("prom_global")))
    corr = (est.groupBy("periodo")
               .agg(F.round(F.corr(F.col("estrato").cast("double"), "punt_global"), 3).alias("corr_estrato_puntaje"),
                    F.round(F.corr(F.col("nivel_educ_padres").cast("double"), "punt_global"), 3)
                     .alias("corr_educ_padres_puntaje")))
    escribir(estrato, refined, "p5_puntaje_estrato")
    escribir(padres, refined, "p5_puntaje_educ_padres")
    escribir(cruce, refined, "p5_puntaje_estrato_educ_padres")
    escribir(corr, refined, "p5_correlaciones")


# P6 -------------------------------------------------------------------------
def p6_top10_colegios(cp: DataFrame, refined, min_evaluados):
    # Un colegio puede aparecer varias veces en un periodo (una fila por jornada): se consolida.
    colegio = (cp.groupBy("periodo", "es_sintetico", "cod_depto", "departamento", "cod_mpio", "municipio",
                          "cod_colegio", "naturaleza")
                 .agg(F.first("nombre_colegio", ignorenulls=True).alias("nombre_colegio"),
                      F.sum("evaluados").alias("evaluados"),
                      ponderado("prom_global").alias("prom_global"))
                 .filter(F.col("evaluados") >= min_evaluados))
    w = Window.partitionBy("periodo", "cod_depto").orderBy(F.desc("prom_global"), F.desc("evaluados"))
    top = colegio.withColumn("puesto", F.row_number().over(w)).filter(F.col("puesto") <= 10)
    concentracion = (top.groupBy("periodo", "es_sintetico", "cod_depto", "departamento", "cod_mpio", "municipio")
                        .agg(F.count("*").alias("colegios_en_top10"))
                        .withColumn("pct_del_top10_depto",
                                    F.round(F.col("colegios_en_top10") * 100.0
                                            / F.sum("colegios_en_top10").over(Window.partitionBy("periodo", "cod_depto")), 1)))
    escribir(top, refined, "p6_top10_colegios_departamento")
    escribir(concentracion, refined, "p6_concentracion_top10_municipio")


# P7 -------------------------------------------------------------------------
def p7_brecha_ciudades(cp: DataFrame, refined):
    grupo = F.coalesce(F.create_map(*[x for k, v in CIUDADES_PRINCIPALES.items()
                                       for x in (F.lit(k), F.lit(v))])[F.col("cod_mpio")], F.lit("RESTO DEL PAIS"))
    base = (cp.withColumn("grupo", grupo)
              .groupBy("periodo", "es_sintetico", "grupo")
              .agg(F.sum("evaluados").alias("evaluados"), ponderado("prom_global").alias("prom_global")))
    resto = base.filter(F.col("grupo") == "RESTO DEL PAIS").select("periodo", F.col("prom_global").alias("prom_resto"))
    base = (base.join(resto, "periodo", "left")
                .withColumn("diferencia_vs_resto", F.round(F.col("prom_global") - F.col("prom_resto"), 2))
                .drop("prom_resto"))
    escribir(base, refined, "p7_brecha_ciudades_principales")


def main():
    args = parse_args()
    spark = (SparkSession.builder.appName("saber11-trusted-to-refined")
             # Que "periodo=20242" se lea como texto (igual que en Glue/Athena) y no como entero.
             .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
             .getOrCreate())

    cp = leer(spark, args.trusted, "colegio_periodo")
    est = leer(spark, args.trusted, "estudiante")

    if cp is None:
        raise SystemExit("No existe trusted/colegio_periodo: ejecute primero raw_to_trusted.py")
    if args.excluir_sinteticos:
        cp = cp.filter(~F.col("es_sintetico"))
        log("Se excluyen los periodos sintéticos")
    cp = cp.cache()
    p1_puntaje_territorial(cp, args.refined)
    p2_brecha_naturaleza_zona(cp, args.refined)
    p3_area_mas_debil_region(cp, args.refined)
    p4_evolucion_departamento(cp, args.refined)
    p6_top10_colegios(cp, args.refined, args.min_evaluados_top)
    p7_brecha_ciudades(cp, args.refined)

    if est is not None:
        p5_estrato_padres(est.cache(), args.refined)
    else:
        log("AVISO: no hay datos por estudiante (trusted/estudiante); no se calcula P5 (estrato y educación de los padres)")

    spark.stop()


if __name__ == "__main__":
    main()
