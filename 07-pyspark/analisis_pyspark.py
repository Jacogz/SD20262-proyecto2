# %% [markdown]
# # Punto 7 - Análisis descriptivo de Saber 11 con PySpark
#
# Responde las 7 preguntas de negocio usando **solo la API de DataFrames de PySpark**
# (sin SQL), sobre las tablas de la zona *trusted*.
#
# - En **EMR** (EMR Studio o JupyterHub): cambie `TRUSTED` por `s3://SU_BUCKET/trusted`.
# - En **local**: deje la ruta de la muestra de prueba.
#
# Los gráficos se guardan en la carpeta `graficos/` para el documento.
#
# Notas de método:
# - Los promedios territoriales se ponderan por número de evaluados.
# - "Último periodo" y la evolución anual usan solo **calendario A**: los periodos de calendario B
#   evalúan ~340 colegios, casi todos privados bilingües, y distorsionan rankings y tendencias.
# - `es_sintetico` marca los periodos cuyo archivo del ICFES llegó corrupto y se reemplazó con datos
#   sintéticos (generate_synthetic_icfes.py).
#
# > **Atención:** las salidas guardadas en este notebook y los gráficos actuales de `graficos/`
# > salen de una **prueba local** (archivos sintéticos del repositorio). Se reemplazan al ejecutar
# > el notebook en EMR sobre el bucket real.

# %%
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

TRUSTED = os.environ.get("TRUSTED", "../pruebas-locales/prueba/trusted")   # en EMR: "s3://SU_BUCKET/trusted"
GRAFICOS = os.environ.get("GRAFICOS", "graficos")
os.makedirs(GRAFICOS, exist_ok=True)

spark = (SparkSession.builder.appName("saber11-pyspark")
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

cp = spark.read.parquet(f"{TRUSTED}/colegio_periodo").cache()
try:
    est = spark.read.parquet(f"{TRUSTED}/estudiante").cache()   # fuente opcional (P5)
except Exception:
    est = None

ultimo = cp.filter(F.col("semestre") != 1).agg(F.max("periodo")).first()[0]   # último de calendario A
cp_ult = cp.filter(F.col("periodo") == ultimo)
print(f"Colegio-periodo: {cp.count()} filas | Estudiantes: {est.count() if est is not None else 'no cargados'}"
      f" | Último periodo (calendario A): {ultimo}")
cp.groupBy("periodo", "es_sintetico").agg(F.count("*").alias("filas"), F.sum("evaluados").alias("evaluados")) \
  .orderBy("periodo").show(40)


def prom_ponderado(col, peso="evaluados"):
    """Promedio ponderado por número de evaluados."""
    return F.round(F.sum(F.col(col) * F.col(peso)) / F.sum(peso), 2)


# %% [markdown]
# ## P1. Departamentos y municipios con mayor y menor puntaje global

# %%
p1_depto = (cp_ult.groupBy("departamento", "region")
                  .agg(F.sum("evaluados").alias("evaluados"), prom_ponderado("prom_global").alias("prom_global"))
                  .orderBy(F.desc("prom_global")))
p1_depto.show(40, truncate=False)

p1_mpio = (cp_ult.groupBy("departamento", "municipio")
                 .agg(F.sum("evaluados").alias("evaluados"), prom_ponderado("prom_global").alias("prom_global"))
                 .filter(F.col("evaluados") >= 100))
print("Mejores municipios:")
p1_mpio.orderBy(F.desc("prom_global")).show(10, truncate=False)
print("Peores municipios:")
p1_mpio.orderBy("prom_global").show(10, truncate=False)

pdf = p1_depto.toPandas().sort_values("prom_global")
fig, ax = plt.subplots(figsize=(8, max(3, 0.35 * len(pdf))))
ax.barh(pdf["departamento"], pdf["prom_global"], color="#2b6c8f")
ax.set_xlabel("Puntaje global promedio (0-500)")
ax.set_title(f"P1. Puntaje global por departamento, periodo {ultimo}")
fig.tight_layout(); fig.savefig(f"{GRAFICOS}/p1_departamentos.png", dpi=150); plt.close(fig)

# %% [markdown]
# ## P2. Oficiales vs no oficiales según la zona
# La zona (urbana/rural) viene del directorio de sedes de RDS, unido por el código DANE de la sede.

# %%
p2 = (cp_ult.filter(F.col("zona").isNotNull() & F.col("naturaleza").isNotNull())
            .groupBy("zona")
            .pivot("naturaleza", ["OFICIAL", "NO OFICIAL"])
            .agg(prom_ponderado("prom_global"))
            .withColumn("brecha", F.round(F.col("NO OFICIAL") - F.col("OFICIAL"), 2)))
print(f"Periodo {ultimo}. Filas con zona asignada: "
      f"{cp_ult.filter(F.col('zona').isNotNull()).count()} de {cp_ult.count()}")
p2.show(truncate=False)

pdf = p2.toPandas().set_index("zona")[["OFICIAL", "NO OFICIAL"]]
ax = pdf.plot(kind="bar", figsize=(7.5, 4), color=["#2b6c8f", "#e0a03a"], rot=0)
ax.set_ylabel("Puntaje global promedio")
ax.set_xlabel("")
ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1))
ax.set_title(f"P2. Puntaje por naturaleza del colegio y zona, periodo {ultimo}")
plt.tight_layout(); plt.savefig(f"{GRAFICOS}/p2_naturaleza_zona.png", dpi=150); plt.close()

# %% [markdown]
# ## P3. Área con peor desempeño por región

# %%
areas = {"prom_lectura": "LECTURA CRITICA", "prom_matematicas": "MATEMATICAS",
         "prom_sociales": "SOCIALES Y CIUDADANAS", "prom_naturales": "CIENCIAS NATURALES",
         "prom_ingles": "INGLES"}
p3 = cp_ult.groupBy("region").agg(*[prom_ponderado(c).alias(c) for c in areas])
# Se pasa a formato largo (región, área, puntaje) y se toma el mínimo por región.
largo = p3.select("region", F.explode(F.array(*[
    F.struct(F.lit(n).alias("area"), F.col(c).alias("puntaje")) for c, n in areas.items()])).alias("x")
).select("region", "x.area", "x.puntaje")
w = Window.partitionBy("region").orderBy("puntaje")
p3_min = largo.withColumn("pos", F.row_number().over(w)).filter("pos = 1").drop("pos")
p3.show(truncate=False)
p3_min.orderBy("region").show(truncate=False)

pdf = p3.toPandas().set_index("region").rename(columns=areas)
ax = pdf.plot(kind="bar", figsize=(10, 4.5), rot=0)
ax.set_ylabel("Puntaje promedio (0-100)")
ax.set_xlabel("")
ax.set_title(f"P3. Puntaje por área y región, periodo {ultimo}")
ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
plt.tight_layout(); plt.savefig(f"{GRAFICOS}/p3_areas_region.png", dpi=150); plt.close()

# %% [markdown]
# ## P4. Evolución del puntaje por departamento

# %%
p4 = (cp.filter(F.col("semestre") != 1)          # solo calendario A
        .groupBy("departamento", "anio")
        .agg(prom_ponderado("prom_global").alias("prom_global"),
             F.max("es_sintetico").alias("incluye_sintetico"))
        .withColumn("variacion", F.round(
            F.col("prom_global") - F.lag("prom_global").over(Window.partitionBy("departamento").orderBy("anio")), 2)))
p4.orderBy("departamento", "anio").show(60, truncate=False)

# Para que el gráfico se lea: promedio nacional + los 3 mejores y 3 peores departamentos del último año.
nacional = (cp.filter(F.col("semestre") != 1).groupBy("anio")
              .agg(prom_ponderado("prom_global").alias("prom_global"), F.max("es_sintetico").alias("sint"))
              .orderBy("anio").toPandas().set_index("anio"))
tabla = p4.toPandas().pivot(index="anio", columns="departamento", values="prom_global").sort_index()
ultimo_anio = tabla.index.max()
orden = tabla.loc[ultimo_anio].dropna().sort_values()
elegidos = list(orden.index[-3:][::-1]) + list(orden.index[:3])
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(nacional.index, nacional["prom_global"], color="black", linewidth=3, marker="o", label="NACIONAL")
for d in elegidos:
    ax.plot(tabla.index, tabla[d], marker="o", linewidth=1.5, label=d)
etiquetas = [f"{a}*" if nacional.loc[a, "sint"] else str(a) for a in nacional.index]
ax.set_xticks(nacional.index); ax.set_xticklabels(etiquetas)
ax.set_ylabel("Puntaje global promedio")
ax.set_xlabel("Año   (* = periodo con datos sintéticos)")
ax.set_title("P4. Evolución del puntaje global: nacional, 3 mejores y 3 peores departamentos")
ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
fig.tight_layout(); fig.savefig(f"{GRAFICOS}/p4_evolucion.png", dpi=150); plt.close(fig)

# %% [markdown]
# ## P5. Estrato, educación de los padres y puntaje

# %%
if est is None:
    print("P5 no se puede calcular: requiere datos por estudiante (estrato y educación de los padres),\n"
          "que no vienen en los archivos agregados por colegio. Ver README del punto 7.")
else:
    p5_estrato = (est.filter(F.col("estrato").isNotNull()).groupBy("estrato")
                     .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("prom_global"))
                     .orderBy("estrato"))
    p5_padres = (est.filter(F.col("nivel_educ_padres").isNotNull()).groupBy("nivel_educ_padres_txt")
                    .agg(F.count("*").alias("estudiantes"), F.round(F.avg("punt_global"), 2).alias("prom_global"))
                    .orderBy("nivel_educ_padres_txt"))
    p5_estrato.show(truncate=False)
    p5_padres.show(truncate=False)

    # stat.corr no descarta nulos por sí solo: se filtran antes para que coincida con CORR() de SQL.
    corr_estrato = est.filter(F.col("estrato").isNotNull()).stat.corr("estrato", "punt_global")
    corr_padres = est.filter(F.col("nivel_educ_padres").isNotNull()).stat.corr("nivel_educ_padres", "punt_global")
    print(f"Correlación estrato-puntaje: {corr_estrato:.3f}")
    print(f"Correlación educación padres-puntaje: {corr_padres:.3f}")

    matriz = (est.filter(F.col("estrato").isNotNull() & F.col("nivel_educ_padres").isNotNull())
                 .groupBy("estrato").pivot("nivel_educ_padres").agg(F.round(F.avg("punt_global"), 1))
                 .orderBy("estrato").toPandas().set_index("estrato"))
    fig, ax = plt.subplots(figsize=(7, 4.5))
    im = ax.imshow(matriz.values, cmap="YlGnBu", aspect="auto")
    ax.set_xticks(range(len(matriz.columns))); ax.set_xticklabels(matriz.columns)
    ax.set_yticks(range(len(matriz.index))); ax.set_yticklabels(matriz.index)
    ax.set_xlabel("Máximo nivel educativo de los padres (0 ninguno - 5 posgrado)")
    ax.set_ylabel("Estrato")
    ax.set_title("P5. Puntaje global promedio por estrato y educación de los padres")
    fig.colorbar(im, ax=ax, label="Puntaje global")
    fig.tight_layout(); fig.savefig(f"{GRAFICOS}/p5_estrato_padres.png", dpi=150); plt.close(fig)

# %% [markdown]
# ## P6. Top 10 colegios por departamento y concentración por municipio

# %%
colegio = (cp_ult.groupBy("departamento", "municipio", "cod_colegio", "naturaleza")
                 .agg(F.first("nombre_colegio", ignorenulls=True).alias("nombre_colegio"),
                      F.sum("evaluados").alias("evaluados"),
                      prom_ponderado("prom_global").alias("prom_global"))
                 .filter(F.col("evaluados") >= 10))
w = Window.partitionBy("departamento").orderBy(F.desc("prom_global"), F.desc("evaluados"))
top10 = colegio.withColumn("puesto", F.row_number().over(w)).filter(F.col("puesto") <= 10)
top10.orderBy("departamento", "puesto").show(40, truncate=False)

concentracion = (top10.groupBy("departamento", "municipio").count()
                      .withColumnRenamed("count", "colegios_en_top10")
                      .orderBy("departamento", F.desc("colegios_en_top10")))
concentracion.show(40, truncate=False)

print("Naturaleza de los colegios del top 10:")
top10.groupBy("naturaleza").count().show()

# %% [markdown]
# ## P7. Brecha entre Bogotá, Medellín, Cali y el resto del país

# %%
grupo = (F.when(F.col("cod_mpio") == "11001", "BOGOTA D.C.")
          .when(F.col("cod_mpio") == "05001", "MEDELLIN")
          .when(F.col("cod_mpio") == "76001", "CALI")
          .otherwise("RESTO DEL PAIS"))
p7 = (cp.withColumn("grupo", grupo).groupBy("periodo", "grupo")
        .agg(F.sum("evaluados").alias("evaluados"), prom_ponderado("prom_global").alias("prom_global")))
resto = p7.filter(F.col("grupo") == "RESTO DEL PAIS").select("periodo", F.col("prom_global").alias("resto"))
p7 = p7.join(resto, "periodo").withColumn("diferencia_vs_resto", F.round(F.col("prom_global") - F.col("resto"), 2))
p7.drop("resto").orderBy("periodo", F.desc("prom_global")).show(40, truncate=False)

pdf = p7.toPandas().pivot(index="periodo", columns="grupo", values="prom_global").sort_index()
ax = pdf.plot(kind="bar", figsize=(10, 4.5), rot=0)
ax.set_ylabel("Puntaje global promedio")
ax.set_xlabel("Periodo")
ax.set_title("P7. Ciudades principales frente al resto del país")
ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1.01, 1))
plt.tight_layout(); plt.savefig(f"{GRAFICOS}/p7_ciudades.png", dpi=150); plt.close()

# %%
print("Gráficos generados:", sorted(os.listdir(GRAFICOS)))
spark.stop()
