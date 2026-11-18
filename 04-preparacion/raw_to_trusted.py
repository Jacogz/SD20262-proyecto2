"""
Punto 4 - Preparación de datos: zona RAW -> zona TRUSTED (bronze -> silver).

Corre igual en:
  - AWS Glue 4.0 (job tipo Spark / glueetl)
  - Amazon EMR (spark-submit)
  - Local (pip install pyspark pandas openpyxl xlrd)

Fuentes que lee de RAW (estructura que deja la ingesta src/scripts/ingest_to_s3.py):
  1. raw/url/icfes_saber11_agregados/          Excel agregados del ICFES por colegio-jornada (fuente URL)
  2. raw/ec2/icfes_saber11_sinteticos/         Excel SINTÉTICOS de los periodos cuyo archivo del ICFES
                                               llega corrupto desde el origen (fuente EC2)
  3. raw/rds/sedes_educativas/<tabla>/ingest_date=AAAA-MM-DD/<tabla>.csv
                                               Directorio de sedes (SISE) exportado de RDS MariaDB
  4. raw/ec2/saber11_individual/  (opcional)   CSV por estudiante de datos.gov.co, si se agrega

Tablas que escribe en TRUSTED (Parquet):
  - colegio_periodo   resultados por colegio, jornada y periodo, con zona de la sede (particionado por periodo)
  - sede              directorio de sedes educativas (une las 7 tablas de RDS)
  - departamento      referencia DIVIPOLA: código, nombre y región natural
  - estudiante        solo si existe la fuente 4 (particionado por periodo)

Uso:
  spark-submit raw_to_trusted.py --raw s3://BUCKET/raw --trusted s3://BUCKET/trusted
"""
import argparse
import io
import os
import re
import sys
import unicodedata

import pandas as pd
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ---------------------------------------------------------------------------
# Referencias fijas
# ---------------------------------------------------------------------------

# Códigos DIVIPOLA de departamento -> (nombre oficial, región natural predominante).
# Bogotá D.C. (11) es una entidad territorial propia, NO parte de Cundinamarca (25).
DEPARTAMENTOS = {
    "05": ("ANTIOQUIA", "ANDINA"), "08": ("ATLANTICO", "CARIBE"), "11": ("BOGOTA D.C.", "ANDINA"),
    "13": ("BOLIVAR", "CARIBE"), "15": ("BOYACA", "ANDINA"), "17": ("CALDAS", "ANDINA"),
    "18": ("CAQUETA", "AMAZONIA"), "19": ("CAUCA", "PACIFICA"), "20": ("CESAR", "CARIBE"),
    "23": ("CORDOBA", "CARIBE"), "25": ("CUNDINAMARCA", "ANDINA"), "27": ("CHOCO", "PACIFICA"),
    "41": ("HUILA", "ANDINA"), "44": ("LA GUAJIRA", "CARIBE"), "47": ("MAGDALENA", "CARIBE"),
    "50": ("META", "ORINOQUIA"), "52": ("NARIÑO", "PACIFICA"), "54": ("NORTE DE SANTANDER", "ANDINA"),
    "63": ("QUINDIO", "ANDINA"), "66": ("RISARALDA", "ANDINA"), "68": ("SANTANDER", "ANDINA"),
    "70": ("SUCRE", "CARIBE"), "73": ("TOLIMA", "ANDINA"), "76": ("VALLE DEL CAUCA", "PACIFICA"),
    "81": ("ARAUCA", "ORINOQUIA"), "85": ("CASANARE", "ORINOQUIA"), "86": ("PUTUMAYO", "AMAZONIA"),
    "88": ("SAN ANDRES", "INSULAR"), "91": ("AMAZONAS", "AMAZONIA"), "94": ("GUAINIA", "AMAZONIA"),
    "95": ("GUAVIARE", "AMAZONIA"), "97": ("VAUPES", "AMAZONIA"), "99": ("VICHADA", "ORINOQUIA"),
}

# Columnas destino -> nombres posibles en los Excel del ICFES (ya normalizados: mayúsculas, sin
# tildes, sin espacios, guiones ni signos; "CODIGODANE_SEDE" -> "CODIGODANESEDE").
ALIAS_AGREGADOS = {
    "cod_colegio": ["CODINST"],                       # código ICFES de la institución (6 dígitos)
    "cod_dane_sede": ["CODIGODANESEDE", "CODDANESEDE"],  # código DANE de la sede (12 dígitos, desde 2023)
    "nombre_colegio": ["NOMBREINSTITUCION", "NOMBREESTABLECIMIENTO"],
    "cod_mpio": ["CODIGOMUNICIPIO", "CODMUNICIPIO"],
    "municipio": ["NOMBREMUNICIPIO", "MUNICIPIO"],
    "departamento_original": ["DEPARTAMENTO"],
    "calendario": ["CALENDARIO"],
    "naturaleza": ["NATURALEZA"],
    "jornada": ["JORNADA"],
    "evaluados": ["EVALUADO", "EVALUADOS", "NUMEROEVALUADOS"],
    "prom_lectura": ["PROMLECTURACRITICA", "PROMEDIOLECTURACRITICA"],
    "prom_matematicas": ["PROMMATEMATICA", "PROMMATEMATICAS", "PROMEDIOMATEMATICAS", "PROMEDIOMATEMATICA"],
    "prom_sociales": ["PROMSOCIALESYCIUDADANAS", "PROMEDIOSOCIALESYCIUDADANAS"],
    "prom_naturales": ["PROMCIENCIASNATURALES", "PROMEDIOCIENCIASNATURALES"],
    "prom_ingles": ["PROMINGLES", "PROMEDIOINGLES"],
    "desv_lectura": ["DESVLECTURACRITICA", "DESVIACIONLECTURACRITICA"],
    "desv_matematicas": ["DESVMATEMATICA", "DESVMATEMATICAS", "DESVIACIONMATEMATICAS"],
    "desv_sociales": ["DESVSOCIALESYCIUDADANAS", "DESVIACIONSOCIALESYCIUDADANAS"],
    "desv_naturales": ["DESVCIENCIASNATURALES", "DESVIACIONCIENCIASNATURALES"],
    "desv_ingles": ["DESVINGLES", "DESVIACIONINGLES"],
    "periodo": ["PERIODO"],
}
OBLIGATORIAS_AGREGADOS = ["cod_colegio", "cod_mpio", "naturaleza", "evaluados",
                          "prom_lectura", "prom_matematicas", "prom_sociales",
                          "prom_naturales", "prom_ingles"]

AREAS = ["lectura", "matematicas", "sociales", "naturales", "ingles"]

# Tablas de la BD sedes_educativas (src/scripts/setup_rds.py)
TABLAS_RDS = ["sede_educativa", "municipio", "departamento", "sector_educativo",
              "zona_geografica", "tipo_calendario", "estado_sede"]

# CSV por estudiante (nombres de la API de datos.gov.co) -> nombre destino. Fuente opcional.
COLUMNAS_ESTUDIANTE = {
    "periodo": "periodo", "estu_consecutivo": "estu_consecutivo",
    "cole_cod_dane_establecimiento": "cod_dane_establecimiento",
    "cole_nombre_establecimiento": "nombre_colegio",
    "cole_cod_mcpio_ubicacion": "cod_mpio", "cole_mcpio_ubicacion": "municipio",
    "cole_naturaleza": "naturaleza", "cole_area_ubicacion": "zona",
    "cole_calendario": "calendario", "cole_jornada": "jornada",
    "fami_estratovivienda": "estrato_original", "fami_educacionpadre": "educ_padre",
    "fami_educacionmadre": "educ_madre", "fami_tieneinternet": "tiene_internet",
    "punt_lectura_critica": "punt_lectura", "punt_matematicas": "punt_matematicas",
    "punt_sociales_ciudadanas": "punt_sociales", "punt_c_naturales": "punt_naturales",
    "punt_ingles": "punt_ingles", "punt_global": "punt_global",
}


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    print(f"[raw_to_trusted] {msg}", flush=True)


def normalizar_nombre(texto) -> str:
    s = unicodedata.normalize("NFKD", str(texto)).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^A-Z0-9]", "", s.upper())


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--raw", required=True, help="Zona raw, ej. s3://bucket/raw")
    p.add_argument("--trusted", required=True, help="Zona trusted, ej. s3://bucket/trusted")
    p.add_argument("--carpetas_agregados", default="url/icfes_saber11_agregados,ec2/icfes_saber11_sinteticos",
                   help="Carpetas con Excel agregados, separadas por coma")
    p.add_argument("--carpeta_rds", default="rds/sedes_educativas")
    p.add_argument("--carpeta_estudiantes", default="ec2/saber11_individual")
    args, _ = p.parse_known_args()  # Glue agrega argumentos propios (--JOB_NAME, etc.)
    return args


def unir_ruta(base: str, *partes: str) -> str:
    return "/".join([base.rstrip("/")] + [x.strip("/") for x in partes])


def es_s3(ruta: str) -> bool:
    return ruta.startswith(("s3://", "s3a://"))


def listar_archivos(base: str, extensiones) -> list:
    """Lista archivos bajo una ruta local o s3://, filtrando por extensión."""
    extensiones = tuple(e.lower() for e in extensiones)
    if es_s3(base):
        import boto3
        bucket, _, prefijo = re.sub(r"^s3a?://", "", base).partition("/")
        s3 = boto3.client("s3")
        salida = []
        for pagina in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefijo.rstrip("/") + "/"):
            for obj in pagina.get("Contents", []):
                if obj["Key"].lower().endswith(extensiones):
                    salida.append(f"s3://{bucket}/{obj['Key']}")
        return sorted(salida)
    salida = []
    for raiz, _, archivos in os.walk(base):
        for a in archivos:
            if a.lower().endswith(extensiones) and not a.startswith(("~$", ".")):
                salida.append(os.path.join(raiz, a))
    return sorted(salida)


def leer_bytes(ruta: str) -> bytes:
    if es_s3(ruta):
        import boto3
        bucket, _, key = re.sub(r"^s3a?://", "", ruta).partition("/")
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    with open(ruta, "rb") as f:
        return f.read()


PERIODO_RE = re.compile(r"(20\d{2})(?:-?([124]))?(?=[-._ ])")


def periodo_desde_nombre(nombre: str):
    """'...Saber-11-2023-4-VF.xlsx' -> '20234'; '...-20181.xlsx' -> '20181'; '...-2012-inst...' -> '20122'.
    Misma regla que generate_synthetic_icfes.py: si no hay semestre se asume 2 (calendario A)."""
    m = PERIODO_RE.search(os.path.basename(nombre))
    return f"{m.group(1)}{m.group(2) or 2}" if m else None


def solo_digitos(col, largo=None):
    """Quita '.0' de Excel y todo lo que no sea dígito; opcionalmente rellena con ceros a la izquierda."""
    limpio = F.regexp_replace(F.regexp_replace(F.trim(col.cast("string")), r"\.0+$", ""), r"[^0-9]", "")
    if largo:
        limpio = F.lpad(limpio, largo, "0")
    return F.when((limpio == "") | (F.regexp_replace(limpio, "0", "") == ""), None).otherwise(limpio)


def texto_normalizado(col):
    """Mayúsculas, sin tildes y sin espacios repetidos (la Ñ se conserva)."""
    s = F.upper(F.trim(col.cast("string")))
    s = F.translate(s, "ÁÉÍÓÚÜÀÈÌÒÙ", "AEIOUUAEIOU")
    s = F.regexp_replace(s, r"\s+", " ")
    return F.when((s == "") | s.isNull(), None).otherwise(s)


def zona_normalizada(col):
    s = texto_normalizado(col)
    return F.when(s.startswith("URBAN"), "URBANA").when(s.startswith("RURAL"), "RURAL")


def numero(col, maximo=None):
    v = F.regexp_replace(col.cast("string"), ",", ".").cast("double")
    return F.when((v >= 0) & (v <= maximo), v) if maximo is not None else v


def agregar_departamento(df: DataFrame, spark: SparkSession) -> DataFrame:
    ref = spark.createDataFrame([(k, v[0], v[1]) for k, v in DEPARTAMENTOS.items()],
                                ["cod_depto", "departamento", "region"])
    return df.withColumn("cod_depto", F.substring("cod_mpio", 1, 2)).join(F.broadcast(ref), "cod_depto", "left")


def escribir(df: DataFrame, ruta: str, particion=None) -> None:
    w = df.write.mode("overwrite").option("compression", "snappy")
    if particion:
        w = w.partitionBy(particion)
    w.parquet(ruta)
    log(f"Escrito {ruta}")


# ---------------------------------------------------------------------------
# 1 y 2. Excel agregados (reales del ICFES + sintéticos de EC2)
# ---------------------------------------------------------------------------

def leer_excel_agregado(contenido: bytes, ruta: str) -> pd.DataFrame:
    """Lee un .xlsx/.xls del ICFES. El encabezado no está en la fila 1 (el ICFES pone títulos
    arriba, en la fila 2, 5, 6 o 7 según la época), así que se busca la fila que contiene CODINST."""
    motor = "xlrd" if ruta.lower().endswith(".xls") else "openpyxl"
    crudo = pd.read_excel(io.BytesIO(contenido), header=None, dtype=str, engine=motor)
    fila = next((i for i in range(min(len(crudo), 40))
                 if {normalizar_nombre(v) for v in crudo.iloc[i].tolist() if pd.notna(v)}
                 & set(ALIAS_AGREGADOS["cod_colegio"])), None)
    if fila is None:
        raise ValueError("no se encontró la fila de encabezado con CODINST (¿formato anterior a 2014?)")

    datos = crudo.iloc[fila + 1:].copy()
    datos.columns = [normalizar_nombre(c) if pd.notna(c) else f"_vacia{j}" for j, c in enumerate(crudo.iloc[fila])]
    renombre = {}
    for destino, alias in ALIAS_AGREGADOS.items():
        encontrado = next((a for a in alias if a in datos.columns), None)
        if encontrado:
            renombre[encontrado] = destino
    faltantes = [c for c in OBLIGATORIAS_AGREGADOS if c not in renombre.values()]
    if faltantes:
        raise ValueError(f"faltan columnas {faltantes}; columnas encontradas: {list(datos.columns)}")

    datos = datos[list(renombre)].rename(columns=renombre)
    for destino in ALIAS_AGREGADOS:
        if destino not in datos.columns:
            datos[destino] = None
    periodo_archivo = periodo_desde_nombre(ruta)
    datos["periodo"] = datos["periodo"].fillna(periodo_archivo)
    if datos["periodo"].isna().all():
        raise ValueError("no se pudo determinar el periodo (ni columna PERIODO ni en el nombre del archivo)")
    datos["es_sintetico"] = "SINTETICO" in normalizar_nombre(ruta)
    datos["archivo_origen"] = os.path.basename(ruta)
    datos = datos.dropna(subset=["cod_colegio"])
    return datos.astype(object).where(pd.notna(datos), None)


def procesar_agregados(spark: SparkSession, raw: str, carpetas: list) -> DataFrame:
    archivos = []
    for c in carpetas:
        encontrados = listar_archivos(unir_ruta(raw, c), [".xlsx", ".xls"])
        log(f"{len(encontrados)} Excel en {unir_ruta(raw, c)}")
        archivos += encontrados
    if not archivos:
        log("AVISO: no hay Excel agregados; se omite colegio_periodo")
        return None

    # Un archivo corrupto en el origen o con estructura anterior a 2014 se omite con aviso,
    # en vez de detener todo el job.
    partes, omitidos = [], []
    for ruta in archivos:
        try:
            partes.append(leer_excel_agregado(leer_bytes(ruta), ruta))
            log(f"  OK  {os.path.basename(ruta)} ({len(partes[-1])} filas)")
        except Exception as e:
            omitidos.append(os.path.basename(ruta))
            log(f"  OMITIDO {os.path.basename(ruta)} -> {type(e).__name__}: {str(e)[:250]}")
    log(f"Excel procesados: {len(partes)} | omitidos: {len(omitidos)}")
    if not partes:
        return None

    pdf = pd.concat(partes, ignore_index=True)
    pdf = pdf.astype(str).replace({"None": None, "nan": None, "True": "true", "False": "false"})
    df = spark.createDataFrame(pdf, schema=T.StructType([T.StructField(c, T.StringType()) for c in pdf.columns]))

    df = (df
          .withColumn("cod_colegio", solo_digitos(F.col("cod_colegio")))
          .withColumn("cod_dane_sede", solo_digitos(F.col("cod_dane_sede"), 12))
          .withColumn("cod_mpio", solo_digitos(F.col("cod_mpio"), 5))
          .withColumn("periodo", solo_digitos(F.col("periodo")))
          .withColumn("es_sintetico", F.col("es_sintetico") == "true")
          .withColumn("nombre_colegio", texto_normalizado(F.col("nombre_colegio")))
          .withColumn("municipio", texto_normalizado(F.col("municipio")))
          .withColumn("naturaleza", texto_normalizado(F.col("naturaleza")))
          .withColumn("calendario", texto_normalizado(F.col("calendario")))
          .withColumn("jornada", texto_normalizado(F.col("jornada")))
          .withColumn("evaluados", numero(F.col("evaluados")).cast("int")))
    for a in AREAS:
        df = (df.withColumn(f"prom_{a}", numero(F.col(f"prom_{a}"), 100))
                .withColumn(f"desv_{a}", numero(F.col(f"desv_{a}"))))

    # Puntaje global con la ponderación oficial del ICFES (los Excel no lo traen):
    # global = (3*LC + 3*MAT + 3*SOC + 3*CN + 1*ING) / 13 * 5   (escala 0-500)
    df = df.withColumn("prom_global", F.round(
        (F.col("prom_lectura") * 3 + F.col("prom_matematicas") * 3 + F.col("prom_sociales") * 3
         + F.col("prom_naturales") * 3 + F.col("prom_ingles")) / 13 * 5, 2))

    df = (df.filter(F.col("cod_colegio").isNotNull() & F.col("cod_mpio").isNotNull() & (F.length("periodo") == 5))
            .filter(F.col("evaluados") > 0)
            .filter(F.col("prom_global").isNotNull())   # instituciones sin puntaje publicado
            .withColumn("anio", F.substring("periodo", 1, 4).cast("int"))
            .withColumn("semestre", F.substring("periodo", 5, 1).cast("int")))

    # Si un periodo existe real y sintético, gana el real.
    reales = df.filter(~F.col("es_sintetico")).select("periodo").distinct().withColumn("hay_real", F.lit(True))
    df = (df.join(F.broadcast(reales), "periodo", "left")
            .filter(~(F.col("es_sintetico") & F.col("hay_real").isNotNull())).drop("hay_real"))

    # Desde 2023 hay una fila por sede-jornada; antes, por institución-jornada.
    df = df.dropDuplicates(["periodo", "cod_colegio", "cod_dane_sede", "jornada", "calendario"])
    return agregar_departamento(df, spark).drop("departamento_original")


# ---------------------------------------------------------------------------
# 3. Directorio de sedes (RDS MariaDB, modelo normalizado)
# ---------------------------------------------------------------------------

def leer_tabla_rds(spark: SparkSession, base: str, tabla: str) -> DataFrame:
    """Lee solo la ingest_date más reciente: cada corrida de la ingesta deja una copia nueva."""
    archivos = listar_archivos(unir_ruta(base, tabla), [".csv"])
    if not archivos:
        return None
    fechas = [m.group(1) for a in archivos for m in [re.search(r"ingest_date=([\d-]+)", a)] if m]
    if fechas:
        ultima = max(fechas)
        archivos = [a for a in archivos if f"ingest_date={ultima}" in a]
        log(f"  {tabla}: ingest_date={ultima}")
    return spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(archivos)


def procesar_sedes(spark: SparkSession, raw: str, carpeta: str) -> DataFrame:
    base = unir_ruta(raw, carpeta)
    tablas = {t: leer_tabla_rds(spark, base, t) for t in TABLAS_RDS}
    if tablas["sede_educativa"] is None or tablas["municipio"] is None:
        log(f"AVISO: faltan sede_educativa o municipio en {base}; se omite sede")
        return None

    sede = tablas["sede_educativa"]
    mpio = tablas["municipio"].select("id_municipio", "codigo_dane_municipio", "nombre_municipio")
    sede = sede.join(F.broadcast(mpio), "id_municipio", "left")
    catalogos = [("sector_educativo", "id_sector", "nombre_sector"), ("zona_geografica", "id_zona", "nombre_zona"),
                 ("estado_sede", "id_estado", "nombre_estado"), ("tipo_calendario", "id_calendario", "codigo_calendario")]
    for tabla, id_col, nombre_col in catalogos:
        if tablas[tabla] is not None:
            sede = sede.join(F.broadcast(tablas[tabla].select(id_col, nombre_col)), id_col, "left")
        else:
            sede = sede.withColumn(nombre_col, F.lit(None).cast("string"))

    sede = sede.select(
        solo_digitos(F.col("cod_dane_sede"), 12).alias("cod_dane_sede"),
        F.col("id_sede").cast("int").alias("id_sede"),
        texto_normalizado(F.col("nombre_institucion")).alias("nombre_sede"),
        solo_digitos(F.col("codigo_dane_municipio"), 5).alias("cod_mpio"),
        texto_normalizado(F.col("nombre_municipio")).alias("municipio"),
        texto_normalizado(F.col("nombre_sector")).alias("naturaleza"),
        zona_normalizada(F.col("nombre_zona")).alias("zona"),
        texto_normalizado(F.col("nombre_estado")).alias("estado"),
        texto_normalizado(F.col("codigo_calendario")).alias("calendario"),
        numero(F.col("latitud")).alias("latitud"),
        numero(F.col("longitud")).alias("longitud"),
    )
    return agregar_departamento(sede, spark)


def asignar_zona(cp: DataFrame, sede: DataFrame) -> DataFrame:
    """Agrega a cada fila de resultados la zona (urbana/rural) y el estado de su sede.

    - Desde 2023 el Excel trae CODIGODANE_SEDE: se une directo con el directorio.
    - Antes no lo trae: se usa la sede que tuvo esa misma institución (CODINST) en los
      periodos que sí lo traen (la más frecuente)."""
    mapa = (cp.filter(F.col("cod_dane_sede").isNotNull())
              .groupBy("cod_colegio", "cod_dane_sede").count()
              .withColumn("n", F.row_number().over(Window.partitionBy("cod_colegio").orderBy(F.desc("count"))))
              .filter("n = 1").select("cod_colegio", F.col("cod_dane_sede").alias("sede_mapeada")))
    cp = (cp.join(mapa, "cod_colegio", "left")
            .withColumn("metodo_zona", F.when(F.col("cod_dane_sede").isNotNull(), "SEDE DEL ARCHIVO")
                                         .when(F.col("sede_mapeada").isNotNull(), "MAPEO POR CODINST"))
            .withColumn("cod_dane_sede_ref", F.coalesce("cod_dane_sede", "sede_mapeada")).drop("sede_mapeada"))
    info = sede.select(F.col("cod_dane_sede").alias("cod_dane_sede_ref"), "zona",
                       F.col("estado").alias("estado_sede"), "latitud", "longitud")
    cp = cp.join(info, "cod_dane_sede_ref", "left")
    return cp.withColumn("metodo_zona", F.when(F.col("zona").isNotNull(), F.col("metodo_zona")))


# ---------------------------------------------------------------------------
# 4. CSV por estudiante (opcional)
# ---------------------------------------------------------------------------

def nivel_educativo(col):
    s = texto_normalizado(col)
    return (F.when(s.isNull() | s.rlike("NO SABE|NO APLICA"), None)
             .when(s.rlike("POSTGRADO|POSGRADO"), 5)
             .when(s.rlike("PROFESIONAL") & s.rlike("COMPLETA") & ~s.rlike("INCOMPLETA"), 4)
             .when(s.rlike("PROFESIONAL|TECNICA|TECNOLOGICA"), 3)
             .when(s.rlike("SECUNDARIA|BACHILLERATO"), 2)
             .when(s.rlike("PRIMARIA"), 1)
             .when(s.rlike("NINGUNO"), 0))


NOMBRE_NIVEL = {0: "0 NINGUNO", 1: "1 PRIMARIA", 2: "2 SECUNDARIA",
                3: "3 TECNICA O PROFESIONAL INCOMPLETA", 4: "4 PROFESIONAL", 5: "5 POSGRADO"}


def procesar_estudiantes(spark: SparkSession, raw: str, carpeta: str) -> DataFrame:
    base = unir_ruta(raw, carpeta)
    archivos = listar_archivos(base, [".csv"])
    if not archivos:
        log(f"Sin datos por estudiante en {base} (fuente opcional)")
        return None
    df = spark.read.option("header", True).option("multiLine", True).option("escape", '"').csv(archivos)
    df = df.toDF(*[c.strip().lower() for c in df.columns])
    faltantes = [c for c in ["periodo", "punt_global"] if c not in df.columns]
    if faltantes:
        raise ValueError(f"El CSV de estudiantes no tiene {faltantes}. Columnas: {df.columns}")
    df = df.select(*[F.col(o).alias(d) if o in df.columns else F.lit(None).cast("string").alias(d)
                     for o, d in COLUMNAS_ESTUDIANTE.items()])
    df = df.dropDuplicates(["periodo", "estu_consecutivo"])   # el portal repite filas

    mapa_nivel = F.create_map(*[x for k, v in NOMBRE_NIVEL.items() for x in (F.lit(k), F.lit(v))])
    estrato = F.regexp_extract(texto_normalizado(F.col("estrato_original")), r"ESTRATO ([1-6])", 1)
    internet = texto_normalizado(F.col("tiene_internet"))
    df = (df.withColumn("periodo", solo_digitos(F.col("periodo")))
            .withColumn("cod_dane_establecimiento", solo_digitos(F.col("cod_dane_establecimiento"), 12))
            .withColumn("cod_mpio", solo_digitos(F.col("cod_mpio"), 5))
            .withColumn("nombre_colegio", texto_normalizado(F.col("nombre_colegio")))
            .withColumn("municipio", texto_normalizado(F.col("municipio")))
            .withColumn("naturaleza", texto_normalizado(F.col("naturaleza")))
            .withColumn("zona", zona_normalizada(F.col("zona")))
            .withColumn("calendario", texto_normalizado(F.col("calendario")))
            .withColumn("jornada", texto_normalizado(F.col("jornada")))
            .withColumn("estrato", F.when(estrato == "", None).otherwise(estrato.cast("int")))
            .withColumn("nivel_educ_padres", F.greatest(nivel_educativo(F.col("educ_padre")),
                                                        nivel_educativo(F.col("educ_madre"))))
            .withColumn("nivel_educ_padres_txt", mapa_nivel[F.col("nivel_educ_padres")])
            .withColumn("tiene_internet", F.when(internet == "SI", True).when(internet == "NO", False)))
    for a in AREAS:
        df = df.withColumn(f"punt_{a}", numero(F.col(f"punt_{a}"), 100))
    df = (df.withColumn("punt_global", numero(F.col("punt_global"), 500))
            .filter(F.col("punt_global").isNotNull() & F.col("cod_mpio").isNotNull())
            .withColumn("anio", F.substring("periodo", 1, 4).cast("int"))
            .withColumn("semestre", F.substring("periodo", 5, 1).cast("int")))
    df = agregar_departamento(df, spark)
    # estu_consecutivo es un identificador personal: no pasa a trusted.
    return df.drop("estu_consecutivo", "estrato_original", "educ_padre", "educ_madre")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

COLUMNAS_COLEGIO_PERIODO = [
    "periodo", "anio", "semestre", "es_sintetico", "cod_colegio", "cod_dane_sede", "cod_dane_sede_ref",
    "nombre_colegio", "cod_mpio", "municipio", "cod_depto", "departamento", "region", "calendario",
    "naturaleza", "jornada", "zona", "metodo_zona", "estado_sede", "latitud", "longitud", "evaluados",
    *[f"prom_{a}" for a in AREAS], "prom_global", *[f"desv_{a}" for a in AREAS], "archivo_origen",
]


def main():
    args = parse_args()
    spark = (SparkSession.builder.appName("saber11-raw-to-trusted")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .getOrCreate())
    resumen = {}

    sede = procesar_sedes(spark, args.raw, args.carpeta_rds)
    if sede is not None:
        sede = sede.cache()
        escribir(sede, unir_ruta(args.trusted, "sede"))
        resumen["sede"] = sede.count()

    cp = procesar_agregados(spark, args.raw, [c.strip() for c in args.carpetas_agregados.split(",") if c.strip()])
    if cp is not None:
        if sede is not None:
            cp = asignar_zona(cp, sede)
        else:
            for c, tipo in [("cod_dane_sede_ref", "string"), ("zona", "string"), ("metodo_zona", "string"),
                            ("estado_sede", "string"), ("latitud", "double"), ("longitud", "double")]:
                cp = cp.withColumn(c, F.lit(None).cast(tipo))
        cp = cp.select(*COLUMNAS_COLEGIO_PERIODO).cache()
        escribir(cp, unir_ruta(args.trusted, "colegio_periodo"), "periodo")
        resumen["colegio_periodo"] = cp.count()

        log("Filas por periodo (real / sintético) y cobertura de zona:")
        (cp.groupBy("periodo", "es_sintetico")
           .agg(F.count("*").alias("filas"), F.sum("evaluados").alias("evaluados"),
                F.round(F.avg(F.col("zona").isNotNull().cast("int")) * 100, 1).alias("pct_con_zona"))
           .orderBy("periodo").show(60, truncate=False))

    est = procesar_estudiantes(spark, args.raw, args.carpeta_estudiantes)
    if est is not None:
        est = est.cache()
        escribir(est, unir_ruta(args.trusted, "estudiante"), "periodo")
        resumen["estudiante"] = est.count()

    deptos = spark.createDataFrame([(k, v[0], v[1]) for k, v in DEPARTAMENTOS.items()],
                                   ["cod_depto", "departamento", "region"])
    escribir(deptos, unir_ruta(args.trusted, "departamento"))
    resumen["departamento"] = deptos.count()

    log(f"Resumen de filas escritas: {resumen}")
    if "colegio_periodo" not in resumen:
        sys.exit("No se pudo construir colegio_periodo: revise las rutas de los Excel en raw.")
    spark.stop()


if __name__ == "__main__":
    main()
