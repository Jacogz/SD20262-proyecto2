"""
Punto 8 - Paso 1: genera la zona REFINED con las respuestas a las 6 preguntas.

Lee de la zona raw del data lake:
  raw/ec2/icfes_saber11_sinteticos/*.xlsx      (periodos sintéticos)
  raw/url/icfes_saber11_agregados/*.xlsx|xls   (periodos reales del ICFES)
  raw/rds/sedes_educativas/<tabla>/ingest_date=*/<tabla>.csv  (sedes DANE)
y escribe un JSON pequeño por pregunta en:
  s3://<bucket>/refined/api/q1.json ... q6.json (+ meta.json)
que luego sirve la Lambda detrás de API Gateway.

Uso:
  python build_refined.py --bucket NOMBRE_BUCKET            # S3 -> S3
  python build_refined.py --bucket NOMBRE_BUCKET --no-upload # solo escribe ./out/
  python build_refined.py --local-dir ../../data            # prueba sin AWS (usa el repo)
"""
import argparse
import io
import json
import logging
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

log = logging.getLogger("refined")

AREAS = {
    "PROMLECTURACRITICA": "Lectura crítica",
    "PROMMATEMATICA": "Matemáticas",
    "PROMSOCIALESYCIUDADANAS": "Sociales y ciudadanas",
    "PROMCIENCIASNATURALES": "Ciencias naturales",
    "PROMINGLES": "Inglés",
}
# Ponderación oficial del puntaje global Saber 11: 3 por área, 1 por inglés (escala 0-500)
PESOS = {"PROMLECTURACRITICA": 3, "PROMMATEMATICA": 3, "PROMSOCIALESYCIUDADANAS": 3,
         "PROMCIENCIASNATURALES": 3, "PROMINGLES": 1}

REGIONES = {
    "Andina": ["ANTIOQUIA", "BOYACA", "CALDAS", "CUNDINAMARCA", "HUILA", "NORTE",
               "QUINDIO", "RISARALDA", "SANTANDER", "TOLIMA", "BOGOTA"],
    "Caribe": ["ATLANTICO", "BOLIVAR", "CESAR", "CORDOBA", "LA GUAJIRA", "GUAJIRA", "MAGDALENA", "SUCRE"],
    "Pacífica": ["CAUCA", "CHOCO", "NARINO", "VALLE"],
    "Orinoquía": ["ARAUCA", "CASANARE", "META", "VICHADA"],
    "Amazonía": ["AMAZONAS", "CAQUETA", "GUAINIA", "GUAVIARE", "PUTUMAYO", "VAUPES"],
    "Insular": ["SAN ANDRES", "ARCHIPIELAGO"],
}
CIUDADES = {"11001": "Bogotá", "05001": "Medellín", "76001": "Cali"}
MIN_EVAL_MUNI = 30   # evita rankear municipios con muy pocos evaluados
MIN_EVAL_COLEGIO = 10


def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode().upper().strip()
    return re.sub(r"\s+", " ", s)


def region_de(depto):
    d = norm(depto)
    for region, keys in REGIONES.items():
        if any(d.startswith(k) for k in keys):
            return region
    return "Sin región"


# ---------------------------------------------------------------- lectura de archivos
def read_saber(data, name):
    """Lee un xlsx/xls agregado del ICFES. El encabezado cambia de fila según la época."""
    engine = "xlrd" if name.lower().endswith(".xls") else None
    raw = pd.read_excel(io.BytesIO(data), header=None, engine=engine)
    hdr = next((i for i in range(min(25, len(raw)))
                if raw.iloc[i].astype(str).map(norm).str.contains("NOMBREINSTITUCION|CODINST").any()), None)
    if hdr is None:
        raise ValueError("no se encontró la fila de encabezado")
    df = raw.iloc[hdr + 1:].copy()
    df.columns = [norm(c).replace(" ", "") for c in raw.iloc[hdr]]
    df = df.loc[:, [c for c in df.columns if c and c != "NAN"]]
    df = df.rename(columns={"EVALUADO": "EVALUADOS"})
    m = re.search(r"(20\d{2})[-_ ]?([1-4])?", name)
    if "PERIODO" in df.columns and df["PERIODO"].notna().any():
        per = df["PERIODO"].dropna().astype(str).iloc[0]
        anio, sem = int(per[:4]), int(per[4:5] or 2)
    else:
        anio, sem = int(m.group(1)), int(m.group(2) or 2)
    df["anio"], df["semestre"] = anio, sem
    for c in list(AREAS) + ["EVALUADOS"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["sintetico"] = "SINTETICO" in name.upper()
    df["archivo"] = name
    return df


def global_score(df):
    cols = [c for c in AREAS if c in df.columns]
    num = sum(df[c].fillna(0) * PESOS[c] for c in cols)
    den = sum(df[c].notna() * PESOS[c] for c in cols)
    return (num / den.replace(0, np.nan)) * 5


def combinar(frames):
    df = pd.concat(frames, ignore_index=True)
    # Si un periodo existe real y sintético, gana el real
    df = df.sort_values("sintetico")
    keep = df.groupby(["anio", "semestre"]).sintetico.transform(lambda s: s.min())
    df = df[df.sintetico == keep].copy()
    df["depto"] = df.DEPARTAMENTO.map(norm)
    df["cod_muni"] = df.CODIGOMUNICIPIO.astype(str).str.replace(r"\.0$", "", regex=False).str.zfill(5)
    df["municipio"] = df.NOMBREMUNICIPIO.astype(str).str.title()
    df["naturaleza"] = df.NATURALEZA.map(norm).map(lambda x: "No oficial" if x.startswith("NO") else "Oficial")
    df["region"] = df.depto.map(region_de)
    df["puntaje_global"] = global_score(df)
    df["EVALUADOS"] = df.EVALUADOS.fillna(1).clip(lower=1)
    return df[df.puntaje_global.notna()]


def wmean(g, col="puntaje_global"):
    return float(np.average(g[col], weights=g.EVALUADOS))


# ---------------------------------------------------------------- zona (urbana/rural) desde RDS
def agregar_zona(df, rds):
    """Cruza instituciones Saber con sedes DANE (RDS) por municipio + nombre normalizado."""
    if rds is None:
        df["zona"] = "Sin dato"
        return df
    sede, muni, zona = rds["sede_educativa"], rds["municipio"], rds["zona_geografica"]
    s = (sede.merge(muni[["id_municipio", "codigo_dane_municipio"]], on="id_municipio")
             .merge(zona, on="id_zona"))
    s["cod_muni"] = s.codigo_dane_municipio.astype(str).str.zfill(5)
    s["nombre_n"] = s.nombre_institucion.map(norm)
    lookup = (s.groupby(["cod_muni", "nombre_n"]).nombre_zona
              .agg(lambda x: x.mode().iat[0]).rename("zona").reset_index())
    df["nombre_n"] = df.NOMBREINSTITUCION.map(norm)
    df = df.merge(lookup, on=["cod_muni", "nombre_n"], how="left")
    df["zona"] = df.zona.fillna("Sin dato").str.title()
    log.info("Cobertura zona: %.1f%% de filas con zona", 100 * (df.zona != "Sin dato").mean())
    return df


# ---------------------------------------------------------------- las 6 preguntas
def q1(df):
    dep = df.groupby("depto").apply(wmean, include_groups=False).sort_values(ascending=False)
    mun = (df.groupby(["depto", "municipio"])
             .apply(lambda g: pd.Series({"puntaje": wmean(g), "evaluados": int(g.EVALUADOS.sum())}),
                    include_groups=False).reset_index())
    mun = mun[mun.evaluados >= MIN_EVAL_MUNI].sort_values("puntaje", ascending=False)
    r = lambda d: d.round(2).to_dict("records")
    dep_df = dep.rename("puntaje").reset_index().rename(columns={"depto": "departamento"})
    mun = mun.rename(columns={"depto": "departamento"})
    return {"departamentos": r(dep_df), "municipios_top": r(mun.head(10)), "municipios_bottom": r(mun.tail(10).iloc[::-1])}


def q2(df):
    nat = df.groupby("naturaleza").apply(wmean, include_groups=False).round(2)
    con_zona = df[df.zona.isin(["Urbano", "Rural"])]
    if con_zona.empty:
        cruce = pd.DataFrame(columns=["zona", "naturaleza", "puntaje"])
    else:
        cruce = (con_zona.groupby(["zona", "naturaleza"]).apply(wmean, include_groups=False)
                 .rename("puntaje").round(2).reset_index())
    return {"naturaleza": nat.rename("puntaje").reset_index().to_dict("records"),
            "diferencia_oficial_vs_no_oficial": round(float(nat.get("No oficial", np.nan) - nat.get("Oficial", np.nan)), 2),
            "zona_naturaleza": cruce.to_dict("records")}


def q3(df):
    rows = []
    for region, g in df.groupby("region"):
        for col, nombre in AREAS.items():
            if col in g.columns and g[col].notna().any():
                sub = g[g[col].notna()]
                rows.append({"region": region, "area": nombre, "puntaje": round(wmean(sub, col), 2)})
    t = pd.DataFrame(rows)
    t["es_minimo"] = t.groupby("region").puntaje.transform(lambda s: s == s.min())
    return {"regiones_areas": t.to_dict("records")}


def q4(df):
    t = (df.groupby(["depto", "anio"]).apply(wmean, include_groups=False)
           .rename("puntaje").round(2).reset_index().rename(columns={"depto": "departamento"}))
    nac = df.groupby("anio").apply(wmean, include_groups=False).round(2).rename("puntaje").reset_index()
    sint = sorted(df[df.sintetico].anio.unique().tolist())
    return {"evolucion": t.to_dict("records"), "nacional": nac.to_dict("records"), "anios_sinteticos": sint}


def q5(df):
    ult = df[df.anio == df.anio.max()]
    ult = ult[ult.EVALUADOS >= MIN_EVAL_COLEGIO]
    # Una institución puede venir por jornada: se agrega por código
    g = (ult.groupby(["depto", "CODINST", "NOMBREINSTITUCION", "municipio", "naturaleza"])
            .apply(lambda x: pd.Series({"puntaje": wmean(x), "evaluados": int(x.EVALUADOS.sum())}),
                   include_groups=False).reset_index())
    g = g.sort_values(["depto", "puntaje"], ascending=[True, False]).groupby("depto").head(10)
    g["ranking"] = g.groupby("depto").cumcount() + 1
    g = g.rename(columns={"depto": "departamento", "NOMBREINSTITUCION": "colegio"}).drop(columns="CODINST")
    return {"anio": int(df.anio.max()), "top10": g.round(2).to_dict("records")}


def q6(df):
    df = df.copy()
    df["grupo"] = df.cod_muni.map(CIUDADES).fillna("Resto del país")
    t = (df.groupby(["anio", "grupo"]).apply(wmean, include_groups=False)
           .rename("puntaje").round(2).reset_index())
    piv = t.pivot(index="anio", columns="grupo", values="puntaje")
    brecha = [{"anio": int(a), "ciudad": c, "brecha": round(float(piv.loc[a, c] - piv.loc[a, "Resto del país"]), 2)}
              for a in piv.index for c in CIUDADES.values() if c in piv.columns]
    return {"puntajes": t.to_dict("records"), "brecha_vs_resto": brecha}


# ---------------------------------------------------------------- E/S
def cargar_local(base):
    base = Path(base)
    frames, rds = [], None
    for p in sorted(list(base.rglob("*.xlsx")) + list(base.rglob("*.xls"))):
        if "SISE_departamentos" in str(p):
            continue
        try:
            frames.append(read_saber(p.read_bytes(), p.name))
        except Exception as e:
            log.warning("omitido %s: %s", p.name, e)
    return frames, rds


def cargar_s3(bucket, region):
    import boto3
    s3 = boto3.client("s3", region_name=region)
    pag = s3.get_paginator("list_objects_v2")

    def keys(prefix):
        return [o["Key"] for page in pag.paginate(Bucket=bucket, Prefix=prefix) for o in page.get("Contents", [])]

    frames = []
    for k in keys("raw/ec2/") + keys("raw/url/"):
        if not k.lower().endswith((".xlsx", ".xls")):
            continue
        try:
            body = s3.get_object(Bucket=bucket, Key=k)["Body"].read()
            frames.append(read_saber(body, Path(k).name))
            log.info("leído %s", k)
        except Exception as e:
            log.warning("omitido %s (%s)", k, e)   # p. ej. archivos truncados en el origen

    rds = {}
    for t in ["sede_educativa", "municipio", "zona_geografica"]:
        ks = sorted(k for k in keys(f"raw/rds/sedes_educativas/{t}/") if k.endswith(".csv"))
        if not ks:
            rds = None
            log.warning("No hay tabla RDS %s en raw; se omite la zona", t)
            break
        rds[t] = pd.read_csv(io.BytesIO(s3.get_object(Bucket=bucket, Key=ks[-1])["Body"].read()), dtype=str)
        if t == "sede_educativa":
            pass
    if rds:
        rds["municipio"]["id_municipio"] = rds["municipio"].id_municipio.astype(int)
        rds["sede_educativa"]["id_municipio"] = rds["sede_educativa"].id_municipio.astype(int)
        rds["sede_educativa"]["id_zona"] = rds["sede_educativa"].id_zona.astype(int)
        rds["zona_geografica"]["id_zona"] = rds["zona_geografica"].id_zona.astype(int)
    return frames, rds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--local-dir")
    ap.add_argument("--out", default="out")
    ap.add_argument("--no-upload", action="store_true")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    frames, rds = cargar_local(a.local_dir) if a.local_dir else cargar_s3(a.bucket, a.region)
    if not frames:
        raise SystemExit("No se pudo leer ningún archivo Saber 11")
    df = agregar_zona(combinar(frames), rds)
    log.info("Filas: %d | periodos: %s", len(df), sorted(df.anio.unique()))

    outputs = {"q1": q1(df), "q2": q2(df), "q3": q3(df), "q4": q4(df), "q5": q5(df), "q6": q6(df),
               "meta": {"filas": int(len(df)), "anios": sorted(int(x) for x in df.anio.unique()),
                        "departamentos": int(df.depto.nunique()),
                        "periodos_sinteticos": sorted(int(x) for x in df[df.sintetico].anio.unique())}}
    out = Path(a.out)
    out.mkdir(exist_ok=True)
    for name, payload in outputs.items():
        body = json.dumps(payload, ensure_ascii=False)
        (out / f"{name}.json").write_text(body, encoding="utf-8")
        if a.bucket and not a.no_upload:
            import boto3
            boto3.client("s3", region_name=a.region).put_object(
                Bucket=a.bucket, Key=f"refined/api/{name}.json", Body=body.encode(),
                ContentType="application/json")
            log.info("subido s3://%s/refined/api/%s.json", a.bucket, name)
    log.info("Listo. JSON en %s/", out)


if __name__ == "__main__":
    main()
