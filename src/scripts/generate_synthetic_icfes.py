"""
Genera archivos SINTÉTICOS de resultados agregados Saber 11 para los periodos cuyo archivo
publicado por el ICFES está corrupto (truncado en el servidor de origen).

Cómo se construyen:
  1. Se descargan (o se leen de --source-dir) los archivos de data/matches.txt.
  2. Los que se pueden abrir son la base real; los que no, son los periodos a sintetizar.
  3. Para cada periodo faltante:
     - Universo de sedes/jornadas = el del periodo real más cercano del MISMO calendario
       (A: periodos -2/-4, B: periodos -1). Se omite ~4 % de filas al azar.
     - Puntaje por área = promedio real de esa institución/jornada en todos los periodos reales
       + tendencia nacional del año (TENDENCIA) + efecto aleatorio de la institución + ruido.
     - Desviaciones y número de evaluados se perturban alrededor de los valores reales.
     - Filas sin puntaje en la base (instituciones con muy pocos evaluados) se mantienen vacías.
  4. Se escribe un .xlsx con el layout que usaba el ICFES en esa época:
       <= 2017: título fila 2, encabezado fila 6, EVALUADOS, puntajes con 2 decimales
       2018-2022: título fila 5, encabezado fila 7, enteros, sin CODIGODANE_SEDE
       >= 2023: título fila 2, encabezado fila 7, con CODIGODANE_SEDE
     El título y el nombre del archivo dicen explícitamente que son datos sintéticos.

Salida: data/ec2_files/icfes_saber11_sinteticos/Resultados-agregados-Saber-11-<AAAA>-<P>-SINTETICO.xlsx

Uso:
  python generate_synthetic_icfes.py                         # descarga desde matches.txt
  python generate_synthetic_icfes.py --source-dir /ruta/xlsx  # usa archivos ya descargados
"""
import argparse
import re
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import numpy as np
import pandas as pd
from openpyxl import Workbook

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent.parent
URLS_FILE = PROJECT / "data/matches.txt"
OUT_DIR = PROJECT / "data/ec2_files/icfes_saber11_sinteticos"

AREAS = ["LECTURACRITICA", "MATEMATICA", "SOCIALESYCIUDADANAS", "CIENCIASNATURALES", "INGLES"]
PROM = [f"PROM{a}" for a in AREAS]
DESV = [f"DESV{a}" for a in AREAS]
ID_COLS = ["CODINST", "CODIGODANE_SEDE", "NOMBREINSTITUCION", "CODIGOMUNICIPIO", "NOMBREMUNICIPIO",
           "DEPARTAMENTO", "CALENDARIO", "NATURALEZA", "JORNADA"]

# Supuesto: desplazamiento nacional (puntos) frente al promedio de los periodos reales.
# Años previos a 2020 algo por encima; caída en 2020-2021 (pandemia).
TENDENCIA = {2012: 1.5, 2013: 1.5, 2014: 1.2, 2015: 1.2, 2016: 1.0, 2017: 1.0, 2018: 0.8,
             2019: 0.5, 2020: -0.8, 2021: -1.2, 2022: -0.5}

PERIOD_RE = re.compile(r"(20\d{2})(?:-?([124]))?(?=[-.])")


def parse_period(name):
    year, sem = PERIOD_RE.search(name).groups()
    return int(year), int(sem or 2)  # 2012 no trae semestre: se asume calendario A


def calendario(sem):
    return "B" if sem == 1 else "A"


def is_valid(path):
    if path.suffix.lower() == ".xlsx":
        return zipfile.is_zipfile(path)
    return False  # .xls (2015) no se usa como base: necesitaría xlrd y no es un periodo faltante


def fetch_sources(dest):
    urls = dict.fromkeys(l.strip() for l in URLS_FILE.read_text().splitlines() if l.strip())
    for url in urls:
        archivo = parse_qs(urlparse(url).query).get("archivo", [url])[0]
        path = dest / Path(urlparse(archivo).path).name
        if not path.exists():
            print(f"descargando {path.name}")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=120) as r:
                path.write_bytes(r.read())
    return dest


def read_real(path):
    raw = pd.read_excel(path, header=None, dtype=str)
    header_row = raw.index[raw.iloc[:, 0].eq("CODINST")][0]
    df = raw.iloc[header_row + 1:].copy()
    df.columns = raw.iloc[header_row]
    df = df.loc[:, df.columns.notna()].rename(columns={"EVALUADOS": "EVALUADO"})
    df = df[df.CODINST.notna()]
    for c in PROM + DESV + ["EVALUADO"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "CODIGODANE_SEDE" not in df:
        df["CODIGODANE_SEDE"] = None
    year, sem = parse_period(path.name)
    df["YEAR"], df["SEM"] = year, sem
    return df


def layout(year):
    if year <= 2017:
        return "legacy"
    if year <= 2022:
        return "mid"
    return "modern"


def synthesize(real, year, sem, rng):
    cal = calendario(sem)
    same_cal = real[real.CALENDARIO.eq(cal)]
    periods = same_cal[["YEAR", "SEM"]].drop_duplicates()
    nearest = periods.iloc[(periods.YEAR - year).abs().argsort().iloc[0]]
    base = same_cal[(same_cal.YEAR == nearest.YEAR) & (same_cal.SEM == nearest.SEM)].copy()
    base = base[rng.random(len(base)) > 0.04].reset_index(drop=True)

    # Promedio histórico real por institución/jornada (todos los periodos reales del calendario)
    hist = same_cal.groupby(["CODINST", "JORNADA"])[PROM + DESV + ["EVALUADO"]].mean()
    empty = base[PROM + DESV].isna().to_numpy()  # sin puntaje publicado en la base: sigue vacío
    base = base.drop(columns=PROM + DESV + ["EVALUADO"]).join(hist, on=["CODINST", "JORNADA"])
    base[PROM + DESV] = base[PROM + DESV].mask(empty)

    n = len(base)
    shift = TENDENCIA.get(year, 0.0) + rng.normal(0, 1.5, n)  # tendencia + efecto institución
    for c in PROM:
        base[c] = (base[c] + shift + rng.normal(0, 1.0, n)).clip(0, 100)
    for c in DESV:
        base[c] = (base[c] + rng.normal(0, 0.8, n)).clip(lower=0)
    base["EVALUADO"] = np.maximum(1, np.round(base.EVALUADO * rng.uniform(0.8, 1.2, n))).astype(int)
    base["CALENDARIO"] = cal
    base["PERIODO"] = f"{year}{sem}"
    return base


def write_xlsx(df, year, sem, path):
    kind = layout(year)
    cols = ID_COLS + ["EVALUADO"] + PROM + DESV + ["PERIODO"]
    df = df[cols].copy()
    if kind == "legacy":
        df[PROM + DESV] = df[PROM + DESV].round(2)
        df["PERIODO"] = df.PERIODO.astype(int)
        df = df.drop(columns="CODIGODANE_SEDE").rename(columns={"EVALUADO": "EVALUADOS"})
        sheet, title_cell, header_row = f"{year}{sem}", "D2", 6
        title = f"ESTADISTICAS POR INSTITUCION PRUEBAS SABER 11° {year} {sem} (DATOS SINTÉTICOS)"
    else:
        df[PROM + DESV] = df[PROM + DESV].round(0).astype("Int64")
        header_row = 7
        if kind == "mid":
            df = df.drop(columns="CODIGODANE_SEDE")
            sheet, title_cell = f"SB {year}-{sem}", "C5"
            title = f"ESTADISTICAS POR INSTITUCION PRUEBAS SABER 11° {year}-{sem} (DATOS SINTÉTICOS)"
        else:
            sheet, title_cell = f"{year}-{sem}", "C2"
            title = (f"Resultados institucionales por sede jornada, Saber 11 {year} "
                     f"calendario {calendario(sem)} (DATOS SINTÉTICOS)")

    wb = Workbook()
    ws = wb.active
    ws.title = sheet
    ws[title_cell] = title
    for j, name in enumerate(df.columns, start=1):
        ws.cell(row=header_row, column=j, value=name)
    for i, row in enumerate(df.itertuples(index=False), start=header_row + 1):
        for j, v in enumerate(row, start=1):
            ws.cell(row=i, column=j, value=None if pd.isna(v) else (v.item() if hasattr(v, "item") else v))
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source-dir", type=Path, help="carpeta con los archivos ya descargados")
    parser.add_argument("--out-dir", type=Path, default=OUT_DIR)
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        src = args.source_dir or fetch_sources(Path(tmp))
        files = sorted(p for p in src.iterdir() if p.suffix.lower() in (".xlsx", ".xls"))
        valid = [p for p in files if is_valid(p)]
        missing = sorted({parse_period(p.name) for p in files if p.suffix.lower() == ".xlsx" and not is_valid(p)})
        real = pd.concat([read_real(p) for p in valid], ignore_index=True)

    print(f"Base real: {len(valid)} archivos, {len(real)} filas")
    print(f"Periodos a sintetizar: {', '.join(f'{y}-{s}' for y, s in missing)}")
    rng = np.random.default_rng(args.seed)
    for year, sem in missing:
        df = synthesize(real, year, sem, rng)
        out = args.out_dir / f"Resultados-agregados-Saber-11-{year}-{sem}-SINTETICO.xlsx"
        write_xlsx(df, year, sem, out)
        print(f"  {out.name}: {len(df)} filas, layout {layout(year)}, "
              f"prom. matemáticas {df.PROMMATEMATICA.mean():.1f}")


if __name__ == "__main__":
    main()
