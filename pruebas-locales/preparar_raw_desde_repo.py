"""
Arma una zona RAW LOCAL con la misma estructura y formato del bucket, a partir del repositorio
del equipo (SD20262-proyecto2), para probar los jobs de Spark sin AWS.

Qué genera en <salida>/raw:
  ec2/icfes_saber11_sinteticos/   copia de los Excel sintéticos del repo (formato real)
  rds/sedes_educativas/<tabla>/ingest_date=AAAA-MM-DD/<tabla>.csv
                                  las 7 tablas de RDS construidas con setup_rds.build_tables() a partir de
                                  data/SISE_departamentos (igual que ingest_to_s3.py: read_sql_table -> to_csv).
                                  Se escriben dos fechas para comprobar que el job usa solo la última.
  url/icfes_saber11_agregados/    - un Excel "2023-4" con el layout moderno (con CODIGODANE_SEDE), hecho con
                                    write_xlsx() del generador del repo a partir del sintético 2021-4 y
                                    asignando a cada institución una sede real del SISE de su municipio
                                  - un Excel corrupto (truncado), como los que publica el ICFES para 2019-2

Los Excel de url/ son de PRUEBA: los reales se descargan de icfes.gov.co con la ingesta.

Uso:
  pip install pandas numpy openpyxl sqlalchemy python-dotenv
  python preparar_raw_desde_repo.py /ruta/SD20262-proyecto2 ./prueba
"""
import importlib.util
import os
import shutil
import sys
from pathlib import Path

import numpy as np


def cargar_modulo(nombre, ruta):
    spec = importlib.util.spec_from_file_location(nombre, ruta)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    if len(sys.argv) < 3:
        sys.exit("Uso: python preparar_raw_desde_repo.py <ruta-repo-SD20262-proyecto2> <carpeta-salida>")
    repo, salida = Path(sys.argv[1]).resolve(), Path(sys.argv[2]).resolve()
    raw = salida / "raw"
    if raw.exists():
        shutil.rmtree(raw)

    # 1. Sintéticos de EC2, tal cual están en el repo
    shutil.copytree(repo / "data/ec2_files/icfes_saber11_sinteticos", raw / "ec2/icfes_saber11_sinteticos")

    # 2. Tablas de RDS con el mismo código que llena la base (setup_rds.py)
    rds = cargar_modulo("setup_rds", repo / "src/scripts/setup_rds.py")
    rds.ROOT_DIR = str(repo / "data/SISE_departamentos")
    tablas = rds.build_tables(rds.clean(rds.load_sedes()))
    for tabla, df in tablas.items():
        for fecha in ["2026-11-17", "2026-11-18"]:
            destino = raw / f"rds/sedes_educativas/{tabla}/ingest_date={fecha}"
            destino.mkdir(parents=True, exist_ok=True)
            df.to_csv(destino / f"{tabla}.csv", index=False)
        print(f"rds/{tabla}: {len(df)} filas")

    # 3. Excel moderno de prueba (con CODIGODANE_SEDE) y uno corrupto
    gen = cargar_modulo("gen", repo / "src/scripts/generate_synthetic_icfes.py")
    base = gen.read_real(raw / "ec2/icfes_saber11_sinteticos/Resultados-agregados-Saber-11-2021-4-SINTETICO.xlsx")
    sedes = tablas["sede_educativa"].merge(tablas["municipio"][["id_municipio", "codigo_dane_municipio"]])
    por_mpio = sedes.dropna(subset=["cod_dane_sede"]).groupby("codigo_dane_municipio").cod_dane_sede.apply(list).to_dict()
    rng = np.random.default_rng(1)
    asignacion = {}
    for codinst, mpio in base[["CODINST", "CODIGOMUNICIPIO"]].drop_duplicates("CODINST").itertuples(index=False):
        opciones = por_mpio.get(str(mpio).zfill(5))
        if opciones:
            asignacion[codinst] = rng.choice(opciones)
    base["CODIGODANE_SEDE"] = base.CODINST.map(asignacion)
    base["PERIODO"] = "20234"
    url = raw / "url/icfes_saber11_agregados"
    url.mkdir(parents=True, exist_ok=True)
    gen.write_xlsx(base, 2023, 4, url / "Resultados-agregados-puntajes-promedio-Saber-11-2023-4-VF.xlsx")
    (url / "Resultados-agregados-puntajes-promedio-Saber-11-2019-2.xlsx").write_bytes(b"PK\x03\x04truncado")

    print(f"\nRaw de prueba listo en {raw}")
    print("Siguiente paso:")
    print(f"  python ../04-preparacion/raw_to_trusted.py --raw {raw} --trusted {salida}/trusted")


if __name__ == "__main__":
    main()
