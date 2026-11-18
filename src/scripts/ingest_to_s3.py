"""
Ingesta automática de las 3 fuentes de datos hacia la zona RAW del data lake (S3).

Fuentes:
  rds    -> todas las tablas de la BD `sedes_educativas` en RDS MariaDB (CSV por tabla)
  urls   -> archivos de resultados agregados Saber 11 listados en data/matches.txt (ICFES)
  files  -> archivos locales de la instancia EC2 (por defecto /data/fuentes)

Layout en S3 (zona raw / bronze):
  s3://<bucket>/raw/rds/sedes_educativas/<tabla>/ingest_date=YYYY-MM-DD/<tabla>.csv
  s3://<bucket>/raw/url/icfes_saber11_agregados/<archivo original>.xlsx
  s3://<bucket>/raw/ec2/<ruta relativa dentro de FILES_DIR>
  s3://<bucket>/raw/_manifests/ingest_date=YYYY-MM-DD/run_<timestamp>.json

Uso:
  python ingest_to_s3.py                       # las 3 fuentes
  python ingest_to_s3.py --sources rds,urls    # solo algunas
  python ingest_to_s3.py --force               # re-sube aunque ya exista en S3
  python ingest_to_s3.py --dry-run             # muestra qué haría, no sube nada

Variables de entorno (.env):
  DATALAKE_BUCKET, AWS_REGION (default us-east-1)
  DATABASE_MASTER_ENDPOINT, DATABASE_MASTER_USER, DATABASE_MASTER_PASSWORD, DB_NAME (default sedes_educativas)
  URLS_FILE (default ../../data/matches.txt), FILES_DIR (default /data/fuentes)
"""
import argparse
import json
import logging
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

load_dotenv()

HERE = Path(__file__).resolve().parent
BUCKET = os.getenv("DATALAKE_BUCKET")
REGION = os.getenv("AWS_REGION", "us-east-1")
DB_NAME = os.getenv("DB_NAME", "sedes_educativas")
URLS_FILE = Path(os.getenv("URLS_FILE", HERE / "../../data/matches.txt"))
FILES_DIR = Path(os.getenv("FILES_DIR", "/data/fuentes"))

NOW = datetime.now(timezone.utc)
INGEST_DATE = NOW.strftime("%Y-%m-%d")
RUN_ID = NOW.strftime("%Y%m%dT%H%M%SZ")

log = logging.getLogger("ingesta")
s3 = boto3.client("s3", region_name=REGION)


class Uploader:
    def __init__(self, dry_run, force):
        self.dry_run, self.force = dry_run, force
        self.manifest = []

    def exists(self, key, size=None):
        try:
            head = s3.head_object(Bucket=BUCKET, Key=key)
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
        return size is None or head["ContentLength"] == size

    def upload(self, path, key, source, skip_if_exists=False, **info):
        path = Path(path)
        size = path.stat().st_size
        if skip_if_exists and not self.force and self.exists(key, size):
            log.info("  = ya existe, se omite: s3://%s/%s", BUCKET, key)
            self.manifest.append({"key": key, "source": source, "bytes": size, "status": "skipped", **info})
            return
        log.info("  ↑ %s (%.1f KB) -> s3://%s/%s", path.name, size / 1024, BUCKET, key)
        if not self.dry_run:
            metadata = {"source": source[:1000], "ingest-run": RUN_ID, **{k: str(v) for k, v in info.items()}}
            s3.upload_file(str(path), BUCKET, key, ExtraArgs={"Metadata": metadata})
        self.manifest.append({"key": key, "source": source, "bytes": size, "status": "uploaded", **info})


def integrity(path):
    """Revisa que el archivo se pueda abrir: .xlsx es un ZIP, .xls es OLE2."""
    suffix = Path(path).suffix.lower()
    if suffix == ".xlsx":
        return "ok" if zipfile.is_zipfile(path) else "corrupt"
    if suffix == ".xls":
        with open(path, "rb") as f:
            return "ok" if f.read(8) == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1" else "corrupt"
    return "unchecked"


def ingest_rds(up: Uploader):
    endpoint = os.environ["DATABASE_MASTER_ENDPOINT"]
    user = os.environ["DATABASE_MASTER_USER"]
    password = os.environ["DATABASE_MASTER_PASSWORD"]
    engine = create_engine(f"mariadb+pymysql://{user}:{password}@{endpoint}/{DB_NAME}?charset=utf8mb4")

    tables = inspect(engine).get_table_names()
    log.info("RDS: %d tablas en %s", len(tables), DB_NAME)
    with tempfile.TemporaryDirectory() as tmp, engine.connect() as conn:
        for table in tables:
            df = pd.read_sql_table(table, conn)
            path = Path(tmp) / f"{table}.csv"
            df.to_csv(path, index=False)
            log.info("  %s: %d filas", table, len(df))
            key = f"raw/rds/{DB_NAME}/{table}/ingest_date={INGEST_DATE}/{table}.csv"
            up.upload(path, key, source=f"rds://{endpoint}/{DB_NAME}/{table}")


def filename_from_url(url):
    archivo = parse_qs(urlparse(url).query).get("archivo", [url])[0]
    return Path(urlparse(archivo).path).name


def ingest_urls(up: Uploader):
    urls = list(dict.fromkeys(l.strip() for l in URLS_FILE.read_text().splitlines() if l.strip()))
    log.info("URLs: %d archivos únicos en %s", len(urls), URLS_FILE)
    with tempfile.TemporaryDirectory() as tmp:
        for url in urls:
            name = filename_from_url(url)
            key = f"raw/url/icfes_saber11_agregados/{name}"
            if not up.force and up.exists(key):
                log.info("  = ya existe, se omite: %s", name)
                up.manifest.append({"key": key, "source": url, "status": "skipped"})
                continue
            path = Path(tmp) / name
            with requests.get(url, stream=True, timeout=120, allow_redirects=True) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
            check = integrity(path)
            if check == "corrupt":
                # Se conserva en raw (es lo que publica la fuente), pero queda marcado.
                log.warning("  ! %s no se puede abrir: el archivo publicado en el origen está incompleto", name)
            up.upload(path, key, source=url, integrity=check)


def ingest_files(up: Uploader):
    files = sorted(p for p in FILES_DIR.rglob("*") if p.is_file() and not p.name.startswith(".") and p.name != "README.md")
    log.info("EC2: %d archivos en %s", len(files), FILES_DIR)
    for p in files:
        rel = p.relative_to(FILES_DIR).as_posix()
        up.upload(p, f"raw/ec2/{rel}", source=f"ec2://{p}", skip_if_exists=True)


SOURCES = {"rds": ingest_rds, "urls": ingest_urls, "files": ingest_files}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sources", default=",".join(SOURCES))
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if not BUCKET:
        sys.exit("Falta la variable DATALAKE_BUCKET")

    up = Uploader(args.dry_run, args.force)
    status = {}
    for name in args.sources.split(","):
        try:
            SOURCES[name](up)
            status[name] = "ok"
        except Exception as e:  # una fuente caída no debe tumbar las demás
            log.exception("Fuente %s falló", name)
            status[name] = f"error: {e}"

    corrupt = [o["key"] for o in up.manifest if o.get("integrity") == "corrupt"]
    manifest = {"run_id": RUN_ID, "ingest_date": INGEST_DATE, "bucket": BUCKET,
                "sources": status, "corrupt_at_source": corrupt, "objects": up.manifest}
    if corrupt:
        log.warning("%d archivos llegaron incompletos desde el origen", len(corrupt))
    log.info("Resumen: %s", json.dumps(status))
    if not args.dry_run:
        s3.put_object(
            Bucket=BUCKET,
            Key=f"raw/_manifests/ingest_date={INGEST_DATE}/run_{RUN_ID}.json",
            Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode(),
            ContentType="application/json",
        )
    sys.exit(0 if all(v == "ok" for v in status.values()) else 1)


if __name__ == "__main__":
    main()
