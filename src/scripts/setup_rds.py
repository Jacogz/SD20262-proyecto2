import os
from glob import glob
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import (
    create_engine, text, MetaData, Table, Column, SmallInteger, Integer, String, CHAR,
    Numeric, ForeignKey, UniqueConstraint, CheckConstraint,
)

load_dotenv()

db_endpoint = os.getenv("DATABASE_MASTER_ENDPOINT")
db_user = os.getenv("DATABASE_MASTER_USER")
db_password = os.getenv("DATABASE_MASTER_PASSWORD")

DB_NAME = "sedes_educativas"
server_url = f"mariadb+pymysql://{db_user}:{db_password}@{db_endpoint}"

ROOT_DIR = os.path.join(os.path.dirname(__file__), '../data/SISE_departamentos')

CALENDARIOS = {"A": "Calendario A (febrero - noviembre)", "B": "Calendario B (agosto - junio)"}

metadata = MetaData()

def catalog_table(name, id_col, name_col, length):
    return Table(
        name, metadata,
        Column(id_col, SmallInteger, primary_key=True),
        Column(name_col, String(length), unique=True, nullable=False),
    )

Table(
    "departamento", metadata,
    Column("id_departamento", SmallInteger, primary_key=True),
    Column("codigo_dane_depto", CHAR(2), unique=True, nullable=False),
    Column("nombre_departamento", String(60), unique=True, nullable=False),
)

Table(
    "municipio", metadata,
    Column("id_municipio", Integer, primary_key=True),
    Column("codigo_dane_municipio", CHAR(5), unique=True),
    Column("nombre_municipio", String(80), nullable=False),
    Column("id_departamento", SmallInteger, ForeignKey("departamento.id_departamento"), nullable=False),
    UniqueConstraint("id_departamento", "nombre_municipio"),
)

catalog_table("estado_sede", "id_estado", "nombre_estado", 30)
catalog_table("sector_educativo", "id_sector", "nombre_sector", 20)
catalog_table("zona_geografica", "id_zona", "nombre_zona", 15)

Table(
    "tipo_calendario", metadata,
    Column("id_calendario", SmallInteger, primary_key=True),
    Column("codigo_calendario", String(10), unique=True, nullable=False),
    Column("descripcion", String(100)),
)

Table(
    "sede_educativa", metadata,
    Column("id_sede", Integer, primary_key=True, autoincrement=False),
    Column("cod_dane_sede", CHAR(12), unique=True),
    Column("nombre_institucion", String(150), nullable=False),
    Column("direccion", String(200), nullable=False),
    Column("latitud", Numeric(10, 8)),
    Column("longitud", Numeric(11, 8)),
    Column("id_municipio", Integer, ForeignKey("municipio.id_municipio"), nullable=False),
    Column("id_estado", SmallInteger, ForeignKey("estado_sede.id_estado")),
    Column("id_sector", SmallInteger, ForeignKey("sector_educativo.id_sector"), nullable=False),
    Column("id_zona", SmallInteger, ForeignKey("zona_geografica.id_zona"), nullable=False),
    Column("id_calendario", SmallInteger, ForeignKey("tipo_calendario.id_calendario"), nullable=False),
    CheckConstraint("latitud BETWEEN -4.5 AND 13.5"),
    CheckConstraint("longitud BETWEEN -82.0 AND -66.5"),
)


def load_sedes():
    return pd.concat([pd.read_excel(f) for f in glob(os.path.join(ROOT_DIR, "*.xlsx"))], ignore_index=True)


def parse_coord(col):
    return pd.to_numeric(col.astype(str).str.replace(",", ".").str.rstrip("."), errors="coerce")


def clean(df):
    df["MUNICIPIO"] = [m.removesuffix(f" {d}") for m, d in zip(df.MUNICIPIO, df.DEPARTAMENTO)]
    df["COD_DANE"] = df.COD_DANE.astype(str).where(df.COD_DANE != 0)
    lat, lon = parse_coord(df.LATITUD), parse_coord(df.LONGITUD)
    in_colombia = lat.between(-4.5, 13.5) & lon.between(-82.0, -66.5)
    df["LATITUD"] = lat.where(in_colombia).round(8)
    df["LONGITUD"] = lon.where(in_colombia).round(8)
    return df


def numbered(table, id_col):
    table = table.reset_index(drop=True)
    table.insert(0, id_col, table.index + 1)
    return table


def catalog(values, id_col, name_col):
    return numbered(pd.DataFrame({name_col: sorted(values.dropna().unique())}), id_col)


def build_tables(df):
    df["COD_MUNI"] = df.COD_DANE.str[1:6]

    depto_codes = df.groupby("DEPARTAMENTO").COD_MUNI.agg(lambda s: s.str[:2].mode()[0])
    departamento = numbered(pd.DataFrame({
        "codigo_dane_depto": depto_codes.values,
        "nombre_departamento": depto_codes.index,
    }), "id_departamento")

    muni_codes = (
        df.value_counts(["DEPARTAMENTO", "MUNICIPIO", "COD_MUNI"]).reset_index()
        .drop_duplicates(["DEPARTAMENTO", "MUNICIPIO"])
        .drop_duplicates("COD_MUNI")
    )
    municipios = numbered(
        df[["DEPARTAMENTO", "MUNICIPIO"]].drop_duplicates().merge(muni_codes, how="left"),
        "id_municipio",
    )
    municipio = pd.DataFrame({
        "id_municipio": municipios.id_municipio,
        "codigo_dane_municipio": municipios.COD_MUNI,
        "nombre_municipio": municipios.MUNICIPIO,
        "id_departamento": municipios.DEPARTAMENTO.map(departamento.set_index("nombre_departamento").id_departamento),
    })

    estado = catalog(df.ESTADO, "id_estado", "nombre_estado")
    sector = catalog(df.SECTOR, "id_sector", "nombre_sector")
    zona = catalog(df.ZONA, "id_zona", "nombre_zona")
    calendario = catalog(df.TIPO_CALENDARIO, "id_calendario", "codigo_calendario")
    calendario["descripcion"] = calendario.codigo_calendario.map(CALENDARIOS)

    sedes = df.merge(municipios[["DEPARTAMENTO", "MUNICIPIO", "id_municipio"]])
    sede = pd.DataFrame({
        "id_sede": sedes.ID_SEDE,
        "cod_dane_sede": sedes.COD_DANE,
        "nombre_institucion": sedes.NOMBRE_INSTITUCION,
        "direccion": sedes.DIRECCION,
        "latitud": sedes.LATITUD,
        "longitud": sedes.LONGITUD,
        "id_municipio": sedes.id_municipio,
        "id_estado": sedes.ESTADO.map(estado.set_index("nombre_estado").id_estado).astype("Int64"),
        "id_sector": sedes.SECTOR.map(sector.set_index("nombre_sector").id_sector),
        "id_zona": sedes.ZONA.map(zona.set_index("nombre_zona").id_zona),
        "id_calendario": sedes.TIPO_CALENDARIO.map(calendario.set_index("codigo_calendario").id_calendario),
    })

    return {
        "departamento": departamento,
        "municipio": municipio,
        "estado_sede": estado,
        "sector_educativo": sector,
        "zona_geografica": zona,
        "tipo_calendario": calendario,
        "sede_educativa": sede,
    }


if __name__ == "__main__":
    tables = build_tables(clean(load_sedes()))

    with create_engine(server_url).begin() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME} CHARACTER SET utf8mb4"))

    engine = create_engine(f"{server_url}/{DB_NAME}?charset=utf8mb4")
    metadata.drop_all(engine)
    metadata.create_all(engine)

    with engine.begin() as conn:
        for table in metadata.sorted_tables:
            tables[table.name].to_sql(table.name, conn, if_exists="append", index=False, chunksize=1000)
