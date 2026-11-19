"""
Punto 6 - Ejecuta en AWS Athena todas las consultas de consultas.sql e imprime los resultados.

Uso (CloudShell o terminal con credenciales del Learner Lab):
  pip install boto3
  python athena_ejecutar.py --bucket SU_BUCKET            # todas
  python athena_ejecutar.py --bucket SU_BUCKET --solo P2  # una sola

Los resultados de Athena quedan en s3://SU_BUCKET/athena-results/.
Para las capturas del documento, también puede pegar cada consulta en el editor de la consola de Athena
(base de datos: saber11).
"""
import argparse
import os
import re
import time

import boto3


def leer_consultas(ruta_sql):
    with open(ruta_sql, encoding="utf-8") as f:
        texto = f.read()
    bloques = re.split(r"^-- @(P\w+): (.*)$", texto, flags=re.M)
    salida = []
    for i in range(1, len(bloques), 3):
        cuerpo = "\n".join(l for l in bloques[i + 2].splitlines() if not l.strip().startswith("--"))
        salida.append((bloques[i], bloques[i + 1].strip(), cuerpo.strip().rstrip(";")))
    return salida


def ejecutar(athena, sql, base, salida_s3):
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": base},
        ResultConfiguration={"OutputLocation": salida_s3},
    )["QueryExecutionId"]
    while True:
        estado = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if estado["State"] != "SUCCEEDED":
        raise RuntimeError(estado.get("StateChangeReason", estado["State"]))
    filas = athena.get_query_results(QueryExecutionId=qid, MaxResults=50)["ResultSet"]["Rows"]
    return qid, [[c.get("VarCharValue", "") for c in f["Data"]] for f in filas]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--base_datos", default="saber11")
    p.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))
    p.add_argument("--solo", help="Ejecutar solo las consultas cuyo id empiece así (ej. P2, P5)")
    p.add_argument("--sql", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "consultas.sql"))
    a = p.parse_args()

    athena = boto3.client("athena", region_name=a.region)
    salida = f"s3://{a.bucket}/athena-results/"
    for qid, titulo, sql in leer_consultas(a.sql):
        if a.solo and not qid.upper().startswith(a.solo.upper()):
            continue
        print(f"\n===== {qid}: {titulo} =====")
        try:
            ejecucion, filas = ejecutar(athena, sql, a.base_datos, salida)
            anchos = [max(len(str(f[i])) for f in filas) for i in range(len(filas[0]))] if filas else []
            for f in filas:
                print(" | ".join(str(v).ljust(w) for v, w in zip(f, anchos)))
            print(f"(QueryExecutionId: {ejecucion})")
        except Exception as e:
            print(f"ERROR: {e}")


if __name__ == "__main__":
    main()
