"""
Regenera el gráfico de P7 solo con periodos de calendario A (uno por año), consultando Athena.
Uso (CloudShell):  python3 grafico_p7_calendario_a.py --bucket NOMBRE_BUCKET
Salida: p7_ciudades.png en la carpeta actual.
"""
import argparse
import time

import boto3
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

SQL = """
SELECT CAST(SUBSTR(periodo, 1, 4) AS INTEGER) AS anio,
       MAX(CASE WHEN es_sintetico THEN 1 ELSE 0 END) AS sintetico,
       CASE cod_mpio WHEN '11001' THEN 'BOGOTA D.C.' WHEN '05001' THEN 'MEDELLIN'
                     WHEN '76001' THEN 'CALI' ELSE 'RESTO DEL PAIS' END AS grupo,
       ROUND(SUM(prom_global * evaluados) / SUM(evaluados), 2) AS prom_global
FROM trusted_colegio_periodo
WHERE semestre <> 1 AND prom_global IS NOT NULL
GROUP BY 1, 3
ORDER BY 1, 3
"""


def consultar(bucket, region):
    athena = boto3.client("athena", region_name=region)
    qid = athena.start_query_execution(
        QueryString=SQL,
        QueryExecutionContext={"Database": "saber11"},
        ResultConfiguration={"OutputLocation": f"s3://{bucket}/athena-results/"},
    )["QueryExecutionId"]
    while True:
        estado = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if estado["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if estado["State"] != "SUCCEEDED":
        raise SystemExit(f"Athena: {estado['State']} {estado.get('StateChangeReason', '')}")
    filas = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
    return [[c.get("VarCharValue") for c in f["Data"]] for f in filas]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--region", default="us-east-1")
    a = p.parse_args()

    datos = consultar(a.bucket, a.region)
    anios = sorted({int(f[0]) for f in datos})
    sinteticos = {int(f[0]) for f in datos if f[1] == "1"}
    grupos = ["BOGOTA D.C.", "MEDELLIN", "CALI", "RESTO DEL PAIS"]
    valor = {(int(f[0]), f[2]): float(f[3]) for f in datos}

    fig, ax = plt.subplots(figsize=(10, 5))
    for g in grupos:
        ax.plot(anios, [valor.get((y, g)) for y in anios], marker="o",
                linewidth=3 if g == "RESTO DEL PAIS" else 1.8,
                color="black" if g == "RESTO DEL PAIS" else None, label=g)
    ax.set_xticks(anios)
    ax.set_xticklabels([f"{y}*" if y in sinteticos else str(y) for y in anios])
    ax.set_title("P7. Bogotá, Medellín y Cali frente al resto del país (calendario A)")
    ax.set_xlabel("Año   (* = periodo con datos sintéticos)")
    ax.set_ylabel("Puntaje global promedio")
    ax.grid(axis="y", alpha=0.3)
    ax.legend(loc="upper left", bbox_to_anchor=(1.01, 1))
    fig.tight_layout()
    fig.savefig("p7_ciudades.png", dpi=150)
    print("Gráfico guardado: p7_ciudades.png")


if __name__ == "__main__":
    main()
