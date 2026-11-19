# Punto 7 · Análisis descriptivo con PySpark

`analisis_pyspark.ipynb` responde las 7 preguntas **solo con la API de DataFrames** de PySpark (sin SQL): `groupBy`, `pivot`, funciones de ventana (`row_number`, `lag`), `explode`, `stat.corr`. `analisis_pyspark.py` es el mismo contenido como script.

También genera los gráficos de `graficos/`, que sirven para el documento y como referencia para el Power BI.

> Los gráficos de `graficos/` se generaron en **Amazon EMR el 2026-11-18** sobre el bucket real. `p7_ciudades.png` se regeneró con `grafico_p7_calendario_a.py` (solo calendario A, consultando Athena) porque el original mezclaba periodos de calendario B.

- P2 usa la zona de la sede (directorio de RDS); P4 usa solo calendario A y grafica el promedio nacional más los 3 mejores y 3 peores departamentos, marcando con `*` los años con datos sintéticos.
- **P5 se omite con un mensaje si no existe `trusted/estudiante`** (datos por estudiante).

## Ejecutar en EMR

**Opción A: notebook (JupyterHub del clúster, para capturas)**
1. EMR › clúster › *Application user interfaces* → JupyterHub (usuario `jovyan`, clave `jupyter`).
2. Subir `analisis_pyspark.ipynb` y elegir el kernel **PySpark**.
3. En la primera celda de código cambiar `TRUSTED` por `s3://SU_BUCKET/trusted`.
4. Ejecutar todas las celdas.

**Opción B: como step**

```bash
aws emr add-steps --cluster-id j-XXXX --steps \
 'Type=CUSTOM_JAR,Name=P7-pyspark,ActionOnFailure=CONTINUE,Jar=command-runner.jar,Args=[bash,-c,"TRUSTED=s3://BUCKET/trusted GRAFICOS=/tmp/graficos spark-submit --deploy-mode client s3://BUCKET/scripts/analisis_pyspark.py"]'
```

En modo step, los gráficos quedan en el disco del nodo maestro; para las capturas conviene la opción A.

## Gráfico P7 (calendario A)

`grafico_p7_calendario_a.py` consulta `trusted_colegio_periodo` en Athena (base `saber11`) solo con periodos de calendario A y guarda `p7_ciudades.png` en la carpeta actual. Requiere `boto3` y `matplotlib`, y credenciales de AWS (por ejemplo, en CloudShell):

```bash
pip install boto3 matplotlib
python3 grafico_p7_calendario_a.py --bucket <BUCKET>   # opcional: --region us-east-1
```

## Ejecutar en local

```bash
pip install pyspark==3.5.3 matplotlib pandas jupyter
TRUSTED=../pruebas-locales/prueba/trusted python analisis_pyspark.py   # ver pruebas-locales/README.md
```

## Capturas para el documento
1. Notebook en JupyterHub de EMR con el kernel PySpark y salidas de al menos 3 preguntas.
2. Las correlaciones de P5 (si se cargan datos por estudiante).
3. Los gráficos de `graficos/` generados en EMR.
