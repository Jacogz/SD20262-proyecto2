# Punto 7 · Análisis descriptivo con PySpark

`analisis_pyspark.ipynb` responde las 7 preguntas **solo con la API de DataFrames** de PySpark (sin SQL): `groupBy`, `pivot`, funciones de ventana (`row_number`, `lag`), `explode`, `stat.corr`. `analisis_pyspark.py` es el mismo contenido como script.

También genera los gráficos de `graficos/`, que sirven para el documento y como referencia para el Power BI.

> **Los gráficos y salidas que hay ahora en el repositorio salen de una prueba local** (archivos sintéticos del repo y un Excel 2023-4 de prueba). No los usen en el documento: vuelvan a ejecutar el notebook en EMR sobre el bucket.

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

## Ejecutar en local

```bash
pip install pyspark==3.5.3 matplotlib pandas jupyter
TRUSTED=../pruebas-locales/prueba/trusted python analisis_pyspark.py   # ver pruebas-locales/README.md
```

## Capturas para el documento
1. Notebook en JupyterHub de EMR con el kernel PySpark y salidas de al menos 3 preguntas.
2. Las correlaciones de P5 (si se cargan datos por estudiante).
3. Los gráficos de `graficos/` generados en EMR.
