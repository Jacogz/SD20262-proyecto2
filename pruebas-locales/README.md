# Pruebas locales

`preparar_raw_desde_repo.py` arma una zona raw local con la misma estructura y formato del bucket, usando el repositorio del equipo (`SD20262-proyecto2`):

- Los Excel sintéticos de `data/ec2_files/` (formato real del ICFES).
- Las 7 tablas de RDS, construidas con el mismo `setup_rds.build_tables()` que llena la base, en dos fechas de ingesta.
- Un Excel moderno de prueba con `CODIGODANE_SEDE` y uno corrupto, como los que publica el ICFES.

```bash
pip install pyspark==3.5.3 pandas numpy openpyxl xlrd sqlalchemy python-dotenv matplotlib
python preparar_raw_desde_repo.py .. ./prueba    # ".." = raíz de este repositorio
python ../04-preparacion/raw_to_trusted.py --raw ./prueba/raw --trusted ./prueba/trusted
python ../04-preparacion/trusted_to_refined.py --trusted ./prueba/trusted --refined ./prueba/refined
python ../06-consultas-sql/sparksql_consultas.py --modo parquet --trusted ./prueba/trusted --salida ./prueba/sql
TRUSTED=./prueba/trusted python ../07-pyspark/analisis_pyspark.py
```

Los Excel de `url/` que genera son de prueba: los resultados no responden las preguntas reales.
