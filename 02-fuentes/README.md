# Punto 2 · Fuentes de datos

El código de este punto vive en [`../src/scripts/`](../src/scripts/) y los datos en [`../data/`](../data/).
**No se mueven a esta carpeta** porque los scripts usan rutas relativas a esas ubicaciones
(por ejemplo `../../data/matches.txt`).

| Fuente | Archivos | Script |
|---|---|---|
| 1. Base de datos relacional: directorio de sedes (RDS MariaDB) | [`data/SISE_departamentos/*.xlsx`](../data/SISE_departamentos/) (33 departamentos) | [`setup_rds.py`](../src/scripts/setup_rds.py) |
| 2. Archivos en URLs: resultados agregados del ICFES | [`data/matches.txt`](../data/matches.txt) (URLs), [`data/pattern.txt`](../data/pattern.txt) (expresión regular de los enlaces de descarga del ICFES) | Los descarga la ingesta (punto 3) |
| 3. Archivos en EC2: resultados sintéticos | [`data/ec2_files/`](../data/ec2_files/) | [`generate_synthetic_icfes.py`](../src/scripts/generate_synthetic_icfes.py) |

## Fuente 1 · RDS MariaDB `sedes_educativas`

`setup_rds.py` lee los Excel del SISE, limpia los datos (municipio sin el sufijo del departamento, código DANE
como texto, coordenadas fuera de Colombia como nulas), los normaliza y crea la base `sedes_educativas` con 7 tablas:

| Tabla | Contenido |
|---|---|
| `departamento` | `id_departamento`, `codigo_dane_depto`, `nombre_departamento` |
| `municipio` | `id_municipio`, `codigo_dane_municipio` (5 dígitos), `nombre_municipio`, `id_departamento` |
| `estado_sede`, `sector_educativo`, `zona_geografica` | Catálogos (id, nombre) |
| `tipo_calendario` | `id_calendario`, `codigo_calendario` (A/B), `descripcion` |
| `sede_educativa` | `id_sede`, `cod_dane_sede` (12 dígitos), institución, dirección, `latitud`, `longitud` y llaves foráneas a las tablas anteriores |

**El script borra y vuelve a crear las tablas** (`drop_all` + `create_all`) en cada ejecución.

### Cómo ejecutarlo

1. Crear una instancia RDS **MariaDB** con identificador `sedes-educativas-rdb` (el nombre que esperan los scripts
   de infraestructura en `RDS_INSTANCE_ID`), accesible desde el equipo donde se corre el script.
   **TODO (Jacobo):** documentar versión del motor, clase de instancia, VPC y security group usados.
2. Crear `.env` en la **raíz del repositorio** (está en `.gitignore`, nunca se sube):
   ```
   DATABASE_MASTER_ENDPOINT=<endpoint-rds>:3306
   DATABASE_MASTER_USER=<usuario>
   DATABASE_MASTER_PASSWORD=<clave>
   ```
3. Instalar dependencias y ejecutar:
   ```bash
   pip install pandas openpyxl sqlalchemy pymysql python-dotenv
   ln -s ../data src/data          # ver nota abajo; el enlace está en .gitignore
   python src/scripts/setup_rds.py
   ```

> **Nota sobre la ruta de los Excel.** `setup_rds.py` busca los archivos en
> `os.path.dirname(__file__) + '/../data/SISE_departamentos'`, es decir `src/data/SISE_departamentos`, pero los
> datos están en `data/SISE_departamentos` (raíz). El enlace simbólico `src/data → ../data` lo resuelve sin tocar
> el código. **TODO (Jacobo):** cambiar la ruta a `'../../data/SISE_departamentos'` y quitar este paso.
> `pruebas-locales/preparar_raw_desde_repo.py` ya sobrescribe `ROOT_DIR`, así que no lo necesita.

## Fuente 2 · URLs del ICFES

[`data/matches.txt`](../data/matches.txt) tiene un enlace por periodo (2012 a 2025-2) a la página de descarga del ICFES
(`registro-descarga?archivo=...`). La ingesta extrae el nombre real del archivo del parámetro `archivo`, elimina
duplicados (2018-1 aparece dos veces) y revisa que cada Excel se pueda abrir: varios periodos llegan
**truncados desde el servidor del ICFES**. Se guardan igual en raw, marcados como corruptos en el manifiesto.

Para agregar un periodo nuevo basta con añadir su URL a `matches.txt` y volver a desplegar la ingesta (punto 3).

## Fuente 3 · Archivos en EC2 (sintéticos)

Para los periodos cuyo Excel del ICFES está corrupto, `generate_synthetic_icfes.py` genera un Excel **sintético**
con el mismo formato que usaba el ICFES en esa época. Toma el universo de sedes del periodo real más cercano del
mismo calendario y construye puntajes a partir de los promedios reales de cada institución, una tendencia nacional
supuesta y ruido. El título y el nombre del archivo dicen `SINTETICO`, y los puntos 4 a 8 marcan esas filas.

```bash
pip install numpy pandas openpyxl
python src/scripts/generate_synthetic_icfes.py                       # descarga desde matches.txt
python src/scripts/generate_synthetic_icfes.py --source-dir /ruta/xlsx  # o usa archivos ya descargados
# salida: data/ec2_files/icfes_saber11_sinteticos/Resultados-agregados-Saber-11-<AAAA>-<P>-SINTETICO.xlsx
```

Los 8 archivos ya generados están en el repositorio (semilla `--seed 2026`). Todo lo que haya en `data/ec2_files/`
se copia a `/data/fuentes/` en la EC2 de ingesta (ver [`data/ec2_files/README.md`](../data/ec2_files/README.md)).

## Evidencias sugeridas
1. Consola RDS: la instancia `sedes-educativas-rdb` disponible.
2. Cliente SQL: `SHOW TABLES` y `SELECT COUNT(*) FROM sede_educativa` en `sedes_educativas`.
3. Terminal de la EC2: `ls ~/fuentes/icfes_saber11_sinteticos/`.
