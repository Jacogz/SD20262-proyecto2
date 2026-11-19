# Punto 8 – Visualización: Streamlit + API Gateway

```
S3 raw (ec2/url/rds) ──build_refined.py──▶ S3 refined/api/q1..q6.json ──▶ Lambda ──▶ API Gateway (HTTP API) ──▶ Streamlit
```

- `src/build_refined.py`: lee la zona **raw** del data lake, calcula las 6 preguntas del Punto 1 y escribe JSON en `refined/api/`.
- `api/lambda_function.py` + `api/deploy_api.sh`: Lambda y API Gateway que sirven esos JSON (`GET /api/q1 … q6`, `/api/meta`).
- `src/app.py`: app Streamlit con gráficos Plotly que consume la API.

Puntaje global = (3·Lectura + 3·Matemáticas + 3·Sociales + 3·Naturales + 1·Inglés) / 13 × 5, promedio ponderado por número de evaluados.

## 0. Requisitos
Python 3.10+, AWS CLI configurado con credenciales de TU cuenta (con acceso al bucket del compañero), `pip install -r src/requirements.txt`.

## 1. Verificar acceso al bucket (cuenta del compañero)
```bash
aws s3 ls s3://NOMBRE_BUCKET/raw/ --recursive | head
aws s3api put-object --bucket NOMBRE_BUCKET --key refined/api/prueba.txt   # debe permitir escribir en refined/
```
Si falla con AccessDenied, tu compañero debe agregar tu cuenta (`arn:aws:iam::TU_CUENTA:root`) en `TEAM_PRINCIPALS` de `src/infra/config.env` y correr `01_create_datalake.sh` de nuevo.

## 2. Generar la zona refined
```bash
cd src
python build_refined.py --bucket NOMBRE_BUCKET
```
Sube `refined/api/q1.json … q6.json` y `meta.json`. (Prueba sin AWS: `--local-dir ../../data/ec2_files --out out`.)

## 3. Crear Lambda + API Gateway
La Lambda corre en TU cuenta y lee el bucket de la otra, así que su rol necesita `s3:GetObject` sobre `arn:aws:s3:::NOMBRE_BUCKET/refined/*`
(en AWS Academy: usar `LabRole`, ya trae acceso a S3 y la bucket policy del compañero debe incluir tu cuenta).
```bash
cd api
BUCKET=NOMBRE_BUCKET ROLE_ARN=arn:aws:iam::TU_CUENTA:role/LabRole ./deploy_api.sh
curl https://XXXX.execute-api.us-east-1.amazonaws.com/api/meta
```

## 4. Correr la app Streamlit
Local:
```bash
cd src
API_URL=https://XXXX.execute-api.us-east-1.amazonaws.com streamlit run app.py
```
En EC2 (Amazon Linux, abrir puerto 8501 en el security group):
```bash
sudo mkdir -p /opt/saber11 && sudo chown ec2-user /opt/saber11
cp src/app.py src/requirements.txt src/streamlit.service /opt/saber11/ && cd /opt/saber11
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
sudo cp streamlit.service /etc/systemd/system/   # editar API_URL dentro del archivo
sudo systemctl daemon-reload && sudo systemctl enable --now streamlit
```
App en `http://IP_PUBLICA_EC2:8501`.

## Evidencias para el informe
1. `aws s3 ls s3://NOMBRE_BUCKET/refined/api/` (los 7 JSON).
2. Consola API Gateway: la API, la ruta `GET /api/{name}` y la integración con la Lambda.
3. `curl .../api/q1` en terminal.
4. Capturas de cada pestaña de la app (6 preguntas).

## Prueba local sin AWS
Desde la raíz del repositorio:
```bash
cd 08-visualizacion-api/src
pip install -r requirements.txt
python build_refined.py --local-dir ../../data/ec2_files --out out   # usa los Excel sintéticos del repo
LOCAL_DIR=./out streamlit run app.py                                  # la app lee los JSON de ./out en vez de la API
```
Sin las tablas de RDS la pestaña 2 muestra solo oficial vs no oficial (sin cruce por zona).
`out/` y `api/lambda.zip` están en `.gitignore`.

## Relación con los puntos 1 a 6
Este punto corre **después de la ingesta (punto 3)** y solo necesita la zona `raw/`: `build_refined.py`
calcula sus propias respuestas con pandas y las deja en `s3://BUCKET/refined/api/`. No lee las tablas
`p1_...` a `p7_...` que generan Glue/EMR (punto 4) ni la base `saber11` del catálogo (punto 5), y no
choca con ellas: los crawlers solo leen las carpetas `refined/p*_*/`.

Equivalencia entre los endpoints de la API y las preguntas del proyecto (ver `../01-caso-estudio/`):

| Endpoint / pestaña | Pregunta del proyecto | Tabla equivalente en refined (punto 4) |
|---|---|---|
| `q1` | P1 · Mayor y menor puntaje por departamento y municipio | `p1_puntaje_departamento`, `p1_puntaje_municipio` |
| `q2` | P2 · Oficial vs no oficial según la zona | `p2_puntaje_naturaleza_zona`, `p2_brecha_naturaleza_zona` |
| `q3` | P3 · Área más débil por región | `p3_area_mas_debil_region` |
| `q4` | P4 · Evolución por departamento | `p4_evolucion_departamento` |
| `q5` | P5 · Top 10 colegios por departamento | `p6_top10_colegios_departamento` |
| `q6` | P6 · Brecha Bogotá, Medellín, Cali vs resto | `p7_brecha_ciudades_principales` |

Diferencias de método frente a los puntos 4 a 6 (los números pueden no coincidir exactamente con Athena):
- Agrupa por año mezclando calendario A y B; los puntos 4 a 7 usan solo calendario A para "último periodo" y evolución.
- La zona urbana/rural se obtiene cruzando municipio + nombre de la institución con el directorio de sedes; el punto 4 usa `CODIGODANE_SEDE`.
- La región se asigna por nombre del departamento; el punto 4 la asigna por código DIVIPOLA.
- Umbrales: mínimo 30 evaluados por municipio (P1b del punto 6 usa 100) y 10 por colegio (igual que el punto 6).
