"""
Punto 8 - Lambda detrás de API Gateway (HTTP API, ruta GET /api/{name}).

Devuelve el JSON precalculado en s3://<BUCKET>/refined/api/<name>.json
  GET /api/q1 ... /api/q6  -> respuesta de cada pregunta
  GET /api/meta            -> resumen del dataset

Variables de entorno de la Lambda:
  BUCKET   nombre del bucket del data lake (cuenta del compañero)
  PREFIX   (opcional) por defecto refined/api/
"""
import json
import os

import boto3
from botocore.exceptions import ClientError

BUCKET = os.environ["BUCKET"]
PREFIX = os.environ.get("PREFIX", "refined/api/")
VALID = {"q1", "q2", "q3", "q4", "q5", "q6", "meta"}
s3 = boto3.client("s3")

HEADERS = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


def _resp(status, body):
    return {"statusCode": status, "headers": HEADERS, "body": body if isinstance(body, str) else json.dumps(body)}


def lambda_handler(event, context):
    name = (event.get("pathParameters") or {}).get("name") or event.get("rawPath", "").rsplit("/", 1)[-1]
    if name not in VALID:
        return _resp(404, {"error": f"endpoint inválido: {name}", "validos": sorted(VALID)})
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{PREFIX}{name}.json")
        return _resp(200, obj["Body"].read().decode("utf-8"))
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            return _resp(404, {"error": f"{name}.json aún no existe en refined/. Corre build_refined.py"})
        return _resp(500, {"error": code})
