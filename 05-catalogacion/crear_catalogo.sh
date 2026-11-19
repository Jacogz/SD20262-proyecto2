#!/usr/bin/env bash
# =============================================================================
# Punto 5 - Catalogación con AWS Glue.
# Crea la base de datos "saber11" en el Glue Data Catalog y dos crawlers:
#   saber11-trusted : tablas trusted_colegio_periodo, trusted_sede, trusted_departamento (y trusted_estudiante si existe)
#   saber11-refined : una tabla por cada pregunta (p1_..., p2_..., ..., p7_...)
#
# Se usa un "target" de S3 por tabla para que el crawler cree exactamente una tabla por carpeta
# (si se apunta a la carpeta padre, Glue puede fusionar tablas con esquemas parecidos).
#
# Uso (CloudShell o terminal con AWS CLI; antes: bash ../04-preparacion/crear_roles.sh, salvo en Academy):
#   export BUCKET=nombre-de-su-bucket
#   bash crear_catalogo.sh
# =============================================================================
set -euo pipefail

: "${BUCKET:?Defina BUCKET, ej: export BUCKET=saber11-datalake-equipo}"
REGION="${AWS_REGION:-us-east-1}"
DB="saber11"
ROL="${GLUE_ROLE:-saber11-glue-role}"   # cuenta normal: crear_roles.sh | AWS Academy: export GLUE_ROLE=LabRole

TABLAS_TRUSTED=(colegio_periodo sede departamento estudiante)   # estudiante solo existe si se carga esa fuente
TABLAS_REFINED=(
  p1_puntaje_departamento p1_puntaje_municipio
  p2_puntaje_naturaleza_zona p2_brecha_naturaleza_zona
  p3_area_mas_debil_region
  p4_evolucion_departamento
  p5_puntaje_estrato p5_puntaje_educ_padres p5_puntaje_estrato_educ_padres p5_correlaciones
  p6_top10_colegios_departamento p6_concentracion_top10_municipio
  p7_brecha_ciudades_principales
)

targets_json () {
  local zona="$1"; shift
  local primero=1
  printf '{"S3Targets":['
  for t in "$@"; do
    [[ $primero -eq 1 ]] || printf ','
    printf '{"Path":"s3://%s/%s/%s/"}' "$BUCKET" "$zona" "$t"
    primero=0
  done
  printf ']}'
}

echo ">> Base de datos $DB"
aws glue create-database --region "$REGION" \
  --database-input "{\"Name\":\"$DB\",\"Description\":\"Datalake Saber 11 - Proyecto 2 Big Data\"}" 2>/dev/null \
  || echo "   (ya existía)"

crear_crawler () {
  local nombre="$1" prefijo="$2" targets="$3"
  echo ">> Crawler $nombre"
  aws glue delete-crawler --region "$REGION" --name "$nombre" 2>/dev/null || true
  aws glue create-crawler --region "$REGION" \
    --name "$nombre" \
    --role "$ROL" \
    --database-name "$DB" \
    --table-prefix "$prefijo" \
    --targets "$targets" \
    --schema-change-policy '{"UpdateBehavior":"UPDATE_IN_DATABASE","DeleteBehavior":"LOG"}' \
    --recrawl-policy '{"RecrawlBehavior":"CRAWL_EVERYTHING"}'
}

crear_crawler "saber11-trusted" "trusted_" "$(targets_json trusted "${TABLAS_TRUSTED[@]}")"
crear_crawler "saber11-refined" ""         "$(targets_json refined "${TABLAS_REFINED[@]}")"

for c in saber11-trusted saber11-refined; do
  echo ">> Ejecutando $c"
  aws glue start-crawler --region "$REGION" --name "$c"
done

echo ">> Esperando a que terminen los crawlers..."
for c in saber11-trusted saber11-refined; do
  while true; do
    estado=$(aws glue get-crawler --region "$REGION" --name "$c" --query 'Crawler.State' --output text)
    [[ "$estado" == "READY" ]] && break
    sleep 15
  done
  ultimo=$(aws glue get-crawler --region "$REGION" --name "$c" --query 'Crawler.LastCrawl.Status' --output text)
  echo "   $c: $ultimo"
done

echo ">> Tablas en el catálogo:"
aws glue get-tables --region "$REGION" --database-name "$DB" --query 'TableList[].Name' --output table
