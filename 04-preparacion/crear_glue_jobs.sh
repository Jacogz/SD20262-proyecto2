#!/usr/bin/env bash
# =============================================================================
# Punto 4 - Crea en AWS Glue los dos jobs de preparación y un Workflow que los
# encadena con los crawlers del punto 5, para que todo corra automático:
#
#   [ingesta de Jaco] -> saber11-raw-to-trusted -> saber11-trusted-to-refined -> crawlers
#
# Uso (CloudShell o terminal con AWS CLI; antes: bash crear_roles.sh, salvo en Academy):
#   export BUCKET=nombre-de-su-bucket
#   bash crear_glue_jobs.sh
# =============================================================================
set -euo pipefail
: "${BUCKET:?Defina BUCKET, ej: export BUCKET=saber11-datalake-equipo}"
REGION="${AWS_REGION:-us-east-1}"
ROL="${GLUE_ROLE:-saber11-glue-role}"   # cuenta normal: crear_roles.sh | AWS Academy: export GLUE_ROLE=LabRole
DIR="$(cd "$(dirname "$0")" && pwd)"

echo ">> Subiendo scripts a s3://$BUCKET/scripts/"
aws s3 cp "$DIR/raw_to_trusted.py"     "s3://$BUCKET/scripts/raw_to_trusted.py"
aws s3 cp "$DIR/trusted_to_refined.py" "s3://$BUCKET/scripts/trusted_to_refined.py"

crear_job () {
  local nombre="$1" script="$2" args="$3"
  echo ">> Job $nombre"
  aws glue delete-job --region "$REGION" --job-name "$nombre" >/dev/null 2>&1 || true
  aws glue create-job --region "$REGION" \
    --name "$nombre" \
    --role "$ROL" \
    --glue-version "4.0" \
    --worker-type "G.1X" --number-of-workers 2 \
    --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/$script\",\"PythonVersion\":\"3\"}" \
    --default-arguments "$args" >/dev/null
}

crear_job "saber11-raw-to-trusted" "raw_to_trusted.py" \
  "{\"--raw\":\"s3://$BUCKET/raw\",\"--trusted\":\"s3://$BUCKET/trusted\",\"--additional-python-modules\":\"openpyxl==3.1.2,xlrd==2.0.1\",\"--enable-continuous-cloudwatch-log\":\"true\",\"--TempDir\":\"s3://$BUCKET/tmp/\"}"

crear_job "saber11-trusted-to-refined" "trusted_to_refined.py" \
  "{\"--trusted\":\"s3://$BUCKET/trusted\",\"--refined\":\"s3://$BUCKET/refined\",\"--enable-continuous-cloudwatch-log\":\"true\",\"--TempDir\":\"s3://$BUCKET/tmp/\"}"

echo ">> Workflow saber11-pipeline"
WF="saber11-pipeline"
for t in saber11-inicio saber11-despues-trusted saber11-despues-refined; do
  aws glue delete-trigger --region "$REGION" --name "$t" >/dev/null 2>&1 || true
done
aws glue delete-workflow --region "$REGION" --name "$WF" >/dev/null 2>&1 || true
aws glue create-workflow --region "$REGION" --name "$WF" \
  --description "raw -> trusted -> refined -> crawlers (Saber 11)" >/dev/null

# 1) Inicio: diario a las 6:00 a.m. hora Colombia (11:00 UTC). También se puede iniciar a mano.
aws glue create-trigger --region "$REGION" --name saber11-inicio --workflow-name "$WF" \
  --type SCHEDULED --schedule "cron(0 11 * * ? *)" --start-on-creation \
  --actions '[{"JobName":"saber11-raw-to-trusted"}]' >/dev/null

# 2) Cuando termina bien raw->trusted, corre trusted->refined
aws glue create-trigger --region "$REGION" --name saber11-despues-trusted --workflow-name "$WF" \
  --type CONDITIONAL --start-on-creation \
  --predicate '{"Conditions":[{"LogicalOperator":"EQUALS","JobName":"saber11-raw-to-trusted","State":"SUCCEEDED"}]}' \
  --actions '[{"JobName":"saber11-trusted-to-refined"}]' >/dev/null

# 3) Cuando termina bien trusted->refined, se actualiza el catálogo (crawlers del punto 5)
aws glue create-trigger --region "$REGION" --name saber11-despues-refined --workflow-name "$WF" \
  --type CONDITIONAL --start-on-creation \
  --predicate '{"Conditions":[{"LogicalOperator":"EQUALS","JobName":"saber11-trusted-to-refined","State":"SUCCEEDED"}]}' \
  --actions '[{"CrawlerName":"saber11-trusted"},{"CrawlerName":"saber11-refined"}]' >/dev/null

echo ">> Listo. Para correr el pipeline completo ahora:"
echo "   aws glue start-workflow-run --name $WF"
echo "   Seguimiento: consola de AWS Glue > Workflows > $WF > History"
