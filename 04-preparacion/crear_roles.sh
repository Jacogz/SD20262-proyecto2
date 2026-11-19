#!/usr/bin/env bash
# =============================================================================
# Crea los roles de IAM que necesitan Glue y EMR en una cuenta de AWS normal (no Academy).
# En AWS Academy NO se usa este script: allí se trabaja con LabRole (export GLUE_ROLE=LabRole).
#
#   saber11-glue-role   rol de servicio de Glue (jobs y crawlers) con acceso al bucket del datalake
#   EMR_DefaultRole / EMR_EC2_DefaultRole   roles por defecto de EMR (aws emr create-default-roles)
#
# Uso:
#   export BUCKET=saber11-datalake-XXXXXXXX
#   bash crear_roles.sh
# =============================================================================
set -euo pipefail
: "${BUCKET:?Defina BUCKET}"
ROL="${GLUE_ROLE:-saber11-glue-role}"

echo ">> Rol de Glue: $ROL"
aws iam get-role --role-name "$ROL" >/dev/null 2>&1 || aws iam create-role --role-name "$ROL" \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]
  }' >/dev/null

# Permisos base del servicio Glue (catálogo, CloudWatch Logs, etc.)
aws iam attach-role-policy --role-name "$ROL" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Lectura y escritura solo sobre el bucket del datalake
aws iam put-role-policy --role-name "$ROL" --policy-name saber11-datalake-s3 --policy-document "{
  \"Version\": \"2012-10-17\",
  \"Statement\": [
    {\"Effect\": \"Allow\", \"Action\": [\"s3:ListBucket\", \"s3:GetBucketLocation\"],
     \"Resource\": \"arn:aws:s3:::$BUCKET\"},
    {\"Effect\": \"Allow\", \"Action\": [\"s3:GetObject\", \"s3:PutObject\", \"s3:DeleteObject\"],
     \"Resource\": \"arn:aws:s3:::$BUCKET/*\"}
  ]
}"

echo ">> Roles por defecto de EMR"
aws emr create-default-roles >/dev/null

echo ">> Listo. ARN del rol de Glue:"
aws iam get-role --role-name "$ROL" --query 'Role.Arn' --output text
echo "   Si el bucket está en OTRA cuenta, el dueño del bucket debe dar permiso a este ARN"
echo "   (ver 'Bucket en otra cuenta' en README_puntos_4_a_7.md)."
