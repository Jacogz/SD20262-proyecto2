#!/usr/bin/env bash
# Crea el bucket S3 del data lake con zonas raw / trusted / refined.
# Idempotente: se puede correr varias veces.
source "$(dirname "$0")/common.sh"

echo "Bucket: s3://$DATALAKE_BUCKET  (cuenta $ACCOUNT_ID, región $AWS_REGION)"

if aws s3api head-bucket --bucket "$DATALAKE_BUCKET" >/dev/null 2>&1; then
  echo "  ya existe"
elif [[ "$AWS_REGION" == "us-east-1" ]]; then
  aws s3api create-bucket --bucket "$DATALAKE_BUCKET"
else
  aws s3api create-bucket --bucket "$DATALAKE_BUCKET" \
    --create-bucket-configuration LocationConstraint="$AWS_REGION"
fi

aws s3api put-public-access-block --bucket "$DATALAKE_BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
aws s3api put-bucket-encryption --bucket "$DATALAKE_BUCKET" --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-versioning --bucket "$DATALAKE_BUCKET" --versioning-configuration Status=Enabled

# Zonas del data lake (carpetas "vacías" para que se vean en la consola)
for zone in raw trusted refined athena-results; do
  aws s3api put-object --bucket "$DATALAKE_BUCKET" --key "$zone/" >/dev/null
done
echo "  zonas: raw/ trusted/ refined/ athena-results/"

# Política: solo HTTPS + acceso para compañeros de otras cuentas (si hay)
principals_json="$(printf '%s\n' $TEAM_PRINCIPALS | sed '/^$/d' | sed 's/.*/"&"/' | paste -sd, -)"
statements='{
  "Sid": "DenyInsecureTransport", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
  "Resource": ["arn:aws:s3:::'"$DATALAKE_BUCKET"'", "arn:aws:s3:::'"$DATALAKE_BUCKET"'/*"],
  "Condition": {"Bool": {"aws:SecureTransport": "false"}}
}'
if [[ -n "$principals_json" ]]; then
  statements+=',{
    "Sid": "TeamList", "Effect": "Allow", "Principal": {"AWS": ['"$principals_json"']},
    "Action": ["s3:ListBucket", "s3:GetBucketLocation"], "Resource": "arn:aws:s3:::'"$DATALAKE_BUCKET"'"
  },{
    "Sid": "TeamReadAll", "Effect": "Allow", "Principal": {"AWS": ['"$principals_json"']},
    "Action": "s3:GetObject", "Resource": "arn:aws:s3:::'"$DATALAKE_BUCKET"'/*"
  },{
    "Sid": "TeamWriteProcessedZones", "Effect": "Allow", "Principal": {"AWS": ['"$principals_json"']},
    "Action": ["s3:PutObject", "s3:DeleteObject"],
    "Resource": ["arn:aws:s3:::'"$DATALAKE_BUCKET"'/trusted/*", "arn:aws:s3:::'"$DATALAKE_BUCKET"'/refined/*",
                 "arn:aws:s3:::'"$DATALAKE_BUCKET"'/athena-results/*"]
  }'
  echo "  acceso otorgado a: $TEAM_PRINCIPALS"
fi
aws s3api put-bucket-policy --bucket "$DATALAKE_BUCKET" \
  --policy '{"Version":"2012-10-17","Statement":['"$statements"']}'

echo "Listo: s3://$DATALAKE_BUCKET"
