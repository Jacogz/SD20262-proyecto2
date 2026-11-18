#!/usr/bin/env bash
# Crea la Lambda + API Gateway (HTTP API) del punto 8. Idempotente a medias: si ya existe, la actualiza.
# Uso:  BUCKET=nombre-del-bucket ROLE_ARN=arn:aws:iam::<TU_CUENTA>:role/LabRole ./deploy_api.sh
set -euo pipefail
: "${BUCKET:?falta BUCKET}"; : "${ROLE_ARN:?falta ROLE_ARN}"
REGION="${AWS_REGION:-us-east-1}"; FN="saber11-api"; API="saber11-api"
cd "$(dirname "$0")"
zip -q -j lambda.zip lambda_function.py

if aws lambda get-function --function-name "$FN" --region "$REGION" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$FN" --zip-file fileb://lambda.zip --region "$REGION" >/dev/null
  aws lambda update-function-configuration --function-name "$FN" --environment "Variables={BUCKET=$BUCKET}" --region "$REGION" >/dev/null
else
  aws lambda create-function --function-name "$FN" --runtime python3.12 --handler lambda_function.lambda_handler \
    --zip-file fileb://lambda.zip --role "$ROLE_ARN" --timeout 15 --memory-size 256 \
    --environment "Variables={BUCKET=$BUCKET}" --region "$REGION" >/dev/null
fi
FN_ARN=$(aws lambda get-function --function-name "$FN" --region "$REGION" --query Configuration.FunctionArn --output text)

# HTTP API con integración Lambda (quick create) y ruta GET /api/{name}
API_ID=$(aws apigatewayv2 get-apis --region "$REGION" --query "Items[?Name=='$API'].ApiId | [0]" --output text)
if [[ "$API_ID" == "None" || -z "$API_ID" ]]; then
  API_ID=$(aws apigatewayv2 create-api --name "$API" --protocol-type HTTP --region "$REGION" \
    --cors-configuration AllowOrigins='*',AllowMethods='GET' --query ApiId --output text)
  INT_ID=$(aws apigatewayv2 create-integration --api-id "$API_ID" --integration-type AWS_PROXY \
    --integration-uri "$FN_ARN" --payload-format-version 2.0 --region "$REGION" --query IntegrationId --output text)
  aws apigatewayv2 create-route --api-id "$API_ID" --route-key 'GET /api/{name}' --target "integrations/$INT_ID" --region "$REGION" >/dev/null
  aws apigatewayv2 create-stage --api-id "$API_ID" --stage-name '$default' --auto-deploy --region "$REGION" >/dev/null
  ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
  aws lambda add-permission --function-name "$FN" --statement-id apigw-invoke --action lambda:InvokeFunction \
    --principal apigateway.amazonaws.com --source-arn "arn:aws:execute-api:$REGION:$ACCOUNT:$API_ID/*/*/api/*" --region "$REGION" >/dev/null
fi
echo "API lista: https://$API_ID.execute-api.$REGION.amazonaws.com"
echo "Prueba:    curl https://$API_ID.execute-api.$REGION.amazonaws.com/api/meta"
