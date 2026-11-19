#!/usr/bin/env bash
# =============================================================================
# Puntos 4, 6 y 7 en Amazon EMR (Spark).
#
#   bash emr_ejecutar.sh crear          crea el clúster (Spark + Hive + JupyterHub, Glue Catalog como metastore)
#   bash emr_ejecutar.sh pasos <id>     ejecuta los jobs de Spark y las consultas como "steps"
#   bash emr_ejecutar.sh terminar <id>  TERMINA el clúster (hágalo siempre al acabar: gasta crédito)
#
# Requiere: export BUCKET=nombre-de-su-bucket   (y haber corrido crear_catalogo.sh para el modo catálogo)
# Opcional: export KEY_NAME=mi-llave   para entrar por SSH al nodo maestro (en Academy: vockey).
#           Sin llave, se entra con EMR Studio / JupyterHub desde la consola.
# Cuenta normal: los roles EMR_DefaultRole y EMR_EC2_DefaultRole los crea crear_roles.sh.
# COSTO: 3 x m5.xlarge ~ US$0,7/hora en cuenta personal. Termínelo siempre al acabar.
# =============================================================================
set -euo pipefail
: "${BUCKET:?Defina BUCKET}"
REGION="${AWS_REGION:-us-east-1}"
RAIZ="$(cd "$(dirname "$0")/.." && pwd)"
RELEASE="${EMR_RELEASE:-emr-7.1.0}"   # si la consola de Academy no la ofrece, use la más reciente disponible

case "${1:-}" in
  crear)
    aws s3 cp "$RAIZ/04-preparacion/bootstrap_emr.sh"        "s3://$BUCKET/scripts/bootstrap_emr.sh"
    aws s3 cp "$RAIZ/04-preparacion/raw_to_trusted.py"       "s3://$BUCKET/scripts/raw_to_trusted.py"
    aws s3 cp "$RAIZ/04-preparacion/trusted_to_refined.py"   "s3://$BUCKET/scripts/trusted_to_refined.py"
    aws s3 cp "$RAIZ/06-consultas-sql/sparksql_consultas.py" "s3://$BUCKET/scripts/sparksql_consultas.py"
    aws s3 cp "$RAIZ/06-consultas-sql/consultas.sql"         "s3://$BUCKET/scripts/consultas.sql"
    aws s3 cp "$RAIZ/06-consultas-sql/consultas_hive.sql"    "s3://$BUCKET/scripts/consultas_hive.sql"
    aws s3 cp "$RAIZ/07-pyspark/analisis_pyspark.py"         "s3://$BUCKET/scripts/analisis_pyspark.py"

    SUBNET=$(aws ec2 describe-subnets --region "$REGION" --filters Name=default-for-az,Values=true \
             --query 'Subnets[0].SubnetId' --output text)
    aws emr create-cluster --region "$REGION" \
      --name "saber11-emr" \
      --release-label "$RELEASE" \
      --applications Name=Spark Name=Hive Name=Hadoop Name=JupyterHub Name=Livy \
      --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.xlarge \
                        InstanceGroupType=CORE,InstanceCount=2,InstanceType=m5.xlarge \
      --use-default-roles \
      --ec2-attributes "${KEY_NAME:+KeyName=$KEY_NAME,}SubnetId=$SUBNET" \
      --configurations "file://$RAIZ/05-catalogacion/emr-configuracion-glue-catalog.json" \
      --bootstrap-actions "Path=s3://$BUCKET/scripts/bootstrap_emr.sh,Name=librerias" \
      --log-uri "s3://$BUCKET/logs/emr/" \
      --auto-termination-policy IdleTimeout=3600 \
      --query 'ClusterId' --output text
    echo "Espere a que el estado sea WAITING:  aws emr describe-cluster --cluster-id <id> --query Cluster.Status.State"
    ;;

  pasos)
    ID="${2:?Indique el id del clúster (j-XXXX)}"
    aws emr add-steps --region "$REGION" --cluster-id "$ID" --steps \
      "Type=Spark,Name=P4-raw-to-trusted,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,s3://$BUCKET/scripts/raw_to_trusted.py,--raw,s3://$BUCKET/raw,--trusted,s3://$BUCKET/trusted]" \
      "Type=Spark,Name=P4-trusted-to-refined,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,s3://$BUCKET/scripts/trusted_to_refined.py,--trusted,s3://$BUCKET/trusted,--refined,s3://$BUCKET/refined]" \
      "Type=Spark,Name=P6-sparksql,ActionOnFailure=CONTINUE,Args=[--deploy-mode,cluster,--files,s3://$BUCKET/scripts/consultas.sql,s3://$BUCKET/scripts/sparksql_consultas.py,--modo,catalogo,--sql,consultas.sql,--salida,s3://$BUCKET/refined/consultas_sparksql]" \
      "Type=Hive,Name=P6-hive,ActionOnFailure=CONTINUE,Args=[-f,s3://$BUCKET/scripts/consultas_hive.sql]" \
      --query 'StepIds' --output text
    echo "Seguimiento: consola de EMR > clúster > Steps (los resultados quedan en stdout de cada step)"
    ;;

  terminar)
    ID="${2:?Indique el id del clúster (j-XXXX)}"
    aws emr terminate-clusters --region "$REGION" --cluster-ids "$ID"
    echo "Clúster $ID terminándose."
    ;;

  *)
    echo "Uso: bash emr_ejecutar.sh crear | pasos <cluster-id> | terminar <cluster-id>"; exit 1 ;;
esac
