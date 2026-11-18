#!/usr/bin/env bash
# Lanza la EC2 de ingesta en la misma VPC que el RDS y le abre el puerto 3306 del RDS.
# Idempotente: si ya hay una instancia con el mismo Name corriendo, la reutiliza.
source "$(dirname "$0")/common.sh"

read -r RDS_VPC RDS_SG <<<"$(aws rds describe-db-instances --db-instance-identifier "$RDS_INSTANCE_ID" \
  --query 'DBInstances[0].[DBSubnetGroup.VpcId, VpcSecurityGroups[0].VpcSecurityGroupId]' --output text)"
echo "RDS $RDS_INSTANCE_ID -> VPC $RDS_VPC, SG $RDS_SG"

# Security group de la EC2: SSH solo desde mi IP
SG_NAME="$EC2_NAME-sg"
EC2_SG="$(aws ec2 describe-security-groups --filters Name=group-name,Values="$SG_NAME" Name=vpc-id,Values="$RDS_VPC" \
  --query 'SecurityGroups[0].GroupId' --output text)"
if [[ "$EC2_SG" == "None" ]]; then
  EC2_SG="$(aws ec2 create-security-group --group-name "$SG_NAME" --vpc-id "$RDS_VPC" \
    --description "EC2 de ingesta al data lake" --query GroupId --output text)"
fi
MY_IP="$(curl -s https://checkip.amazonaws.com)"
aws ec2 authorize-security-group-ingress --group-id "$EC2_SG" --protocol tcp --port 22 --cidr "$MY_IP/32" >/dev/null 2>&1 \
  && echo "  SSH abierto para $MY_IP" || echo "  regla SSH para $MY_IP ya existía"

# El RDS acepta MariaDB (3306) desde la EC2
aws ec2 authorize-security-group-ingress --group-id "$RDS_SG" --protocol tcp --port 3306 --source-group "$EC2_SG" >/dev/null 2>&1 \
  && echo "  RDS:3306 abierto para $EC2_SG" || echo "  regla RDS:3306 ya existía"

# Key pair
mkdir -p "$INFRA_DIR/keys"
if ! aws ec2 describe-key-pairs --key-names "$EC2_KEY_NAME" >/dev/null 2>&1; then
  aws ec2 create-key-pair --key-name "$EC2_KEY_NAME" --query KeyMaterial --output text > "$KEY_FILE"
  chmod 600 "$KEY_FILE"
  echo "  llave creada en $KEY_FILE"
fi
[[ -f "$KEY_FILE" ]] || { echo "La llave $EC2_KEY_NAME existe en AWS pero no en $KEY_FILE"; exit 1; }

# Rol IAM de la EC2: solo puede escribir raw/ y leer/listar el bucket del data lake
if [[ -z "${EC2_INSTANCE_PROFILE:-}" ]]; then
  ROLE="$EC2_NAME-role"
  EC2_INSTANCE_PROFILE="$EC2_NAME-profile"
  if ! aws iam get-role --role-name "$ROLE" >/dev/null 2>&1; then
    aws iam create-role --role-name "$ROLE" --assume-role-policy-document \
      '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}' >/dev/null
    echo "  rol creado: $ROLE"
  fi
  aws iam put-role-policy --role-name "$ROLE" --policy-name datalake-raw-writer --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
      {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::'"$DATALAKE_BUCKET"'"},
      {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"], "Resource": "arn:aws:s3:::'"$DATALAKE_BUCKET"'/raw/*"}
    ]}'
  # Permite administrar la instancia desde la consola con Session Manager (sin SSH)
  aws iam attach-role-policy --role-name "$ROLE" --policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore
  if ! aws iam get-instance-profile --instance-profile-name "$EC2_INSTANCE_PROFILE" >/dev/null 2>&1; then
    aws iam create-instance-profile --instance-profile-name "$EC2_INSTANCE_PROFILE" >/dev/null
    aws iam add-role-to-instance-profile --instance-profile-name "$EC2_INSTANCE_PROFILE" --role-name "$ROLE"
    echo "  instance profile creado: $EC2_INSTANCE_PROFILE (esperando propagación de IAM...)"
    sleep 15
  fi
fi

INSTANCE_ID="$(aws ec2 describe-instances \
  --filters Name=tag:Name,Values="$EC2_NAME" Name=instance-state-name,Values=pending,running,stopped \
  --query 'Reservations[0].Instances[0].InstanceId' --output text)"

if [[ "$INSTANCE_ID" == "None" ]]; then
  AMI="$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
    --query Parameter.Value --output text)"
  SUBNET="$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$RDS_VPC" Name=map-public-ip-on-launch,Values=true \
    --query 'Subnets[0].SubnetId' --output text)"
  INSTANCE_ID="$(aws ec2 run-instances --image-id "$AMI" --instance-type "$EC2_INSTANCE_TYPE" \
    --key-name "$EC2_KEY_NAME" --iam-instance-profile Name="$EC2_INSTANCE_PROFILE" \
    --network-interfaces "DeviceIndex=0,SubnetId=$SUBNET,Groups=$EC2_SG,AssociatePublicIpAddress=true" \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=20,VolumeType=gp3}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$EC2_NAME}]" \
    --query 'Instances[0].InstanceId' --output text)"
  echo "  instancia creada: $INSTANCE_ID"
else
  aws ec2 start-instances --instance-ids "$INSTANCE_ID" >/dev/null
  echo "  reutilizando instancia: $INSTANCE_ID"
fi

aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
PUBLIC_IP="$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" \
  --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"
printf 'INSTANCE_ID=%s\nPUBLIC_IP=%s\n' "$INSTANCE_ID" "$PUBLIC_IP" > "$STATE_FILE"
echo "EC2 lista: $INSTANCE_ID  $PUBLIC_IP"
echo "SSH: ssh -i $KEY_FILE ec2-user@$PUBLIC_IP"
