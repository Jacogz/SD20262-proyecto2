#!/usr/bin/env bash
# Copia el código, la lista de URLs y los archivos fuente a la EC2, y la deja con la
# ingesta programada (systemd timer). Al final ejecuta una ingesta completa.
source "$(dirname "$0")/common.sh"
source "$STATE_FILE"

SSH_OPTS=(-i "$KEY_FILE" -o StrictHostKeyChecking=accept-new)
HOST="ec2-user@$PUBLIC_IP"
echo "Esperando SSH en $PUBLIC_IP..."
until ssh "${SSH_OPTS[@]}" -o ConnectTimeout=5 "$HOST" true 2>/dev/null; do sleep 5; done

# .env remoto: credenciales de BD (del .env local) + configuración del data lake
REMOTE_ENV="$(mktemp)"; trap 'rm -f "$REMOTE_ENV"' EXIT
grep -E '^DATABASE_MASTER_' "$PROJECT_DIR/.env" > "$REMOTE_ENV"
cat >> "$REMOTE_ENV" <<ENV
DATALAKE_BUCKET=$DATALAKE_BUCKET
AWS_REGION=$AWS_REGION
URLS_FILE=/opt/ingesta/matches.txt
FILES_DIR=/data/fuentes
ENV

ssh "${SSH_OPTS[@]}" "$HOST" 'sudo mkdir -p /opt/ingesta /data/fuentes && sudo chown -R ec2-user: /opt/ingesta /data/fuentes'
scp "${SSH_OPTS[@]}" \
  "$PROJECT_DIR/src/scripts/ingest_to_s3.py" \
  "$PROJECT_DIR/src/scripts/requirements-ingesta.txt" \
  "$PROJECT_DIR/data/matches.txt" \
  "$INFRA_DIR/ec2_bootstrap.sh" \
  "$HOST:/opt/ingesta/"
scp "${SSH_OPTS[@]}" "$REMOTE_ENV" "$HOST:/opt/ingesta/.env"

# Fuente "archivos en EC2": todo lo que haya en data/ec2_files/ queda en /data/fuentes/
if compgen -G "$PROJECT_DIR/data/ec2_files/*" >/dev/null; then
  scp -r "${SSH_OPTS[@]}" "$PROJECT_DIR/data/ec2_files/." "$HOST:/data/fuentes/"
fi

ssh "${SSH_OPTS[@]}" "$HOST" 'chmod 600 /opt/ingesta/.env && sudo bash /opt/ingesta/ec2_bootstrap.sh'
