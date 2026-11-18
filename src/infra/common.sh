# Carga config.env y deriva valores comunes. Se usa con: source common.sh
set -euo pipefail
INFRA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$INFRA_DIR/../.." && pwd)"
[[ -f "$INFRA_DIR/config.env" ]] || { echo "Falta $INFRA_DIR/config.env (copiar config.env.example)"; exit 1; }
set -a; source "$INFRA_DIR/config.env"; set +a
export AWS_DEFAULT_REGION="$AWS_REGION"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
DATALAKE_BUCKET="${DATALAKE_BUCKET:-saber11-datalake-$ACCOUNT_ID}"
STATE_FILE="$INFRA_DIR/.ec2_state"
KEY_FILE="$INFRA_DIR/keys/$EC2_KEY_NAME.pem"
