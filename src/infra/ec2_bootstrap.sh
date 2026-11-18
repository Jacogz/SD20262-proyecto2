#!/usr/bin/env bash
# Se ejecuta DENTRO de la EC2 (Amazon Linux 2023) como root. Instala dependencias y
# programa la ingesta diaria con un systemd timer.
set -euo pipefail
APP=/opt/ingesta

PY=python3.12
dnf install -y -q $PY $PY-pip || { PY=python3.11; dnf install -y -q $PY $PY-pip; }

sudo -u ec2-user $PY -m venv $APP/.venv
sudo -u ec2-user $APP/.venv/bin/pip install -q --upgrade pip
sudo -u ec2-user $APP/.venv/bin/pip install -q -r $APP/requirements-ingesta.txt

cat > /etc/systemd/system/ingesta-datalake.service <<UNIT
[Unit]
Description=Ingesta de fuentes (RDS, URLs, archivos EC2) al data lake S3
Wants=network-online.target
After=network-online.target

[Service]
Type=oneshot
User=ec2-user
WorkingDirectory=$APP
ExecStart=$APP/.venv/bin/python $APP/ingest_to_s3.py
UNIT

# Todos los días a las 2:00 a.m. hora Colombia. Persistent=true: si la instancia
# estaba apagada a esa hora, corre apenas vuelva a encender.
cat > /etc/systemd/system/ingesta-datalake.timer <<UNIT
[Unit]
Description=Programación diaria de la ingesta al data lake

[Timer]
OnCalendar=*-*-* 02:00:00 America/Bogota
Persistent=true

[Install]
WantedBy=timers.target
UNIT

# Accesos directos en el home para encontrar todo al entrar por SSH
ln -sfn /data/fuentes /home/ec2-user/fuentes
ln -sfn $APP /home/ec2-user/ingesta
chown -h ec2-user: /home/ec2-user/fuentes /home/ec2-user/ingesta

systemctl daemon-reload
systemctl enable --now ingesta-datalake.timer
echo "Timer activo:"; systemctl list-timers ingesta-datalake.timer --no-pager

echo "Ejecutando una ingesta completa ahora..."
systemctl start ingesta-datalake.service || true
journalctl -u ingesta-datalake.service -n 60 --no-pager
