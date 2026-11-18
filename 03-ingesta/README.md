# Punto 3 · Ingesta automática al data lake (zona raw)

El código está en [`../src/infra/`](../src/infra/) (infraestructura) y [`../src/scripts/ingest_to_s3.py`](../src/scripts/ingest_to_s3.py)
(ingesta). No se copia aquí para no romper sus rutas relativas.

```
RDS MariaDB (sedes_educativas) ─┐
URLs ICFES (data/matches.txt)  ─┼─▶ EC2 "ingesta-datalake" ─ ingest_to_s3.py (systemd timer diario 02:00 Bogotá) ─▶ s3://BUCKET/raw/
Archivos EC2 (/data/fuentes)   ─┘
```

## Resultado en S3

```
s3://BUCKET/raw/
├── rds/sedes_educativas/<tabla>/ingest_date=AAAA-MM-DD/<tabla>.csv   las 7 tablas, una copia por día
├── url/icfes_saber11_agregados/<archivo original>.xlsx              no se vuelve a descargar si ya existe
├── ec2/<ruta dentro de /data/fuentes>                               p. ej. ec2/icfes_saber11_sinteticos/*.xlsx
└── _manifests/ingest_date=AAAA-MM-DD/run_<timestamp>.json           fuentes ok/error, objetos subidos u omitidos, archivos corruptos
```

`01_create_datalake.sh` también crea `trusted/`, `refined/` y `athena-results/`, que usan los puntos 4 a 8.

## Requisitos

- AWS CLI v2 con un perfil configurado (`AWS_PROFILE` en `config.env`), `bash`, `ssh`/`scp`, `curl`.
- La RDS del punto 2 creada y cargada, y `.env` en la raíz del repositorio con `DATABASE_MASTER_ENDPOINT`,
  `DATABASE_MASTER_USER` y `DATABASE_MASTER_PASSWORD` (ver [`../02-fuentes/`](../02-fuentes/README.md)).

## Pasos

```bash
# 0. Configuración (config.env NO se sube a git)
cp src/infra/config.env.example src/infra/config.env
#    editar: AWS_REGION, AWS_PROFILE, DATALAKE_BUCKET (vacío = saber11-datalake-<account_id>),
#            RDS_INSTANCE_ID, EC2_KEY_NAME y TEAM_PRINCIPALS (cuentas de los compañeros, entre comillas)

# 1. Bucket del data lake: bloqueo de acceso público, cifrado AES256, versionado, zonas
#    y política (solo HTTPS + lectura total / escritura en trusted, refined y athena-results para TEAM_PRINCIPALS)
bash src/infra/01_create_datalake.sh

# 2. EC2 de ingesta en la VPC de la RDS: security group con SSH solo desde su IP, regla 3306 en el SG de la RDS,
#    llave en src/infra/keys/ (en .gitignore) y rol IAM que solo puede escribir raw/ (+ Session Manager)
bash src/infra/02_launch_ec2.sh

# 3. Copia ingest_to_s3.py, matches.txt, data/ec2_files/ y un .env remoto (credenciales de BD + bucket);
#    instala Python y dependencias, programa el timer diario y ejecuta una primera ingesta completa
bash src/infra/03_deploy_ec2.sh
```

`01` y `02` son idempotentes; `03` se puede volver a correr para redesplegar (por ejemplo, después de agregar URLs a `matches.txt`).

### Operación en la EC2

```bash
ssh -i src/infra/keys/<EC2_KEY_NAME>.pem ec2-user@<IP>      # IP en src/infra/.ec2_state
systemctl list-timers ingesta-datalake.timer                 # próxima ejecución
sudo systemctl start ingesta-datalake.service                # forzar una ingesta
journalctl -u ingesta-datalake.service -n 100 --no-pager     # log
```

### Ejecución manual (sin EC2)

```bash
pip install -r src/scripts/requirements-ingesta.txt
export DATALAKE_BUCKET=<bucket> FILES_DIR=data/ec2_files    # más DATABASE_MASTER_* en .env para la fuente rds
python src/scripts/ingest_to_s3.py --dry-run                 # muestra qué subiría, sin subir nada
python src/scripts/ingest_to_s3.py --sources urls,files      # solo algunas fuentes
python src/scripts/ingest_to_s3.py --force                   # vuelve a subir aunque ya exista
```

Una fuente caída no detiene las demás; el proceso termina con código 1 si alguna falló, y el manifiesto lo registra.

## Siguiente paso
Con `raw/` poblado se ejecuta la preparación: [`../04-preparacion/`](../04-preparacion/README.md).

## Evidencias sugeridas
1. S3: `raw/` con las carpetas `rds/`, `url/`, `ec2/` y `_manifests/`.
2. Un manifiesto JSON con `"sources": {"rds": "ok", "urls": "ok", "files": "ok"}` y la lista `corrupt_at_source`.
3. EC2: `systemctl list-timers` mostrando `ingesta-datalake.timer` y el `journalctl` de una ejecución.
4. IAM: política `datalake-raw-writer` del rol de la EC2.
