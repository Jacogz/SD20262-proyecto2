#!/usr/bin/env bash
# Bootstrap action de EMR: librerías que usa raw_to_trusted.py para leer los Excel del ICFES.
set -euo pipefail
sudo python3 -m pip install --quiet pandas openpyxl xlrd boto3 matplotlib
