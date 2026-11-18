Todo lo que se ponga en esta carpeta se copia a `/data/fuentes/` en la EC2 de ingesta
(fuente "archivos en EC2") y de ahí se sube a `s3://<bucket>/raw/ec2/`, respetando subcarpetas.

Organizar por dataset, p. ej.:
  data/ec2_files/saber11_individual/SB11_20231.csv
