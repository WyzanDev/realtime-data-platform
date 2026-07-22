#!/bin/bash
# Tải các jar cần cho Hive Metastore (postgres driver + S3A). Chạy một lần
# trước khi `docker compose up hive-metastore`.
set -e
cd "$(dirname "$0")"
python3 -c "import urllib.request as u; u.urlretrieve('https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar','postgresql.jar')"
docker cp spark-master:/opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar ./hadoop-aws-3.3.4.jar
docker cp spark-master:/opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar ./aws-java-sdk-bundle-1.12.262.jar
echo "Đã tải xong jar cho Hive Metastore."
