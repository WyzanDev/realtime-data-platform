"""
DP3 — Luồng tính bảng feature offline từ tầng Gold.

Hai giai đoạn trên Airflow UI:

    ┌─────────────────────────────────────────────────────────┐
    │  INGEST STAGE                                            │
    │   build_features : Spark tính 3 bảng feature từ Gold,    │
    │                    ghi Delta trên MinIO + mirror Postgres│
    │   refresh_trino  : làm mới cache Trino                   │
    └───────────────────────────┬─────────────────────────────┘
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │  VALIDATE STAGE                                          │
    │   validate_features : số dòng, đủ hai cột Feast          │
    │                       (event_timestamp, created)         │
    └─────────────────────────────────────────────────────────┘

Ba bảng feature (`feat_*`) dùng cho ML về sau, mỗi bảng có `event_timestamp`
và `created` theo chuẩn Feast. Spark chạy qua `docker exec`, khoá lấy từ
Airflow Connection bằng Jinja — không viết cứng.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, str(Path(__file__).resolve().parent))

from dp3.validate import validate_features  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

_BUILD_CMD = (
    "docker exec "
    "-e HOME=/tmp "
    "-e LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so "
    "-e NSS_WRAPPER_PASSWD=/opt/bitnami/spark/tmp/nss_passwd "
    "-e NSS_WRAPPER_GROUP=/opt/bitnami/spark/tmp/nss_group "
    "-e MINIO_ENDPOINT='{{ conn.minio_s3.extra_dejson.endpoint_url }}' "
    "-e MINIO_ACCESS_KEY='{{ conn.minio_s3.login }}' "
    "-e MINIO_SECRET_KEY='{{ conn.minio_s3.password }}' "
    "-e PG_HOST='{{ conn.postgres_warehouse.host }}' "
    "-e PG_DB='{{ conn.postgres_warehouse.schema }}' "
    "-e PG_USER='{{ conn.postgres_warehouse.login }}' "
    "-e PG_PASSWORD='{{ conn.postgres_warehouse.password }}' "
    "spark-master spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
    "--packages io.delta:delta-spark_2.12:3.2.0,org.postgresql:postgresql:42.7.4 "
    "--conf spark.executor.memory=2g "
    "/opt/spark-jobs/dp3_build_features.py"
)

_REFRESH_TRINO_CMD = (
    "for t in feat_customer_order_freq_7d feat_restaurant_avg_prep_time_30d "
    "feat_driver_acceptance_rate_7d; do "
    "docker exec trino trino --execute "
    "\"CALL delta.system.flush_metadata_cache(schema_name => 'gold', table_name => '$t')\" "
    "|| true; done"
)

with DAG(
    dag_id="dp3_compute_features",
    description="Tính bảng feature offline (feat_*) từ Gold, chuẩn Feast",
    default_args=DEFAULT_ARGS,
    schedule="0 5 * * *",  # sau DP2 (4h)
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dp3", "feature", "feast"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="ingest_stage", tooltip="Tính và ghi bảng feature") as ingest_stage:
        build = BashOperator(
            task_id="build_features",
            bash_command=_BUILD_CMD,
            doc_md="Spark tính 3 bảng feature từ Gold, ghi Delta + mirror Postgres.",
        )
        refresh = BashOperator(
            task_id="refresh_trino",
            bash_command=_REFRESH_TRINO_CMD,
            doc_md="Làm mới cache Trino cho các bảng feature vừa ghi.",
        )
        build >> refresh

    with TaskGroup(group_id="validate_stage", tooltip="Kiểm tra bảng feature") as validate_stage:
        PythonOperator(
            task_id="validate_features",
            python_callable=validate_features,
            doc_md="Số dòng, đủ hai cột Feast (event_timestamp, created), khoá không rỗng.",
        )

    end = EmptyOperator(task_id="end")

    start >> ingest_stage >> validate_stage >> end
