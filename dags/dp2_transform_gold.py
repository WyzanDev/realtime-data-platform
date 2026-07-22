"""
DP2 — Luồng biến đổi Bronze → Silver/Gold (mô hình chiều/sự kiện SCD2).

Luồng gồm hai giai đoạn, thể hiện bằng hai nhóm tác vụ trên Airflow UI:

    ┌─────────────────────────────────────────────────────────┐
    │  INGEST STAGE                                            │
    │   build_lakehouse_gold : Spark dựng Silver + Gold dạng   │
    │                          Delta trên MinIO (đăng ký vào   │
    │                          Hive Metastore cho Trino) và    │
    │                          mirror Gold sang PostgreSQL     │
    │   add_pg_constraints   : gắn khoá chính/ngoại cho bản    │
    │                          Postgres để DBeaver vẽ ERD      │
    │   refresh_trino        : làm mới cache Trino             │
    └───────────────────────────┬─────────────────────────────┘
                                 ▼
    ┌─────────────────────────────────────────────────────────┐
    │  VALIDATE STAGE                                          │
    │   validate_gold : số dòng, bất biến SCD2 (một phiên bản  │
    │                   hiện hành mỗi khoá), đủ cột SCD2        │
    └─────────────────────────────────────────────────────────┘

Kiến trúc lưu trữ (phương án lakehouse + mirror):
  - Nguồn sự thật Silver/Gold là Delta trên MinIO, truy vấn qua Trino.
  - Bản sao Gold trong PostgreSQL chỉ để có khoá ngoại thật cho sơ đồ DBeaver.

Spark chạy qua `docker exec` (như DP2 dùng lại hạ tầng của Phase 4). Thông
tin đăng nhập MinIO và Postgres lấy từ Airflow Connection bằng Jinja, không
viết cứng.
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

from dp2.validate import add_constraints, reset_postgres_gold, validate_gold  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# Lệnh docker exec chạy Spark job dựng Gold. Truyền đủ biến nss_wrapper cho
# Spark đăng nhập, khoá MinIO và Postgres lấy từ Connection, cùng hai gói
# Delta và trình điều khiển PostgreSQL.
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
    "/opt/spark-jobs/dp2_build_gold.py"
)

# Làm mới cache metadata của Trino cho mọi bảng Gold/Silver sau khi Spark ghi
# đè, để truy vấn không trỏ vào tệp cũ.
_REFRESH_TRINO_CMD = (
    "for t in dim_customer dim_restaurant dim_driver dim_menu_item "
    "fact_orders fact_delivery_events; do "
    "docker exec trino trino --execute "
    "\"CALL delta.system.flush_metadata_cache(schema_name => 'gold', table_name => '$t')\" "
    "|| true; done"
)

with DAG(
    dag_id="dp2_transform_gold",
    description="Bronze → Silver/Gold (SCD2) trên lakehouse Delta + mirror Postgres",
    default_args=DEFAULT_ARGS,
    schedule="0 4 * * *",  # sau DP1 (2h) và Spark batch (3h)
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dp2", "silver", "gold", "scd2", "lakehouse"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    with TaskGroup(group_id="ingest_stage", tooltip="Dựng Silver/Gold và hoàn thiện") as ingest_stage:
        prepare_pg = PythonOperator(
            task_id="prepare_postgres",
            python_callable=reset_postgres_gold,
            doc_md="Tạo lại schema gold sạch để Spark mirror ghi đè không vướng khoá ngoại cũ.",
        )
        build = BashOperator(
            task_id="build_lakehouse_gold",
            bash_command=_BUILD_CMD,
            doc_md="Spark dựng Silver + Gold (Delta trên MinIO) và mirror Gold sang Postgres.",
        )
        constraints = PythonOperator(
            task_id="add_pg_constraints",
            python_callable=add_constraints,
            doc_md="Gắn khoá chính và khoá ngoại cho bản Postgres để DBeaver vẽ ERD.",
        )
        refresh = BashOperator(
            task_id="refresh_trino",
            bash_command=_REFRESH_TRINO_CMD,
            doc_md="Làm mới cache Trino cho các bảng vừa ghi đè.",
        )
        prepare_pg >> build >> [constraints, refresh]

    with TaskGroup(group_id="validate_stage", tooltip="Kiểm tra mô hình Gold") as validate_stage:
        PythonOperator(
            task_id="validate_gold",
            python_callable=validate_gold,
            doc_md="Số dòng, bất biến SCD2 (một phiên bản hiện hành mỗi khoá), đủ cột SCD2.",
        )

    end = EmptyOperator(task_id="end")

    start >> ingest_stage >> validate_stage >> end
