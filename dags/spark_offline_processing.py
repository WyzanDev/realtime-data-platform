"""
Luồng xử lý dữ liệu offline bằng Spark (Phase 4).

Luồng này chạy các Spark job đã tối ưu trên bộ dữ liệu Bronze, mỗi job xử
lý một vấn đề dữ liệu offline đã cài sẵn ở khâu sinh dữ liệu:

    ┌──────────────────────────────────────────────────────────┐
    │  start                                                    │
    │    ▼                                                      │
    │  dedup_orders        khử 2% đơn trùng (row_number)        │
    │    ▼                                                      │
    │  process_skew        salting theo delivery_city          │
    │    ▼                                                      │
    │  process_cardinality approx_count_distinct + broadcast    │
    │    ▼                                                      │
    │  resolve_schema      mergeSchema + xử lý null spice_level │
    │    ▼                                                      │
    │  end                                                      │
    └──────────────────────────────────────────────────────────┘

Cơ chế gọi Spark
----------------
Airflow không tự chạy spark-submit mà ra lệnh cho container spark-master
chạy hộ, qua `docker exec`. Cách này tận dụng luôn image Spark đã có sẵn
các gói kết nối MinIO (hadoop-aws, aws-java-sdk), không phải nhồi Spark vào
image Airflow.

Thông tin đăng nhập MinIO **không viết cứng**: lấy từ Airflow Connection
`minio_s3` ngay trong lệnh bằng biểu thức Jinja `{{ conn.minio_s3.* }}`,
đồng nhất với nguyên tắc đã áp dụng ở DP1.

Ba biến môi trường nss_wrapper là bắt buộc: container Spark chạy bằng UID
không có tên trong /etc/passwd, thiếu chúng thì Hadoop báo lỗi đăng nhập.
Chúng do entrypoint của image đặt lúc chạy, nên `docker exec` phải truyền
lại tường minh.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

# Phần đầu lệnh docker exec dùng chung cho mọi job: các biến môi trường để
# Spark đăng nhập được và kết nối MinIO. Tách ra hằng số để bốn task không
# lặp lại một khối dài giống hệt nhau.
_DOCKER_EXEC_ENV = (
    "docker exec "
    "-e HOME=/tmp "
    "-e LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so "
    "-e NSS_WRAPPER_PASSWD=/opt/bitnami/spark/tmp/nss_passwd "
    "-e NSS_WRAPPER_GROUP=/opt/bitnami/spark/tmp/nss_group "
    "-e MINIO_ENDPOINT='{{ conn.minio_s3.extra_dejson.endpoint_url }}' "
    "-e MINIO_ACCESS_KEY='{{ conn.minio_s3.login }}' "
    "-e MINIO_SECRET_KEY='{{ conn.minio_s3.password }}' "
    "spark-master spark-submit "
    "--master spark://spark-master:7077 "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
    "--conf spark.executor.memory=2g "
)


def _spark_command(job_file: str, *args: str) -> str:
    """Ghép lệnh docker exec đầy đủ để chạy một Spark job.

    Args:
        job_file: tên tệp job trong /opt/spark-jobs.
        args: tham số dòng lệnh truyền cho job.

    Returns:
        Chuỗi lệnh bash hoàn chỉnh.
    """
    tail = f"/opt/spark-jobs/{job_file} " + " ".join(args)
    return _DOCKER_EXEC_ENV + tail


with DAG(
    dag_id="spark_offline_processing",
    description="Chạy các Spark job đã tối ưu xử lý vấn đề dữ liệu offline",
    default_args=DEFAULT_ARGS,
    schedule="0 3 * * *",  # sau DP1 (2 giờ sáng) một tiếng
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1,
    tags=["phase4", "spark", "batch", "offline"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    dedup_orders = BashOperator(
        task_id="dedup_orders",
        bash_command=_spark_command("job_dedup.py", "--mode", "dedup"),
        doc_md="Khử 2% đơn trùng bằng row_number theo ingested_at giảm dần.",
    )

    process_skew = BashOperator(
        task_id="process_skew",
        bash_command=_spark_command("job_skew.py", "--mode", "salted", "--salt", "24"),
        doc_md="Tổng hợp doanh thu theo thành phố, salting để tránh straggler do skew.",
    )

    process_cardinality = BashOperator(
        task_id="process_cardinality",
        bash_command=_spark_command("job_cardinality.py", "--mode", "optimized"),
        doc_md="approx_count_distinct cho cột lực lượng lớn + broadcast join bảng danh mục.",
    )

    resolve_schema = BashOperator(
        task_id="resolve_schema",
        bash_command=_spark_command("job_schema_evolution.py", "--mode", "merged"),
        doc_md="mergeSchema hợp nhất lược đồ, phân loại và xử lý null spice_level.",
    )

    end = EmptyOperator(task_id="end")

    # Chạy tuần tự: cụm Spark chỉ có 2 nhân, chạy song song sẽ giành tài
    # nguyên và không job nào nhanh hơn. Tuần tự cho thời gian dễ đọc và
    # nhật ký sạch.
    start >> dedup_orders >> process_skew >> process_cardinality >> resolve_schema >> end
