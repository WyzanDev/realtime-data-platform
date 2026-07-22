"""
Tiện ích dùng chung cho các Spark job xử lý dữ liệu offline (Phase 4).

Module gom ba việc mọi job đều cần:

  1. Tạo SparkSession đã cấu hình sẵn kết nối tới MinIO qua giao thức S3A.
  2. Đọc bảng Bronze thành DataFrame.
  3. Đo và in thời gian chạy của từng bước để so sánh baseline với bản
     tối ưu.

Nguyên tắc về thông tin đăng nhập: **không viết cứng trong mã**. Endpoint
và khoá truy cập MinIO đều đọc từ biến môi trường, vốn được Airflow truyền
vào lúc chạy (lấy từ Connection `minio_s3`) hoặc do người chạy tay export
trước khi gọi spark-submit. Nhờ vậy mã nguồn đẩy lên kho không chứa bí mật.

Vì sao cấu hình S3A đặt ở đây mà không phải trong spark-defaults: đặt trong
mã giữ cho mọi job dùng đúng một bộ cấu hình, và khi đọc job là thấy ngay
nó nối tới đâu, không phải lần theo tệp cấu hình bên ngoài image.
"""

from __future__ import annotations

import os
import time
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession

# Tên bucket theo tầng của hồ dữ liệu. Bronze do DP1 (Phase 3) ghi ra,
# analytics là nơi các job Phase 4 ghi kết quả đã xử lý.
BUCKET_BRONZE = "bronze"
BUCKET_ANALYTICS = "analytics"


def _env(name: str, default: str | None = None) -> str:
    """Đọc một biến môi trường bắt buộc, báo lỗi rõ ràng nếu thiếu.

    Args:
        name: tên biến môi trường.
        default: giá trị mặc định; nếu None thì biến là bắt buộc.

    Returns:
        Giá trị của biến môi trường.

    Raises:
        RuntimeError: khi biến bắt buộc không được đặt.
    """
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(
            f"Thiếu biến môi trường bắt buộc '{name}'. Job cần thông tin này "
            f"để kết nối MinIO. Airflow truyền vào qua Connection minio_s3; "
            f"nếu chạy tay hãy export trước khi gọi spark-submit."
        )
    return value


def build_spark(app_name: str, extra_conf: dict[str, str] | None = None) -> SparkSession:
    """Tạo SparkSession đã cấu hình kết nối MinIO qua S3A.

    Các tham số S3A giải thích ngắn:
      - path.style.access=true: MinIO dùng đường dẫn kiểu `host/bucket`
        thay vì kiểu `bucket.host` của AWS thật.
      - connection.ssl.enabled=false: MinIO nội bộ chạy HTTP, không TLS.
      - SimpleAWSCredentialsProvider: dùng thẳng cặp khoá access/secret,
        không đi tìm khoá từ hồ sơ IAM như trên AWS.

    Args:
        app_name: tên ứng dụng, hiện trên Spark UI để dễ nhận ra job.
        extra_conf: cấu hình bổ sung, ghi đè mặc định nếu trùng khoá.

    Returns:
        SparkSession đã sẵn sàng đọc ghi trên MinIO.
    """
    endpoint = _env("MINIO_ENDPOINT", "http://minio:9000")
    access_key = _env("MINIO_ACCESS_KEY")
    secret_key = _env("MINIO_SECRET_KEY")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # Ivy phải trỏ tới thư mục ghi được: container Spark chạy bằng UID
        # không có thư mục nhà, mặc định của Ivy sẽ hỏng.
        .config("spark.jars.ivy", "/tmp/.ivy2")
        # Ghi nhật ký sự kiện để Spark History Server dựng lại giao diện của
        # job đã chạy xong. Đây là nguồn ảnh chụp bằng chứng cho Phase 4:
        # phân bố thời gian task ở baseline (thấy straggler do skew) và sau
        # khi tối ưu. Không bật thì job ngắn chạy xong là UI biến mất.
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", "file:/opt/spark-events")
    )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    # Chỉ in cảnh báo trở lên, để nhật ký job không bị lấp bởi log INFO.
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark: SparkSession, table: str, merge_schema: bool = False) -> DataFrame:
    """Đọc một bảng Bronze từ MinIO thành DataFrame.

    Args:
        spark: phiên Spark đang chạy.
        table: tên bảng Bronze, ví dụ `raw_orders`.
        merge_schema: có hợp nhất lược đồ giữa các file phân vùng không.
            Chỉ bật cho bảng có schema evolution như `raw_menu_items`;
            bật vô cớ sẽ làm chậm vì Spark phải đọc footer mọi file.

    Returns:
        DataFrame nội dung bảng.
    """
    reader = spark.read
    if merge_schema:
        reader = reader.option("mergeSchema", "true")
    return reader.parquet(f"s3a://{BUCKET_BRONZE}/{table}/")


def write_analytics(df: DataFrame, name: str, mode: str = "overwrite") -> None:
    """Ghi kết quả đã xử lý xuống bucket analytics dưới dạng parquet.

    Args:
        df: dữ liệu cần ghi.
        name: tên thư mục đích trong bucket analytics.
        mode: chế độ ghi, mặc định ghi đè.
    """
    df.write.mode(mode).parquet(f"s3a://{BUCKET_ANALYTICS}/{name}/")


@contextmanager
def timed(label: str):
    """Đo thời gian chạy của một khối lệnh và in ra nhật ký.

    Dùng để đặt cạnh nhau con số baseline và con số sau tối ưu. Thời gian
    in ở đây là thời gian treo (wall-clock) của driver, gồm cả lập lịch
    và chờ shuffle — đúng thứ người dùng cảm nhận.

    Args:
        label: nhãn mô tả khối lệnh đang đo.

    Yields:
        None.
    """
    print(f"\n===== BẮT ĐẦU: {label} =====", flush=True)
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"===== KẾT THÚC: {label} — {elapsed:.1f} giây =====\n", flush=True)
