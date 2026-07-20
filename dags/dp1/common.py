"""
Cấu hình và tiện ích dùng chung cho các luồng xử lý dữ liệu.

Module này tập trung ba việc mà mọi DAG đều cần: khai báo bảng nào nằm ở
đâu, tạo kết nối tới MinIO và PostgreSQL từ Airflow Connection, và các
hàm đọc ghi parquet.

Nguyên tắc xuyên suốt: không viết cứng thông tin đăng nhập trong mã. Mọi
kết nối đều lấy qua Airflow Connection, vốn được khai báo bằng biến môi
trường trong tệp .env. Nhờ vậy đổi mật khẩu chỉ cần sửa một chỗ, và mã
nguồn đẩy lên kho lưu trữ không chứa thông tin nhạy cảm.
"""

from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

# --- Tên các Airflow Connection đã khai trong tệp .env ---
CONN_MINIO = "minio_s3"
CONN_POSTGRES = "postgres_warehouse"
CONN_KAFKA = "kafka"

# --- Tên bucket theo từng tầng của kiến trúc hồ dữ liệu ---
BUCKET_RAW = "raw"
BUCKET_BRONZE = "bronze"


@dataclass(frozen=True)
class TableSpec:
    """Mô tả một bảng cần nạp vào tầng Bronze.

    Gom mọi thông tin về một bảng vào một chỗ thay vì rải rác trong mã
    của từng tác vụ. Khi thêm bảng mới chỉ cần thêm một mục vào danh
    sách bên dưới, không phải sửa logic.

    Attributes:
        name: tên bảng ở tầng Bronze, theo quy ước tiền tố raw_.
        source: nguồn dữ liệu, một trong minio, postgres hoặc kafka.
        source_path: đường dẫn hoặc tên bảng ở phía nguồn.
        primary_key: khoá nghiệp vụ, dùng để kiểm tra trùng lặp.
        required_columns: các cột bắt buộc phải có mặt và không được rỗng.
        partition_by: cột dùng để chia phân vùng khi ghi xuống Bronze.
        min_rows: số dòng tối thiểu kỳ vọng, dùng cho bước kiểm tra.
    """

    name: str
    source: str
    source_path: str
    primary_key: str
    required_columns: list[str] = field(default_factory=list)
    partition_by: str | None = "ingested_date"
    min_rows: int = 1


# Danh sách bảng cần nạp. Thứ tự trong danh sách cũng là thứ tự ưu tiên
# khi cần xử lý tuần tự, tuy các tác vụ hiện chạy song song.
BRONZE_TABLES: list[TableSpec] = [
    TableSpec(
        name="raw_orders",
        source="minio",
        source_path="order",
        primary_key="order_id",
        required_columns=["order_id", "customer_id", "restaurant_id", "order_time", "status"],
        min_rows=1000,
    ),
    TableSpec(
        name="raw_order_items",
        source="minio",
        source_path="order_item",
        primary_key="order_item_id",
        required_columns=["order_item_id", "order_id", "menu_item_id", "quantity"],
        min_rows=1000,
    ),
    TableSpec(
        name="raw_menu_items",
        source="minio",
        source_path="menu_item",
        primary_key="menu_item_id",
        required_columns=["menu_item_id", "restaurant_id", "price"],
        min_rows=100,
    ),
    TableSpec(
        name="raw_reviews",
        source="minio",
        source_path="review",
        primary_key="review_id",
        required_columns=["review_id", "order_id", "rating"],
        min_rows=100,
    ),
    TableSpec(
        name="raw_customers",
        source="postgres",
        source_path="source_system.customer",
        primary_key="customer_id",
        required_columns=["customer_id", "city", "segment"],
        min_rows=100,
    ),
    TableSpec(
        name="raw_restaurants",
        source="postgres",
        source_path="source_system.restaurant",
        primary_key="restaurant_id",
        required_columns=["restaurant_id", "city", "category"],
        min_rows=100,
    ),
    TableSpec(
        name="raw_drivers",
        source="postgres",
        source_path="source_system.driver",
        primary_key="driver_id",
        required_columns=["driver_id", "vehicle_type", "city"],
        min_rows=100,
    ),
    TableSpec(
        name="raw_delivery_events",
        source="kafka",
        source_path="gps-topic",
        primary_key="event_id",
        required_columns=["event_id", "order_id", "event_type", "event_time", "sent_at"],
        min_rows=100,
    ),
]


def get_s3_client():
    """Tạo đối tượng kết nối tới MinIO qua giao thức tương thích S3.

    Thông tin đăng nhập lấy từ Airflow Connection có định danh minio_s3,
    vốn được khai bằng biến môi trường AIRFLOW_CONN_MINIO_S3 trong tệp
    .env. Không có giá trị nào được viết cứng ở đây.

    Returns:
        Đối tượng client của boto3 đã sẵn sàng gọi các thao tác trên S3.
    """
    from airflow.hooks.base import BaseHook
    import boto3

    conn = BaseHook.get_connection(CONN_MINIO)
    extra = conn.extra_dejson

    return boto3.client(
        "s3",
        endpoint_url=extra.get("endpoint_url"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )


def get_postgres_engine():
    """Tạo engine SQLAlchemy trỏ tới cơ sở dữ liệu kho.

    Returns:
        Đối tượng Engine của SQLAlchemy.
    """
    from airflow.hooks.base import BaseHook
    from sqlalchemy import create_engine

    conn = BaseHook.get_connection(CONN_POSTGRES)
    uri = (
        f"postgresql+psycopg2://{conn.login}:{conn.password}"
        f"@{conn.host}:{conn.port}/{conn.schema}"
    )
    return create_engine(uri)


def get_kafka_servers() -> str:
    """Lấy địa chỉ máy chủ Kafka từ Airflow Connection.

    Returns:
        Chuỗi địa chỉ dạng host:port.
    """
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(CONN_KAFKA)
    return f"{conn.host}:{conn.port}"


def list_s3_objects(client, bucket: str, prefix: str) -> list[str]:
    """Liệt kê toàn bộ khoá đối tượng nằm dưới một tiền tố.

    Dùng cơ chế phân trang vì một tiền tố có thể chứa hàng trăm tệp,
    vượt quá giới hạn một nghìn khoá của mỗi lần gọi.

    Args:
        client: đối tượng client S3.
        bucket: tên bucket.
        prefix: tiền tố đường dẫn cần liệt kê.

    Returns:
        Danh sách khoá đối tượng.
    """
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_parquet_from_s3(client, bucket: str, key: str) -> pd.DataFrame:
    """Đọc một tệp parquet từ object storage vào bộ nhớ.

    Args:
        client: đối tượng client S3.
        bucket: tên bucket.
        key: khoá đối tượng.

    Returns:
        DataFrame chứa nội dung tệp.
    """
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def write_parquet_to_s3(
    client, df: pd.DataFrame, bucket: str, key: str, compression: str = "snappy"
) -> int:
    """Ghi một DataFrame thành tệp parquet trên object storage.

    Ghi qua bộ đệm trong bộ nhớ thay vì tệp tạm trên đĩa, tránh phụ
    thuộc vào quyền ghi của vùng chứa Airflow.

    Args:
        client: đối tượng client S3.
        df: dữ liệu cần ghi.
        bucket: tên bucket đích.
        key: khoá đối tượng đích.
        compression: thuật toán nén.

    Returns:
        Số byte đã ghi.
    """
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression=compression)
    data = buf.getvalue()
    client.put_object(Bucket=bucket, Key=key, Body=data)
    return len(data)


def add_ingestion_metadata(df: pd.DataFrame, source: str, table: str) -> pd.DataFrame:
    """Gắn thêm các cột siêu dữ liệu ghi nhận nguồn gốc của dữ liệu.

    Ba cột này không thuộc dữ liệu nghiệp vụ mà phục vụ việc truy vết:
    khi phát hiện sai lệch ở tầng trên, có thể lần ngược về lô nạp nào,
    từ hệ thống nào, vào lúc nào.

    Đây cũng là phần thông tin dòng dõi dữ liệu sẽ được khai báo cho
    DataHub ở giai đoạn sau.

    Args:
        df: dữ liệu gốc.
        source: tên hệ thống nguồn.
        table: tên bảng ở tầng Bronze.

    Returns:
        DataFrame đã thêm ba cột siêu dữ liệu.
    """
    out = df.copy()
    out["_bronze_ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
    out["_bronze_source"] = source
    out["_bronze_table"] = table
    return out
