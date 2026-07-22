"""
Kết nối và tiện ích dùng chung cho luồng DP2 (Bronze → Silver → Gold).

DP2 biến dữ liệu thô ở tầng Bronze (parquet trên MinIO) thành mô hình
chiều/sự kiện chuẩn trong PostgreSQL — kho dữ liệu mà DBeaver kết nối trực
tiếp để xem lược đồ và quan hệ.

Chiến lược tránh tràn bộ nhớ: không kéo cả 2,5 triệu đơn vào pandas. Thay
vào đó nạp từng tệp parquet của Bronze vào bảng tạm trong PostgreSQL bằng
lệnh COPY (rất nhanh, đọc một tệp một lúc), rồi để chính PostgreSQL làm phần
nặng — khử trùng, dựng chiều SCD2, nối bảng sự kiện — bằng SQL.

Không viết cứng thông tin đăng nhập: mọi kết nối lấy từ Airflow Connection,
đồng nhất với DP1.
"""

from __future__ import annotations

import io
import logging

import pandas as pd

# Tên schema theo tầng trong kho dữ liệu.
SCHEMA_STAGING = "staging"   # bản sao thô của Bronze, phục vụ biến đổi
SCHEMA_SILVER = "silver"     # đã khử trùng, làm sạch (stg_*)
SCHEMA_GOLD = "gold"         # mô hình chiều/sự kiện (dim_*, fact_*)

# Tên Airflow Connection, trùng với DP1 để tái dùng cấu hình.
CONN_MINIO = "minio_s3"
CONN_POSTGRES = "postgres_warehouse"

BUCKET_BRONZE = "bronze"

log = logging.getLogger(__name__)


def get_s3_client():
    """Tạo client MinIO từ Airflow Connection minio_s3.

    Returns:
        Client boto3 đã cấu hình endpoint và khoá.
    """
    from airflow.hooks.base import BaseHook
    import boto3

    conn = BaseHook.get_connection(CONN_MINIO)
    return boto3.client(
        "s3",
        endpoint_url=conn.extra_dejson.get("endpoint_url"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )


def get_pg_conn():
    """Mở kết nối psycopg2 tới kho dữ liệu từ Airflow Connection.

    Dùng psycopg2 trực tiếp (không qua SQLAlchemy) vì cần lệnh COPY tốc độ
    cao và chạy nhiều câu DDL/DML trong cùng một giao dịch.

    Returns:
        Đối tượng kết nối psycopg2.
    """
    from airflow.hooks.base import BaseHook
    import psycopg2

    conn = BaseHook.get_connection(CONN_POSTGRES)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        dbname=conn.schema,
        user=conn.login,
        password=conn.password,
    )


def list_parquet_keys(client, prefix: str) -> list[str]:
    """Liệt kê mọi tệp parquet dưới một tiền tố trong bucket Bronze.

    Args:
        client: client S3.
        prefix: tiền tố đường dẫn, ví dụ `raw_orders/`.

    Returns:
        Danh sách khoá đối tượng kết thúc bằng .parquet.
    """
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET_BRONZE, Prefix=prefix
    ):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys


def read_parquet(client, key: str, columns: list[str] | None = None) -> pd.DataFrame:
    """Đọc một tệp parquet từ Bronze vào DataFrame.

    Args:
        client: client S3.
        key: khoá đối tượng.
        columns: danh sách cột cần đọc; None để đọc hết. Với bảng có schema
            evolution, truyền None rồi tự bù cột thiếu ở tầng trên.

    Returns:
        DataFrame nội dung tệp.
    """
    body = client.get_object(Bucket=BUCKET_BRONZE, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body), columns=columns)


def copy_df_to_table(cur, df: pd.DataFrame, table: str, columns: list[str]) -> int:
    """Nạp một DataFrame vào bảng PostgreSQL bằng COPY.

    Ghi qua bộ đệm CSV trong bộ nhớ rồi COPY, nhanh hơn nhiều so với chèn
    từng dòng. Cột được sắp đúng thứ tự khai báo để khớp bảng đích.

    Args:
        cur: con trỏ psycopg2.
        df: dữ liệu cần nạp.
        table: tên bảng đích, đủ cả schema.
        columns: danh sách cột theo đúng thứ tự của bảng.

    Returns:
        Số dòng đã nạp.
    """
    # Bù cột thiếu bằng None để mọi tệp cùng khuôn, kể cả tệp thuộc lược đồ
    # cũ chưa có cột spice_level.
    out = df.reindex(columns=columns)

    buf = io.StringIO()
    out.to_csv(buf, index=False, header=False)
    buf.seek(0)
    col_list = ", ".join(columns)
    cur.copy_expert(f"COPY {table} ({col_list}) FROM STDIN WITH CSV", buf)
    return len(out)
