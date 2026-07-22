"""
Kiểm chứng dữ liệu đã nằm thật sự trên MinIO và PostgreSQL.

Rubric yêu cầu chứng minh dữ liệu sau khi sinh đã được lưu ở một hệ thống
nguồn bên ngoài, sẵn sàng để kéo về tầng Bronze. Script này đọc NGƯỢC lại
từ hai hệ thống đó, không đọc file trên máy, để bằng chứng là thật:

  - MinIO: liệt kê tiền tố, đếm file, tổng dung lượng, đọc thử một file
    parquet và in cấu trúc schema của nó
  - PostgreSQL: đếm dòng từng bảng, in kiểu dữ liệu từng cột

Cách chạy:
    set -a && source .env && set +a
    python -m data_generator.verify_sinks
"""

from __future__ import annotations

import argparse
import os
from collections import defaultdict

import pandas as pd

from data_generator.common.config import load_config


def verify_minio(cfg) -> None:
    """Liệt kê và thống kê các đối tượng đang nằm trên MinIO.

    In ra số file, số partition và dung lượng theo từng bảng, sau đó đọc
    thử một file parquet để xác nhận nội dung đọc được chứ không chỉ tồn
    tại cái tên.

    Args:
        cfg: cấu hình đã nạp.
    """
    import boto3

    endpoint = os.environ.get("MINIO_ENDPOINT", cfg.get("sinks.minio.endpoint"))
    bucket = cfg.get("sinks.minio.bucket")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )

    paginator = client.get_paginator("list_objects_v2")
    per_table: dict[str, dict] = defaultdict(
        lambda: {"files": 0, "bytes": 0, "partitions": set()}
    )

    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            if len(parts) < 2:
                continue
            table = parts[0]
            per_table[table]["files"] += 1
            per_table[table]["bytes"] += obj["Size"]
            if parts[1].startswith("ingested_date="):
                per_table[table]["partitions"].add(parts[1])

    rows = [
        {
            "table": t,
            "files": v["files"],
            "partitions": len(v["partitions"]),
            "size_mb": round(v["bytes"] / 1024 / 1024, 2),
        }
        for t, v in sorted(per_table.items())
    ]

    print(f"\n{'=' * 70}\nMINIO — bucket '{bucket}' tại {endpoint}\n{'=' * 70}")
    print(pd.DataFrame(rows).to_string(index=False))
    total_mb = sum(r["size_mb"] for r in rows)
    total_files = sum(r["files"] for r in rows)
    print(f"\nTổng dung lượng: {total_mb:,.2f} MB trên {total_files:,} file")

    # Đọc thử một file để xác nhận nội dung đọc được, không chỉ tồn tại tên.
    sample_key = None
    for page in paginator.paginate(Bucket=bucket, Prefix="menu_item/"):
        for obj in page.get("Contents", []):
            sample_key = obj["Key"]
            break
        if sample_key:
            break

    if sample_key:
        import io

        import pyarrow.parquet as pq

        body = client.get_object(Bucket=bucket, Key=sample_key)["Body"].read()
        schema = pq.read_schema(io.BytesIO(body))
        print(f"\nĐọc thử file: {sample_key}")
        print(f"  Số cột: {len(schema.names)}")
        print(f"  Các cột: {', '.join(schema.names)}")


def verify_postgres(cfg) -> None:
    """Đếm dòng và in cấu trúc các bảng danh mục trong PostgreSQL.

    Args:
        cfg: cấu hình đã nạp.
    """
    from sqlalchemy import create_engine, text

    host = os.environ.get("POSTGRES_HOST", cfg.get("sinks.postgres.host"))
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = cfg.get("sinks.postgres.database")
    schema = cfg.get("sinks.postgres.schema")
    user = os.environ["POSTGRES_USER"]
    pwd = os.environ["POSTGRES_PASSWORD"]

    engine = create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")

    print(f"\n{'=' * 70}\nPOSTGRESQL — {db}.{schema} tại {host}:{port}\n{'=' * 70}")

    with engine.connect() as conn:
        tables = (
            conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = :s ORDER BY table_name"
                ),
                {"s": schema},
            )
            .scalars()
            .all()
        )

        rows = []
        for t in tables:
            n = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{t}")).scalar()
            n_col = conn.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.columns "
                    "WHERE table_schema = :s AND table_name = :t"
                ),
                {"s": schema, "t": t},
            ).scalar()
            rows.append({"table": t, "rows": n, "columns": n_col})

        print(pd.DataFrame(rows).to_string(index=False))

        # In chi tiết cấu trúc một bảng làm ví dụ.
        if tables:
            cols = conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position"
                ),
                {"s": schema, "t": tables[0]},
            ).all()
            print(f"\nCấu trúc bảng '{tables[0]}':")
            for name, dtype in cols:
                print(f"  {name:<22} {dtype}")


def main() -> None:
    """Phân tích tham số dòng lệnh và chạy hai bước kiểm chứng."""
    parser = argparse.ArgumentParser(
        description="Kiểm chứng dữ liệu trên MinIO và PostgreSQL"
    )
    parser.add_argument("--config", default=None)
    parser.add_argument("--skip-minio", action="store_true", help="bỏ qua phần MinIO")
    parser.add_argument("--skip-postgres", action="store_true", help="bỏ qua phần PostgreSQL")
    args = parser.parse_args()

    cfg = load_config(args.config)

    if not args.skip_minio:
        verify_minio(cfg)
    if not args.skip_postgres:
        verify_postgres(cfg)


if __name__ == "__main__":
    main()
