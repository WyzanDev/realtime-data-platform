"""
Tối ưu lưu trữ tầng kho dữ liệu (DWH) bằng chỉ mục trên PostgreSQL.

PostgreSQL đóng vai kho dữ liệu phân tích trong dự án này. Kịch bản minh
hoạ tác động của **chỉ mục (index)** lên thời gian truy vấn:

  1. Nạp một phần bảng đơn hàng từ tầng Bronze vào bảng `public.dwh_orders`.
  2. Chạy một truy vấn lọc theo `delivery_city` và khoảng `order_time`,
     đo thời gian khi CHƯA có chỉ mục — PostgreSQL phải quét tuần tự cả bảng.
  3. Tạo chỉ mục phức hợp trên `(delivery_city, order_time)`.
  4. Chạy lại đúng truy vấn đó, đo thời gian khi ĐÃ có chỉ mục — PostgreSQL
     nhảy thẳng tới vùng dữ liệu cần qua chỉ mục.

Dùng `EXPLAIN ANALYZE` để lấy cả thời gian thực thi lẫn kiểu quét (Seq Scan
so với Index Scan), làm bằng chứng khách quan.

Không viết cứng thông tin đăng nhập: đọc từ biến môi trường, do người chạy
hoặc Airflow truyền vào.

Cách chạy:
    docker exec -e PG_HOST=postgres -e PG_USER=admin -e PG_PASSWORD=... \\
        -e MINIO_ENDPOINT=http://minio:9000 -e MINIO_ACCESS_KEY=minio \\
        -e MINIO_SECRET_KEY=... airflow-worker \\
        python /opt/airflow/dags/../jobs/dwh/dwh_indexing.py
"""

from __future__ import annotations

import io
import os

import boto3
import pandas as pd
import psycopg2

# Số dòng nạp vào DWH. Đủ lớn để quét tuần tự chậm hơn quét chỉ mục rõ rệt,
# nhưng vẫn vừa bộ nhớ khi nạp qua pandas.
N_ROWS = 800_000

TABLE = "public.dwh_orders"

# Truy vấn đối chứng: lọc theo đúng hai cột sẽ đánh chỉ mục, chọn một cửa sổ
# thời gian hẹp (khung trưa một ngày) để truy vấn có tính chọn lọc cao — khi
# đó chỉ mục thể hiện lợi thế rõ nhất so với quét tuần tự toàn bảng.
FILTER_CITY = "Ho Chi Minh"
FILTER_FROM = "2026-02-10 11:00:00"
FILTER_TO = "2026-02-10 13:00:00"


def _s3_client():
    """Tạo client MinIO từ biến môi trường."""
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
    )


def _load_orders_subset() -> pd.DataFrame:
    """Đọc một phần bảng raw_orders từ Bronze cho tới khi đủ N_ROWS.

    Returns:
        DataFrame các cột cần cho truy vấn đối chứng.
    """
    client = _s3_client()
    cols = ["order_id", "delivery_city", "restaurant_id", "total_amount", "order_time", "status"]
    frames: list[pd.DataFrame] = []
    total = 0

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="bronze", Prefix="raw_orders/"):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".parquet"):
                continue
            body = client.get_object(Bucket="bronze", Key=obj["Key"])["Body"].read()
            df = pd.read_parquet(io.BytesIO(body), columns=cols)
            frames.append(df)
            total += len(df)
            if total >= N_ROWS:
                break
        if total >= N_ROWS:
            break

    return pd.concat(frames, ignore_index=True).head(N_ROWS)


def _pg_conn():
    """Kết nối PostgreSQL kho dữ liệu từ biến môi trường."""
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "postgres"),
        port=int(os.environ.get("PG_PORT", "5432")),
        dbname=os.environ.get("PG_DB", "warehouse"),
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
    )


def _load_to_postgres(df: pd.DataFrame, conn) -> None:
    """Tạo lại bảng DWH và nạp dữ liệu bằng COPY cho nhanh.

    Args:
        df: dữ liệu cần nạp.
        conn: kết nối PostgreSQL.
    """
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(
            f"""
            CREATE TABLE {TABLE} (
                order_id      TEXT,
                delivery_city TEXT,
                restaurant_id TEXT,
                total_amount  BIGINT,
                order_time    TIMESTAMP,
                status        TEXT
            )
            """
        )
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)
        cur.copy_expert(f"COPY {TABLE} FROM STDIN WITH CSV", buf)
    conn.commit()


def _explain_analyze(conn, label: str) -> None:
    """Chạy EXPLAIN ANALYZE truy vấn đối chứng và in kiểu quét + thời gian.

    Args:
        conn: kết nối PostgreSQL.
        label: nhãn phân biệt trước/sau khi đánh chỉ mục.
    """
    query = f"""
        SELECT count(*), sum(total_amount)
        FROM {TABLE}
        WHERE delivery_city = %s AND order_time BETWEEN %s AND %s
    """
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, TIMING ON) " + query, (FILTER_CITY, FILTER_FROM, FILTER_TO))
        plan = [r[0] for r in cur.fetchall()]

    scan = next((l.strip() for l in plan if "Scan" in l), "?")
    exec_time = next((l for l in plan if "Execution Time" in l), "?")
    print(f"\n----- {label} -----", flush=True)
    print(f"  Kiểu quét : {scan}", flush=True)
    print(f"  {exec_time.strip()}", flush=True)


def main() -> None:
    """Nạp DWH, đo truy vấn chưa index, tạo index, đo lại."""
    print(f"Đang đọc ~{N_ROWS:,} đơn từ Bronze...", flush=True)
    df = _load_orders_subset()
    print(f"Đã đọc {len(df):,} dòng.", flush=True)

    conn = _pg_conn()
    try:
        _load_to_postgres(df, conn)
        print(f"Đã nạp vào {TABLE}.", flush=True)

        # Buộc PostgreSQL cập nhật thống kê để kế hoạch truy vấn chính xác.
        with conn.cursor() as cur:
            cur.execute(f"ANALYZE {TABLE}")
        conn.commit()

        _explain_analyze(conn, "TRƯỚC (chưa có chỉ mục)")

        with conn.cursor() as cur:
            cur.execute(
                f"CREATE INDEX idx_dwh_orders_city_time ON {TABLE} (delivery_city, order_time)"
            )
            cur.execute(f"ANALYZE {TABLE}")
        conn.commit()
        print("\nĐã tạo chỉ mục (delivery_city, order_time).", flush=True)

        _explain_analyze(conn, "SAU (đã có chỉ mục)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
