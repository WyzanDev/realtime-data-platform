"""
Đăng ký các bảng Bronze (raw_*) vào Hive Metastore để DBeaver/Trino thấy được.

DP1 chỉ ghi Bronze dưới dạng parquet thô trên MinIO (s3a://bronze/<table>/),
không qua `saveAsTable`, nên các bảng này KHÔNG xuất hiện như bảng trong
metastore — Trino và DBeaver không nhìn thấy chúng. Trong khi Silver/Gold đã
được DP2 đăng ký nên hiện đủ trong catalog `delta`.

Script này đọc từng parquet Bronze rồi ghi lại thành bảng Delta managed trong
schema `bronze` (cùng lakehouse với silver/gold). Sau khi chạy, kết nối DBeaver
tới Trino sẽ thấy đủ ba zone trong một cây: delta.bronze / delta.silver /
delta.gold — phục vụ mục "Visualize tables on all zones" của rubric Schema
design (Phase 10).

Chạy một lần là đủ (Bronze ít đổi). Nếu chạy lại DP1 sinh Bronze mới thì chạy
lại script này để đồng bộ.

Cách chạy: xem lệnh spark-submit kèm --packages Delta trong
docs/StorageOptimization.md (thay tên tệp job bằng register_bronze_tables.py).
"""

from __future__ import annotations

from common import build_spark, read_bronze
from dp2_build_gold import DELTA_CONF, LAKE, _save_delta

# Tám bảng Bronze do DP1 nạp. raw_menu_items cần merge_schema vì có schema
# evolution (cột spice_level chỉ xuất hiện từ phiên bản v2).
BRONZE_TABLES = [
    ("raw_orders", False),
    ("raw_order_items", False),
    ("raw_menu_items", True),
    ("raw_customers", False),
    ("raw_restaurants", False),
    ("raw_drivers", False),
    ("raw_reviews", False),
    ("raw_delivery_events", False),
]


def main() -> None:
    """Tạo schema bronze và đăng ký từng bảng raw_* thành bảng Delta."""
    spark = build_spark("register_bronze_tables", extra_conf=DELTA_CONF)

    # Database trỏ location trên lakehouse, cùng chỗ với silver/gold.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS bronze LOCATION '{LAKE}/bronze'")

    for table, merge_schema in BRONZE_TABLES:
        df = read_bronze(spark, table, merge_schema=merge_schema)
        n = _save_delta(spark, df, "bronze", table)
        print(f"  Đã đăng ký bronze.{table}: {n:,} dòng", flush=True)

    print("\nHoàn tất — Trino nay thấy đủ delta.bronze / silver / gold.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
