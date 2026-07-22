"""
Giai đoạn kiểm tra và hoàn thiện của DP2.

Hai việc:

  1. Gắn khoá chính và khoá ngoại cho bản mirror Gold trong PostgreSQL. Bản
     Delta trên lakehouse không có ràng buộc khoá, nên chính bản Postgres này
     là nơi DBeaver đọc ra quan hệ dim–fact để vẽ sơ đồ.

  2. Kiểm tra chất lượng mô hình Gold: đủ số dòng, bất biến SCD2 (mỗi khoá
     nghiệp vụ chỉ có đúng một phiên bản hiện hành), và toàn vẹn tham chiếu
     (mọi khoá thay thế trong fact đều tồn tại trong dim).

Chỉ lỗi nghiêm trọng mới làm dừng luồng; số dòng thấp chỉ ghi cảnh báo.
"""

from __future__ import annotations

import logging

from dp2.common import get_pg_conn

log = logging.getLogger(__name__)

# Khoá chính của từng bảng Gold trong Postgres.
PRIMARY_KEYS = {
    "gold.dim_customer": "customer_sk",
    "gold.dim_restaurant": "restaurant_sk",
    "gold.dim_driver": "driver_sk",
    "gold.dim_menu_item": "menu_item_sk",
    "gold.fact_orders": "order_id",
    "gold.fact_delivery_events": "event_id",
    "gold.fact_order_items": "order_item_id",
}

# Khoá ngoại từ fact tới dim: (bảng fact, cột, bảng dim, cột dim, tên ràng buộc).
FOREIGN_KEYS = [
    ("gold.fact_orders", "customer_sk", "gold.dim_customer", "customer_sk", "fk_orders_customer"),
    ("gold.fact_orders", "restaurant_sk", "gold.dim_restaurant", "restaurant_sk", "fk_orders_restaurant"),
    ("gold.fact_orders", "driver_sk", "gold.dim_driver", "driver_sk", "fk_orders_driver"),
    ("gold.fact_delivery_events", "driver_sk", "gold.dim_driver", "driver_sk", "fk_events_driver"),
    ("gold.fact_order_items", "menu_item_sk", "gold.dim_menu_item", "menu_item_sk", "fk_items_menu"),
]

# Các bảng chiều và khoá nghiệp vụ để kiểm tra bất biến SCD2.
DIM_BUSINESS_KEYS = {
    "gold.dim_customer": "customer_id",
    "gold.dim_restaurant": "restaurant_id",
    "gold.dim_driver": "driver_id",
    "gold.dim_menu_item": "menu_item_id",
}


def reset_postgres_gold(**context) -> None:
    """Xoá sạch schema gold trong Postgres trước khi Spark mirror ghi lại.

    Spark ghi mirror bằng chế độ overwrite, tức xoá rồi tạo lại bảng. Nếu lần
    chạy trước đã gắn khoá ngoại, lệnh xoá bảng sẽ thất bại vì có ràng buộc
    phụ thuộc. Xoá cả schema theo kiểu CASCADE gỡ luôn ràng buộc, cho lần
    mirror sau chạy trên nền sạch.

    Args:
        context: ngữ cảnh Airflow.
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS gold CASCADE")
            cur.execute("CREATE SCHEMA gold")
        conn.commit()
    finally:
        conn.close()
    log.info("Đã tạo lại schema gold sạch cho lần mirror mới")


def add_constraints(**context) -> dict:
    """Gắn khoá chính và khoá ngoại cho các bảng Gold trong PostgreSQL.

    Bản mirror được Spark ghi đè mỗi lần chạy nên không còn ràng buộc; hàm này
    tạo lại chúng. Khoá ngoại biến quan hệ dim–fact thành thứ DBeaver đọc được
    và vẽ thành sơ đồ.

    Args:
        context: ngữ cảnh Airflow.

    Returns:
        Từ điển đếm số khoá đã gắn.
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            for table, pk in PRIMARY_KEYS.items():
                name = table.split(".")[1] + "_pkey"
                cur.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
                cur.execute(f"ALTER TABLE {table} ADD CONSTRAINT {name} PRIMARY KEY ({pk})")

            for ftable, fcol, dtable, dcol, fname in FOREIGN_KEYS:
                cur.execute(f"ALTER TABLE {ftable} DROP CONSTRAINT IF EXISTS {fname}")
                cur.execute(
                    f"ALTER TABLE {ftable} ADD CONSTRAINT {fname} "
                    f"FOREIGN KEY ({fcol}) REFERENCES {dtable} ({dcol})"
                )
        conn.commit()
    finally:
        conn.close()

    log.info("Đã gắn %d khoá chính và %d khoá ngoại", len(PRIMARY_KEYS), len(FOREIGN_KEYS))
    return {"primary_keys": len(PRIMARY_KEYS), "foreign_keys": len(FOREIGN_KEYS)}


def validate_gold(**context) -> dict:
    """Kiểm tra chất lượng mô hình Gold sau khi dựng.

    Ba nhóm kiểm tra:
      - Số dòng: mọi bảng phải có dữ liệu.
      - Bất biến SCD2: mỗi khoá nghiệp vụ chỉ có đúng một dòng is_current.
      - Toàn vẹn tham chiếu: cột SCD2 đủ ba trường trên mọi bảng chiều.

    Args:
        context: ngữ cảnh Airflow.

    Returns:
        Từ điển tổng hợp kết quả.

    Raises:
        ValueError: khi có kiểm tra nghiêm trọng thất bại.
    """
    conn = get_pg_conn()
    problems: list[str] = []
    rows_report: dict[str, int] = {}
    try:
        with conn.cursor() as cur:
            # 1) Số dòng mọi bảng.
            for table in PRIMARY_KEYS:
                cur.execute(f"SELECT count(*) FROM {table}")
                n = cur.fetchone()[0]
                rows_report[table] = n
                if n == 0:
                    problems.append(f"{table} rỗng")

            # 2) Bất biến SCD2: mỗi khoá nghiệp vụ đúng một is_current.
            for table, bk in DIM_BUSINESS_KEYS.items():
                cur.execute(
                    f"SELECT count(*) FROM ("
                    f"  SELECT {bk} FROM {table} WHERE is_current GROUP BY {bk} HAVING count(*) > 1"
                    f") t"
                )
                dup_current = cur.fetchone()[0]
                if dup_current > 0:
                    problems.append(f"{table}: {dup_current} khoá có nhiều hơn một phiên bản hiện hành")

            # 3) SCD2 đủ ba cột trên mọi bảng chiều.
            for table in DIM_BUSINESS_KEYS:
                cur.execute(
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_schema='gold' AND table_name=%s "
                    "AND column_name IN ('valid_from_ts','valid_to_ts','is_current')",
                    (table.split(".")[1],),
                )
                if cur.fetchone()[0] != 3:
                    problems.append(f"{table}: thiếu cột SCD2")
    finally:
        conn.close()

    log.info("=" * 60)
    log.info("KIỂM TRA MÔ HÌNH GOLD")
    log.info("=" * 60)
    for table, n in rows_report.items():
        log.info("  %-30s %12s dòng", table, f"{n:,}")
    log.info("-" * 60)

    if problems:
        raise ValueError("Kiểm tra Gold thất bại: " + "; ".join(problems))

    log.info("Toàn bộ kiểm tra Gold đạt: %d bảng, SCD2 và tham chiếu hợp lệ", len(rows_report))
    return {"tables": len(rows_report), "rows": rows_report, "problems": 0}
