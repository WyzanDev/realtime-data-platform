"""
Giai đoạn kiểm tra của DP3.

Kiểm tra ba bảng feature trong PostgreSQL:

  - Có đủ dữ liệu (số dòng > 0).
  - Có đúng hai cột bắt buộc theo chuẩn Feast: `event_timestamp` và `created`.
  - Khoá thực thể không rỗng.

Hai cột `event_timestamp`/`created` là điều kiện để bảng feature dùng được
với Feast ở giai đoạn ML sau này, nên đây là kiểm tra bắt buộc.
"""

from __future__ import annotations

import logging

from dp2.common import get_pg_conn

log = logging.getLogger(__name__)

# Bảng feature và khoá thực thể tương ứng.
FEATURE_TABLES = {
    "gold.feat_customer_order_freq_7d": "customer_id",
    "gold.feat_restaurant_avg_prep_time_30d": "restaurant_id",
    "gold.feat_driver_acceptance_rate_7d": "driver_id",
}

REQUIRED_COLUMNS = ("event_timestamp", "created")


def validate_features(**context) -> dict:
    """Kiểm tra ba bảng feature: số dòng, hai cột Feast, khoá không rỗng.

    Args:
        context: ngữ cảnh Airflow.

    Returns:
        Từ điển tổng hợp kết quả.

    Raises:
        ValueError: khi có bảng thiếu cột bắt buộc hoặc rỗng.
    """
    conn = get_pg_conn()
    problems: list[str] = []
    report: dict[str, int] = {}
    try:
        with conn.cursor() as cur:
            for table, entity in FEATURE_TABLES.items():
                name = table.split(".")[1]

                # Đủ hai cột Feast.
                cur.execute(
                    "SELECT count(*) FROM information_schema.columns "
                    "WHERE table_schema='gold' AND table_name=%s AND column_name = ANY(%s)",
                    (name, list(REQUIRED_COLUMNS)),
                )
                if cur.fetchone()[0] != len(REQUIRED_COLUMNS):
                    problems.append(f"{table}: thiếu cột event_timestamp/created")

                # Số dòng và khoá không rỗng.
                cur.execute(f"SELECT count(*), count(*) FILTER (WHERE {entity} IS NULL) FROM {table}")
                n, null_keys = cur.fetchone()
                report[table] = n
                if n == 0:
                    problems.append(f"{table} rỗng")
                if null_keys > 0:
                    problems.append(f"{table}: {null_keys} khoá thực thể rỗng")
    finally:
        conn.close()

    log.info("=" * 60)
    log.info("KIỂM TRA BẢNG FEATURE (DP3)")
    log.info("=" * 60)
    for table, n in report.items():
        log.info("  %-42s %10s dòng", table, f"{n:,}")

    if problems:
        raise ValueError("Kiểm tra feature thất bại: " + "; ".join(problems))

    log.info("Toàn bộ %d bảng feature hợp lệ (đủ event_timestamp + created)", len(report))
    return {"tables": len(report), "rows": report}
