"""
Spark job — xử lý cột có lực lượng lớn (high cardinality) và join hiệu quả.

Bối cảnh vấn đề
---------------
Hệ thống có vài cột lực lượng rất lớn: `customer_id` (~120 nghìn UUID),
`menu_item_id` (45 nghìn), `restaurant_id` (8 nghìn). Đây là hai nhu cầu
thường gặp và cùng bị chi phối bởi lực lượng lớn:

  1. Đếm số giá trị phân biệt (bao nhiêu khách, bao nhiêu món đã bán).
  2. Nối bảng lớn với bảng danh mục theo khoá lực lượng lớn.

Phần A — đếm phân biệt
----------------------
`countDistinct` cho kết quả chính xác nhưng phải gom toàn bộ giá trị phân
biệt qua shuffle, rất nặng khi lực lượng lớn. `approx_count_distinct` dùng
thuật toán HyperLogLog, chỉ giữ một bản phác thảo nhỏ trong bộ nhớ, nhanh
hơn nhiều lần với sai số điển hình dưới 2%. Với báo cáo giám sát thì mức
sai số này hoàn toàn chấp nhận được.

Phần B — join
-------------
Nối `raw_order_items` (6,25 triệu dòng) với `raw_menu_items` (45 nghìn dòng)
theo `menu_item_id`. Sort-merge join phải shuffle cả 6,25 triệu dòng của
bảng lớn theo khoá — tốn kém. Vì bảng danh mục đủ nhỏ để nằm gọn trong bộ
nhớ, phát nó tới mọi executor (broadcast) giúp tránh hẳn shuffle bảng lớn.

Lưu ý về "broadcast sai chỗ": broadcast chỉ đúng khi bảng được phát đủ nhỏ.
Khi CẢ HAI bảng đều lớn (ví dụ nối `raw_orders` 2,55 triệu với
`raw_order_items` 6,25 triệu), không bảng nào broadcast được; lúc đó cách
đúng là bucketing hai bảng theo cùng khoá để khử shuffle lặp lại — xem
phần thảo luận trong `docs/SparkOptimization.md`.

Cách chạy
---------
    spark-submit job_cardinality.py --mode baseline    # countDistinct + SMJ
    spark-submit job_cardinality.py --mode optimized   # approx + broadcast
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import build_spark, read_bronze, timed, write_analytics


def _count_distinct_exact(spark: SparkSession) -> None:
    """Đếm phân biệt chính xác — nặng vì phải shuffle mọi giá trị.

    Args:
        spark: phiên Spark đang chạy.
    """
    orders = read_bronze(spark, "raw_orders")
    items = read_bronze(spark, "raw_order_items")

    with timed("baseline: countDistinct chính xác"):
        row = orders.agg(
            F.countDistinct("customer_id").alias("customers"),
            F.countDistinct("restaurant_id").alias("restaurants"),
        ).collect()[0]
        items_distinct = items.agg(
            F.countDistinct("menu_item_id").alias("menu_items")
        ).collect()[0]["menu_items"]

    print(
        f"[chính xác] customers={row['customers']:,} | "
        f"restaurants={row['restaurants']:,} | menu_items={items_distinct:,}",
        flush=True,
    )


def _count_distinct_approx(spark: SparkSession) -> None:
    """Đếm phân biệt xấp xỉ bằng HyperLogLog — nhanh, sai số nhỏ.

    Args:
        spark: phiên Spark đang chạy.
    """
    orders = read_bronze(spark, "raw_orders")
    items = read_bronze(spark, "raw_order_items")

    with timed("optimized: approx_count_distinct (HLL)"):
        row = orders.agg(
            F.approx_count_distinct("customer_id").alias("customers"),
            F.approx_count_distinct("restaurant_id").alias("restaurants"),
        ).collect()[0]
        items_approx = items.agg(
            F.approx_count_distinct("menu_item_id").alias("menu_items")
        ).collect()[0]["menu_items"]

    print(
        f"[xấp xỉ]   customers={row['customers']:,} | "
        f"restaurants={row['restaurants']:,} | menu_items={items_approx:,}",
        flush=True,
    )


def _join_smj(spark: SparkSession) -> None:
    """Nối order_items với menu_items bằng sort-merge join (baseline).

    Tắt broadcast để ép sort-merge join: cả 6,25 triệu dòng order_items bị
    shuffle theo menu_item_id.

    Args:
        spark: phiên Spark đang chạy.
    """
    items = read_bronze(spark, "raw_order_items").select(
        "order_item_id", "menu_item_id", "quantity", "subtotal"
    )
    menu = read_bronze(spark, "raw_menu_items", merge_schema=True).select(
        "menu_item_id", "category"
    )

    with timed("baseline: sort-merge join (shuffle 6,25 triệu dòng)"):
        joined = items.join(menu, on="menu_item_id", how="inner")
        result = joined.groupBy("category").agg(
            F.round(F.sum("subtotal"), 0).alias("revenue"),
            F.sum("quantity").alias("units"),
        )
        write_analytics(result, "category_revenue_baseline")

    result.orderBy(F.desc("revenue")).show(12, False)


def _join_broadcast(spark: SparkSession) -> None:
    """Nối order_items với menu_items bằng broadcast join (optimized).

    Phát bảng danh mục 45 nghìn dòng tới mọi executor, tránh hẳn việc
    shuffle bảng lớn.

    Args:
        spark: phiên Spark đang chạy.
    """
    items = read_bronze(spark, "raw_order_items").select(
        "order_item_id", "menu_item_id", "quantity", "subtotal"
    )
    menu = read_bronze(spark, "raw_menu_items", merge_schema=True).select(
        "menu_item_id", "category"
    )

    with timed("optimized: broadcast join (không shuffle bảng lớn)"):
        joined = items.join(F.broadcast(menu), on="menu_item_id", how="inner")
        result = joined.groupBy("category").agg(
            F.round(F.sum("subtotal"), 0).alias("revenue"),
            F.sum("quantity").alias("units"),
        )
        write_analytics(result, "category_revenue_optimized")

    result.orderBy(F.desc("revenue")).show(12, False)


def main() -> None:
    """Điểm vào: đọc tham số và chạy đúng chế độ."""
    parser = argparse.ArgumentParser(description="Xử lý high cardinality + join")
    parser.add_argument("--mode", choices=["baseline", "optimized"], required=True)
    args = parser.parse_args()

    # Tắt AQE và broadcast tự động để hai chế độ khác biệt rõ trên Spark UI.
    # Ở chế độ optimized ta chủ động gọi F.broadcast, không phụ thuộc ngưỡng.
    spark = build_spark(
        f"phase4_cardinality_{args.mode}",
        extra_conf={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
        },
    )

    if args.mode == "baseline":
        _count_distinct_exact(spark)
        _join_smj(spark)
    else:
        _count_distinct_approx(spark)
        _join_broadcast(spark)

    spark.stop()


if __name__ == "__main__":
    main()
