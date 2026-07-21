"""
Spark job — khử bản ghi trùng trong `raw_orders`.

Bối cảnh vấn đề
---------------
Cổng thanh toán retry khi không nhận được ACK, nên khoảng 2% đơn bị ghi
trùng `order_id`. Bản trùng giống hệt bản gốc ở mọi cột nghiệp vụ, chỉ
khác `ingested_at` muộn hơn vài giây đến vài phút (xem
`data_generator/offline/facts.py::inject_duplicates`).

Vì sao không dùng dropDuplicates
--------------------------------
`dropDuplicates(["order_id"])` giữ lại **một bản bất kỳ** trong nhóm trùng,
không đảm bảo là bản mới nhất. Với dữ liệu retry, bản đến sau mới là bản
phản ánh trạng thái cuối cùng, nên giữ nhầm bản cũ có thể mất thông tin đã
được cập nhật ở lần ghi sau.

Cách xử lý đúng
---------------
Đánh số thứ tự trong mỗi nhóm `order_id` theo `ingested_at` giảm dần bằng
hàm cửa sổ `row_number`, rồi chỉ giữ dòng số 1 — tức bản nạp muộn nhất.
Cách này cho kết quả xác định, không phụ thuộc thứ tự Spark đọc file.

    row_number() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) = 1

Cách chạy
---------
    spark-submit job_dedup.py --mode baseline   # chỉ đo mức trùng
    spark-submit job_dedup.py --mode dedup      # khử trùng và ghi kết quả
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from common import build_spark, read_bronze, timed, write_analytics


def run_baseline(spark: SparkSession) -> None:
    """Đo mức độ trùng lặp mà không xử lý — làm mốc so sánh.

    Args:
        spark: phiên Spark đang chạy.
    """
    orders = read_bronze(spark, "raw_orders")

    with timed("baseline: đếm trùng theo order_id"):
        total = orders.count()
        distinct = orders.select("order_id").distinct().count()

    dup = total - distinct
    print(
        f"Tổng dòng: {total:,} | order_id phân biệt: {distinct:,} | "
        f"trùng: {dup:,} ({dup / distinct * 100:.2f}%)",
        flush=True,
    )


def run_dedup(spark: SparkSession) -> None:
    """Khử trùng bằng hàm cửa sổ, giữ bản nạp muộn nhất, rồi ghi kết quả.

    Args:
        spark: phiên Spark đang chạy.
    """
    orders = read_bronze(spark, "raw_orders")
    total = orders.count()

    with timed("dedup: row_number theo ingested_at giảm dần"):
        window = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
        deduped = (
            orders.withColumn("_rn", F.row_number().over(window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        write_analytics(deduped, "orders_deduped")
        kept = deduped.count()

    print(
        f"Trước: {total:,} dòng | Sau khử trùng: {kept:,} dòng | "
        f"Đã loại: {total - kept:,} bản trùng",
        flush=True,
    )


def main() -> None:
    """Điểm vào: đọc tham số và chạy đúng chế độ."""
    parser = argparse.ArgumentParser(description="Khử trùng raw_orders")
    parser.add_argument("--mode", choices=["baseline", "dedup"], required=True)
    args = parser.parse_args()

    spark = build_spark(f"phase4_dedup_{args.mode}")

    if args.mode == "baseline":
        run_baseline(spark)
    else:
        run_dedup(spark)

    spark.stop()


if __name__ == "__main__":
    main()
