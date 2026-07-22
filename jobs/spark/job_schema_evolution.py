"""
Spark job — xử lý schema evolution ở bảng `raw_menu_items`.

Bối cảnh vấn đề
---------------
Thực đơn tiến hoá theo thời gian: các phân vùng cũ (trước mốc chuyển v2)
KHÔNG có cột `spice_level`, các phân vùng mới thì CÓ. Vì Parquet lưu lược
đồ ngay trong từng file, hai nhóm phân vùng thật sự khác cấu trúc (xem
`data_generator/offline/dimensions.py::generate_menu_items`).

Rủi ro khi đọc ngây thơ
-----------------------
Đọc parquet không bật `mergeSchema`, Spark suy lược đồ từ một tập file bất
kỳ. Nếu file mẫu rơi vào nhóm cũ, cột `spice_level` biến mất khỏi DataFrame
và dữ liệu độ cay của nhóm mới bị **bỏ qua âm thầm** — loại lỗi nguy hiểm
vì không có thông báo nào. Nếu file mẫu rơi vào nhóm mới, các dòng cũ nhận
null. Kết quả phụ thuộc may rủi thứ tự đọc file, không thể tái lập.

Cách xử lý
----------
1. Bật `mergeSchema=true` để Spark hợp nhất lược đồ mọi phân vùng: cột
   `spice_level` luôn hiện diện, dòng thuộc phân vùng cũ mang null.
2. Phân biệt hai loại null khác hẳn bản chất:
     - null do schema evolution: dòng ở phân vùng cũ, cột chưa từng tồn tại.
     - null hợp lệ: dòng ở phân vùng mới nhưng nhóm món không cay.
   Dùng mốc thời gian `v2_start_date` để tách. Với null do evolution, gắn
   cờ `is_legacy_schema=true` và điền giá trị mặc định -1 (nghĩa "không rõ")
   thay vì để lẫn với null hợp lệ.

Cách chạy
---------
    spark-submit job_schema_evolution.py --mode baseline   # đọc không merge
    spark-submit job_schema_evolution.py --mode merged     # merge + xử lý null
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import build_spark, read_bronze, timed, write_analytics

# Mốc chuyển sang schema v2, khớp `schema_evolution.v2_start_date` trong
# `data_generator/config/generator.yaml`. Dòng nạp trước mốc này thuộc lược
# đồ cũ (không có cột spice_level).
V2_START_DATE = "2026-04-20"


def run_baseline(spark: SparkSession) -> None:
    """Đọc bảng không bật mergeSchema để lộ rủi ro mất cột.

    Args:
        spark: phiên Spark đang chạy.
    """
    with timed("baseline: đọc raw_menu_items KHÔNG mergeSchema"):
        df = read_bronze(spark, "raw_menu_items", merge_schema=False)
        has_col = "spice_level" in df.columns
        total = df.count()

    print(f"Tổng dòng: {total:,}", flush=True)
    print(f"Có cột spice_level trong lược đồ suy ra? {has_col}", flush=True)
    if has_col:
        n_null = df.filter(F.col("spice_level").isNull()).count()
        print(f"  spice_level null: {n_null:,} (lẫn lộn cũ + hợp lệ, không phân biệt được)", flush=True)
    else:
        print("  → Cột spice_level đã bị BỎ QUA âm thầm, mất toàn bộ dữ liệu độ cay.", flush=True)
    print(f"Các cột đọc được: {df.columns}", flush=True)


def run_merged(spark: SparkSession) -> None:
    """Đọc có mergeSchema và phân loại hai kiểu null.

    Args:
        spark: phiên Spark đang chạy.
    """
    with timed("merged: đọc raw_menu_items CÓ mergeSchema + xử lý null"):
        df = read_bronze(spark, "raw_menu_items", merge_schema=True)

        if "spice_level" not in df.columns:
            raise RuntimeError(
                "Bật mergeSchema mà vẫn không thấy spice_level — kiểm tra lại "
                "dữ liệu Bronze có thật sự chứa phân vùng v2 không."
            )

        # ingested_date là cột phân vùng do DP1 ghi ra; dùng nó để xác định
        # dòng thuộc lược đồ cũ hay mới.
        is_legacy = F.col("ingested_date") < F.lit(V2_START_DATE)

        processed = df.withColumn("is_legacy_schema", is_legacy).withColumn(
            "spice_level",
            # Null ở phân vùng cũ: điền -1 (không rõ). Null ở phân vùng mới:
            # giữ nguyên vì đó là null hợp lệ (món không cay).
            F.when(is_legacy & F.col("spice_level").isNull(), F.lit(-1)).otherwise(
                F.col("spice_level")
            ),
        )
        write_analytics(processed, "menu_items_schema_resolved")

        total = processed.count()
        legacy = processed.filter(F.col("is_legacy_schema")).count()
        legit_null = processed.filter(
            (~F.col("is_legacy_schema")) & F.col("spice_level").isNull()
        ).count()

    print(f"Tổng dòng sau merge: {total:,}", flush=True)
    print(f"  Dòng lược đồ cũ (spice_level điền -1): {legacy:,}", flush=True)
    print(f"  Null hợp lệ ở phân vùng mới (món không cay): {legit_null:,}", flush=True)
    processed.select("menu_item_id", "category", "spice_level", "is_legacy_schema").show(
        8, False
    )


def main() -> None:
    """Điểm vào: đọc tham số và chạy đúng chế độ."""
    parser = argparse.ArgumentParser(description="Xử lý schema evolution menu_item")
    parser.add_argument("--mode", choices=["baseline", "merged"], required=True)
    args = parser.parse_args()

    spark = build_spark(f"phase4_schema_{args.mode}")

    if args.mode == "baseline":
        run_baseline(spark)
    else:
        run_merged(spark)

    spark.stop()


if __name__ == "__main__":
    main()
