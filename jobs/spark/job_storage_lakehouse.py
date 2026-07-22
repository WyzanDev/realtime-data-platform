"""
Spark job — tối ưu lưu trữ tầng lakehouse bằng Delta Lake.

Minh hoạ hai kỹ thuật tối ưu kho dữ liệu hồ (lakehouse) và đo tác động:

  1. Compaction: gom nhiều tệp nhỏ thành ít tệp lớn. Ghi theo lô/luồng
     thường tạo ra hàng trăm tệp nhỏ; mỗi tệp là một lần mở/đóng khi đọc,
     nên nhiều tệp nhỏ làm truy vấn chậm. Lệnh OPTIMIZE gộp chúng lại.

  2. Z-order: sắp xếp lại dữ liệu theo nhiều cột (ở đây `delivery_city` và
     `restaurant_id`) sao cho các dòng có giá trị gần nhau nằm chung tệp.
     Kết hợp với thống kê min/max mà Delta lưu cho mỗi tệp, truy vấn lọc
     theo các cột này bỏ qua được phần lớn tệp không chứa dữ liệu cần —
     gọi là data skipping.

Quy trình đo:
  - Ghi bảng Delta cố tình chia thành nhiều tệp nhỏ (mô phỏng ghi luồng).
  - Đo số tệp và thời gian một truy vấn lọc TRƯỚC tối ưu.
  - Chạy OPTIMIZE ... ZORDER BY.
  - Đo lại số tệp và thời gian cùng truy vấn SAU tối ưu.

Cách chạy: xem lệnh spark-submit kèm --packages Delta trong
docs/StorageOptimization.md.
"""

from __future__ import annotations

import os

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from common import BUCKET_ANALYTICS, build_spark, read_bronze, timed

# Đường dẫn bảng Delta trên MinIO.
DELTA_PATH = f"s3a://{BUCKET_ANALYTICS}/delta_orders"

# Số tệp nhỏ cố ý tạo ra khi ghi, để có gì đó cho compaction gom lại.
N_SMALL_FILES = 200

# Truy vấn đối chứng: lọc đúng theo hai cột sẽ Z-order, để thấy tác dụng
# data skipping. Chọn một thành phố lớn và một dải nhà hàng hẹp.
FILTER_CITY = "Ho Chi Minh"
FILTER_RESTAURANT = "RST003000"


def _count_files(spark) -> int:
    """Đếm số tệp dữ liệu hiện tại của bảng Delta.

    Args:
        spark: phiên Spark.

    Returns:
        Số tệp parquet mà phiên bản mới nhất của bảng đang trỏ tới.
    """
    detail = DeltaTable.forPath(spark, DELTA_PATH).detail().collect()[0]
    return int(detail["numFiles"])


def _timed_query(spark, label: str) -> None:
    """Chạy truy vấn lọc đối chứng và in thời gian.

    Args:
        spark: phiên Spark.
        label: nhãn để phân biệt lần đo trước/sau.
    """
    with timed(f"truy vấn lọc [{label}]"):
        n = (
            spark.read.format("delta")
            .load(DELTA_PATH)
            .where(
                (F.col("delivery_city") == FILTER_CITY)
                & (F.col("restaurant_id") == FILTER_RESTAURANT)
            )
            .count()
        )
    print(f"  [{label}] số dòng khớp: {n:,} | số tệp bảng: {_count_files(spark):,}", flush=True)


def _step_write(spark) -> None:
    """Ghi bảng Delta thành nhiều tệp nhỏ và đo truy vấn TRƯỚC tối ưu.

    Dừng ngay sau bước này (khi STEP=write) để kịp chụp trạng thái
    ~200 tệp nhỏ trên MinIO console trước khi OPTIMIZE gộp lại.
    """
    orders = read_bronze(spark, "raw_orders").select(
        "order_id", "delivery_city", "restaurant_id", "total_amount", "order_time", "status"
    )
    with timed(f"ghi Delta thành {N_SMALL_FILES} tệp nhỏ"):
        orders.repartition(N_SMALL_FILES).write.format("delta").mode("overwrite").save(
            DELTA_PATH
        )
    print("\n========== TRƯỚC TỐI ƯU ==========", flush=True)
    _timed_query(spark, "trước")


def _step_optimize(spark) -> None:
    """Chạy OPTIMIZE + ZORDER rồi VACUUM để xoá hẳn tệp cũ, đo truy vấn SAU.

    VACUUM RETAIN 0 HOURS dọn các tệp nhỏ đã bị compaction thay thế, để ảnh
    chụp MinIO sau tối ưu chỉ còn đúng tệp lớn (không lẫn tệp cũ của Delta).
    """
    with timed("OPTIMIZE + ZORDER BY (delivery_city, restaurant_id)"):
        DeltaTable.forPath(spark, DELTA_PATH).optimize().executeZOrderBy(
            "delivery_city", "restaurant_id"
        )
    # Xoá tệp cũ đã bị thay thế để ảnh "sau" trên MinIO sạch còn 1 tệp.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    with timed("VACUUM RETAIN 0 HOURS (dọn tệp cũ)"):
        DeltaTable.forPath(spark, DELTA_PATH).vacuum(0)

    print("\n========== SAU TỐI ƯU ==========", flush=True)
    _timed_query(spark, "sau")


def main() -> None:
    """Dựng bảng Delta nhiều tệp nhỏ, đo, OPTIMIZE ZORDER, đo lại.

    Biến môi trường STEP điều khiển bước chạy để chụp proof trước/sau:
      - STEP=write     : chỉ ghi 200 tệp nhỏ rồi dừng (chụp ảnh "trước").
      - STEP=optimize  : chỉ OPTIMIZE+ZORDER+VACUUM rồi dừng (chụp ảnh "sau").
      - không set (mặc định): chạy trọn cả hai (giữ nguyên hành vi cũ).
    """
    step = os.environ.get("STEP", "all").lower()
    spark = build_spark(
        "phase6_lakehouse_delta",
        extra_conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # Bật thu thập thống kê để data skipping có dữ liệu mà dùng.
            "spark.databricks.delta.optimize.maxFileSize": "134217728",
        },
    )

    if step in ("write", "all"):
        _step_write(spark)
    if step in ("optimize", "all"):
        _step_optimize(spark)

    spark.stop()


if __name__ == "__main__":
    main()
