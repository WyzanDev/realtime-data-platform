"""
DP3 (phần xử lý bằng Spark) — tính bảng feature offline từ tầng Gold.

Ba bảng feature phục vụ mô hình ML sau này (final coursework), đặt nền cho
feature store (Feast):

  - feat_customer_order_freq_7d      : số đơn của mỗi khách trong 7 ngày gần nhất
  - feat_restaurant_avg_prep_time_30d: thời gian chuẩn bị trung bình mỗi quán 30 ngày
  - feat_driver_acceptance_rate_7d   : tỷ lệ hoàn tất đơn của mỗi tài xế 7 ngày

Mỗi bảng feature có đúng hai cột bắt buộc theo chuẩn Feast:

  - `event_timestamp`: mốc thời gian mà giá trị feature có hiệu lực (as-of).
  - `created`:         thời điểm feature được tính ra.

Vì dữ liệu là ảnh chụp 180 ngày, feature "N ngày gần nhất" được tính **tính
đến ngày mốc** là ngày đơn mới nhất trong kho. Mỗi thực thể cho một dòng,
`event_timestamp` bằng ngày mốc đó.

Kết quả ghi ra hai nơi giống DP2: Delta trên lakehouse (Trino truy vấn) và
mirror PostgreSQL (DBeaver xem). Thông tin đăng nhập đọc từ biến môi trường.
"""

from __future__ import annotations

from datetime import timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common import build_spark
from dp2_build_gold import DELTA_CONF, _mirror_to_postgres, _save_delta


def _with_feast_columns(df: DataFrame, event_ts) -> DataFrame:
    """Gắn hai cột chuẩn Feast vào một bảng feature.

    Args:
        df: bảng feature đã có khoá thực thể và giá trị.
        event_ts: mốc thời gian hiệu lực (as-of) của toàn bộ bảng.

    Returns:
        DataFrame kèm `event_timestamp` và `created`.
    """
    return df.withColumn("event_timestamp", F.lit(event_ts).cast("timestamp")).withColumn(
        "created", F.current_timestamp()
    )


def main() -> None:
    """Tính ba bảng feature từ Gold rồi ghi Delta + mirror PostgreSQL."""
    spark = build_spark("dp3_build_features", extra_conf=DELTA_CONF)

    # Đọc tầng Silver/Gold đã đăng ký trong Hive Metastore.
    orders = spark.table("silver.stg_orders")
    dim_restaurant = spark.table("gold.dim_restaurant").filter("is_current")

    # Ngày mốc: thời điểm đơn mới nhất trong kho. Mọi cửa sổ tính lùi từ đây.
    ref_ts = orders.agg(F.max("order_time")).collect()[0][0]
    cutoff_7d = ref_ts - timedelta(days=7)
    cutoff_30d = ref_ts - timedelta(days=30)
    print(f">>> Ngày mốc (event_timestamp) = {ref_ts}", flush=True)

    # --- feat_customer_order_freq_7d ---
    feat_customer = _with_feast_columns(
        orders.filter(F.col("order_time") >= F.lit(cutoff_7d))
        .groupBy("customer_id")
        .agg(F.count("*").cast("long").alias("order_freq_7d")),
        ref_ts,
    )
    _save_delta(spark, feat_customer, "gold", "feat_customer_order_freq_7d")
    _mirror_to_postgres(feat_customer, "gold.feat_customer_order_freq_7d")

    # --- feat_restaurant_avg_prep_time_30d ---
    # Quán có đơn trong 30 ngày, thời gian chuẩn bị trung bình (lấy từ chiều
    # quán, tính trên các đơn thuộc cửa sổ 30 ngày).
    feat_restaurant = _with_feast_columns(
        orders.filter(F.col("order_time") >= F.lit(cutoff_30d))
        .join(dim_restaurant.select("restaurant_id", "prep_time_minutes"), "restaurant_id")
        .groupBy("restaurant_id")
        .agg(
            F.round(F.avg("prep_time_minutes"), 2).alias("avg_prep_time_30d"),
            F.count("*").cast("long").alias("orders_30d"),
        ),
        ref_ts,
    )
    _save_delta(spark, feat_restaurant, "gold", "feat_restaurant_avg_prep_time_30d")
    _mirror_to_postgres(feat_restaurant, "gold.feat_restaurant_avg_prep_time_30d")

    # --- feat_driver_acceptance_rate_7d ---
    # Tỷ lệ đơn hoàn tất trên tổng đơn được gán cho tài xế trong 7 ngày.
    feat_driver = _with_feast_columns(
        orders.filter(
            (F.col("order_time") >= F.lit(cutoff_7d)) & F.col("driver_id").isNotNull()
        )
        .groupBy("driver_id")
        .agg(
            F.round(
                F.sum(F.when(F.col("status") == "completed", 1).otherwise(0))
                / F.count("*"),
                4,
            ).alias("acceptance_rate_7d"),
            F.count("*").cast("long").alias("orders_7d"),
        ),
        ref_ts,
    )
    _save_delta(spark, feat_driver, "gold", "feat_driver_acceptance_rate_7d")
    _mirror_to_postgres(feat_driver, "gold.feat_driver_acceptance_rate_7d")

    print(">>> Hoàn tất DP3 build.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
