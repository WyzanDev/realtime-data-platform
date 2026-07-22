"""
DP2 (phần xử lý bằng Spark) — dựng tầng Silver và Gold từ Bronze.

Đầu vào là dữ liệu thô ở tầng Bronze (parquet trên MinIO). Đầu ra gồm hai
bản, phục vụ hai mục đích khác nhau:

  1. Lakehouse (Delta trên MinIO): nguồn sự thật của Silver/Gold. Bảng được
     ghi bằng `saveAsTable` nên vừa nằm dưới dạng Delta trên MinIO, vừa được
     đăng ký vào Hive Metastore — Trino nhờ đó truy vấn được ngay.

  2. Bản sao trong PostgreSQL (schema `gold`): dùng để DBeaver vẽ sơ đồ quan
     hệ dim–fact bằng khoá ngoại thật — điều Trino/Delta không có sẵn.

Mô hình Gold theo hình sao:

  - 4 bảng chiều SCD2: dim_customer, dim_restaurant, dim_driver, dim_menu_item.
    Mỗi bảng có khoá thay thế `<thực thể>_sk`, khoá nghiệp vụ `<thực thể>_id`,
    và ba cột SCD2 `valid_from_ts`, `valid_to_ts`, `is_current`.
  - 2 bảng sự kiện: fact_orders, fact_delivery_events, tham chiếu tới chiều
    qua khoá thay thế.

Tầng Silver (`stg_*`) nằm giữa: bản đã khử trùng của Bronze, là đầu vào sạch
cho các bảng fact.

Thông tin đăng nhập đọc từ biến môi trường (MinIO, Postgres), không viết cứng.
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from common import build_spark, read_bronze

# Gốc lakehouse trên MinIO cho tầng Silver/Gold dạng Delta.
LAKE = "s3a://lakehouse"

# Cấu hình phiên Spark: Delta + danh mục Hive (thrift tới Hive Metastore) để
# saveAsTable vừa ghi Delta lên MinIO vừa đăng ký bảng cho Trino thấy.
DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.sql.catalogImplementation": "hive",
    "spark.hadoop.hive.metastore.uris": "thrift://hive-metastore:9083",
    # Một số cột thời gian ở Bronze do pandas ghi ở độ chính xác nanosecond,
    # mà Spark không đọc trực tiếp được. Bật cờ này để đọc chúng dưới dạng
    # long (nanosecond kể từ epoch) rồi tự chuyển về timestamp khi cần.
    "spark.sql.legacy.parquet.nanosAsLong": "true",
}


def _nanos_to_ts(col: str):
    """Chuyển một cột long (nanosecond kể từ epoch) về timestamp.

    Args:
        col: tên cột chứa nanosecond dạng long.

    Returns:
        Cột timestamp tương ứng.
    """
    return F.timestamp_seconds(F.col(col) / F.lit(1_000_000_000))


def _save_delta(spark: SparkSession, df: DataFrame, db: str, table: str) -> int:
    """Ghi một DataFrame thành bảng Delta được quản lý trong Hive Metastore.

    Xoá bảng cũ trước khi ghi để tránh lỗi ghi đè của DeltaCatalog và đảm bảo
    mỗi lần chạy cho ra bảng sạch. Vì database trỏ location trên MinIO nên
    bảng managed nằm luôn trên lakehouse.

    Args:
        spark: phiên Spark.
        df: dữ liệu cần ghi.
        db: tên database (silver hoặc gold).
        table: tên bảng.

    Returns:
        Số dòng đã ghi.
    """
    n = df.count()
    spark.sql(f"DROP TABLE IF EXISTS {db}.{table}")
    df.write.format("delta").saveAsTable(f"{db}.{table}")
    return n


def _mirror_to_postgres(df: DataFrame, table: str) -> None:
    """Ghi một bảng Gold sang PostgreSQL để DBeaver vẽ sơ đồ quan hệ.

    Args:
        df: dữ liệu cần mirror.
        table: tên bảng đích dạng `schema.table`.
    """
    url = f"jdbc:postgresql://{os.environ.get('PG_HOST', 'postgres')}:5432/{os.environ.get('PG_DB', 'warehouse')}"
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", os.environ["PG_USER"])
        .option("password", os.environ["PG_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("batchsize", 10000)
        .mode("overwrite")
        .save()
    )


def _add_scd2(df: DataFrame, sk_name: str, order_col: str) -> DataFrame:
    """Thêm khoá thay thế và ba cột SCD2 cho một bảng chiều (lần nạp đầu).

    Lần nạp đầu tiên: mọi dòng là phiên bản hiện hành, hiệu lực từ thời điểm
    chạy luồng, chưa có mốc hết hiệu lực. Khoá thay thế đánh số tuần tự theo
    khoá nghiệp vụ để ổn định và dễ đối chiếu.

    Khi có lần nạp sau với thuộc tính thay đổi, quy trình SCD2 sẽ đóng dòng cũ
    (đặt `valid_to_ts`, `is_current=false`) và thêm dòng mới — cấu trúc ba cột
    này chính là chỗ để làm việc đó.

    Args:
        df: bảng chiều đã chọn cột nghiệp vụ.
        sk_name: tên cột khoá thay thế cần tạo.
        order_col: cột khoá nghiệp vụ để đánh số khoá thay thế.

    Returns:
        DataFrame kèm khoá thay thế và ba cột SCD2.
    """
    w = Window.orderBy(order_col)
    return (
        df.withColumn(sk_name, F.row_number().over(w).cast("long"))
        .withColumn("valid_from_ts", F.current_timestamp())
        .withColumn("valid_to_ts", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )


def build_silver(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """Dựng tầng Silver: khử trùng đơn hàng và sự kiện giao.

    Args:
        spark: phiên Spark.

    Returns:
        Cặp (stg_orders, stg_delivery_events) đã khử trùng.
    """
    # stg_orders: giữ bản nạp muộn nhất cho mỗi order_id (loại 2% trùng).
    orders = read_bronze(spark, "raw_orders")
    w_ord = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
    stg_orders = (
        orders.withColumn("_rn", F.row_number().over(w_ord))
        .filter(F.col("_rn") == 1)
        .select(
            "order_id", "customer_id", "restaurant_id", "driver_id", "order_time",
            "status", "total_amount", "payment_method", "delivery_city", "distance_km",
            "estimated_delivery_time", "actual_delivery_time",
        )
    )
    _save_delta(spark, stg_orders, "silver", "stg_orders")

    # stg_delivery_events: giữ một bản cho mỗi event_id (loại 1.5% trùng).
    events = read_bronze(spark, "raw_delivery_events")
    w_evt = Window.partitionBy("event_id").orderBy(F.col("sequence_number").asc())
    stg_events = (
        events.withColumn("_rn", F.row_number().over(w_evt))
        .filter(F.col("_rn") == 1)
        .select(
            "event_id", "order_id", "driver_id", "event_type",
            F.col("event_time").cast("timestamp").alias("event_time"),
            "lat", "lon", "sequence_number",
        )
    )
    _save_delta(spark, stg_events, "silver", "stg_delivery_events")

    return stg_orders, stg_events


def build_dimensions(spark: SparkSession) -> dict[str, DataFrame]:
    """Dựng bốn bảng chiều SCD2 và ghi ra lakehouse.

    Args:
        spark: phiên Spark.

    Returns:
        Từ điển tên chiều → DataFrame, để bước dựng fact nối khoá thay thế.
    """
    dim_customer = _add_scd2(
        read_bronze(spark, "raw_customers")
        .select("customer_id", "name", "city", "district", "segment", "signup_date")
        # signup_date ở Bronze là nanosecond-long, chuyển về timestamp.
        .withColumn("signup_date", _nanos_to_ts("signup_date")),
        "customer_sk", "customer_id",
    )
    _save_delta(spark, dim_customer, "gold", "dim_customer")

    dim_restaurant = _add_scd2(
        read_bronze(spark, "raw_restaurants").select(
            "restaurant_id", "name", "city", "district", "category", "rating",
            "prep_time_minutes",
        ),
        "restaurant_sk", "restaurant_id",
    )
    _save_delta(spark, dim_restaurant, "gold", "dim_restaurant")

    dim_driver = _add_scd2(
        read_bronze(spark, "raw_drivers").select(
            "driver_id", "name", "vehicle_type", "city", "rating"
        ),
        "driver_sk", "driver_id",
    )
    _save_delta(spark, dim_driver, "gold", "dim_driver")

    # dim_menu_item: mergeSchema để có cột spice_level từ phân vùng v2.
    menu_src = read_bronze(spark, "raw_menu_items", merge_schema=True)
    if "spice_level" not in menu_src.columns:
        menu_src = menu_src.withColumn("spice_level", F.lit(None).cast("double"))
    dim_menu_item = _add_scd2(
        menu_src.select(
            "menu_item_id", "restaurant_id", "name", "category", "price",
            "is_available", "spice_level",
        ),
        "menu_item_sk", "menu_item_id",
    )
    _save_delta(spark, dim_menu_item, "gold", "dim_menu_item")

    return {
        "customer": dim_customer,
        "restaurant": dim_restaurant,
        "driver": dim_driver,
        "menu_item": dim_menu_item,
    }


def build_facts(
    spark: SparkSession, stg_orders: DataFrame, stg_events: DataFrame, dims: dict[str, DataFrame]
) -> dict[str, DataFrame]:
    """Dựng ba bảng fact, nối tới chiều qua khoá thay thế của phiên bản hiện hành.

    Args:
        spark: phiên Spark.
        stg_orders: đơn hàng đã khử trùng.
        stg_events: sự kiện giao đã khử trùng.
        dims: các bảng chiều đã dựng.

    Returns:
        Từ điển tên fact → DataFrame.
    """
    dc = dims["customer"].filter("is_current").select("customer_sk", "customer_id")
    dr = dims["restaurant"].filter("is_current").select("restaurant_sk", "restaurant_id")
    dd = dims["driver"].filter("is_current").select("driver_sk", "driver_id")
    dm = dims["menu_item"].filter("is_current").select("menu_item_sk", "menu_item_id")

    fact_orders = (
        stg_orders.join(dc, "customer_id", "left")
        .join(dr, "restaurant_id", "left")
        .join(dd, "driver_id", "left")
        .select(
            "order_id", "customer_sk", "restaurant_sk", "driver_sk", "order_time",
            "status", "total_amount", "payment_method", "delivery_city", "distance_km",
        )
    )
    _save_delta(spark, fact_orders, "gold", "fact_orders")

    fact_events = (
        stg_events.join(dd, "driver_id", "left")
        .select(
            "event_id", "order_id", "driver_sk", "event_type", "event_time",
            "lat", "lon", "sequence_number",
        )
    )
    _save_delta(spark, fact_events, "gold", "fact_delivery_events")

    # fact_order_items: chi tiết món trong đơn, nối tới dim_menu_item. Đây là
    # bảng khiến dim_menu_item có mặt trong sơ đồ sao (trước đó chưa nối fact nào).
    order_items = read_bronze(spark, "raw_order_items").select(
        "order_item_id", "order_id", "menu_item_id", "quantity", "unit_price", "subtotal"
    )
    fact_order_items = order_items.join(dm, "menu_item_id", "left").select(
        "order_item_id", "order_id", "menu_item_sk", "quantity", "unit_price", "subtotal"
    )
    _save_delta(spark, fact_order_items, "gold", "fact_order_items")

    return {
        "orders": fact_orders,
        "delivery_events": fact_events,
        "order_items": fact_order_items,
    }


def main() -> None:
    """Dựng Silver + Gold (Delta lakehouse) rồi mirror Gold sang PostgreSQL."""
    spark = build_spark("dp2_build_gold", extra_conf=DELTA_CONF)

    # Database trỏ location trên MinIO: bảng managed nằm luôn trên lakehouse.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS silver LOCATION '{LAKE}/silver'")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS gold LOCATION '{LAKE}/gold'")

    print(">>> Dựng tầng Silver...", flush=True)
    stg_orders, stg_events = build_silver(spark)

    print(">>> Dựng bốn bảng chiều SCD2...", flush=True)
    dims = build_dimensions(spark)

    print(">>> Dựng ba bảng fact...", flush=True)
    facts = build_facts(spark, stg_orders, stg_events, dims)

    print(">>> Mirror Gold sang PostgreSQL (cho sơ đồ quan hệ DBeaver)...", flush=True)
    _mirror_to_postgres(dims["customer"], "gold.dim_customer")
    _mirror_to_postgres(dims["restaurant"], "gold.dim_restaurant")
    _mirror_to_postgres(dims["driver"], "gold.dim_driver")
    _mirror_to_postgres(dims["menu_item"], "gold.dim_menu_item")
    _mirror_to_postgres(facts["orders"], "gold.fact_orders")
    _mirror_to_postgres(facts["delivery_events"], "gold.fact_delivery_events")
    _mirror_to_postgres(facts["order_items"], "gold.fact_order_items")

    print(">>> Hoàn tất DP2 build.", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
