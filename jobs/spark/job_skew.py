"""
Spark job — xử lý dữ liệu lệch (skew) theo thành phố giao hàng.

Bối cảnh vấn đề
---------------
Bảng `raw_orders` có cột `delivery_city` phân phối cực lệch: TP.HCM chiếm
khoảng 44% tổng đơn, Hà Nội 30%, phần còn lại là đuôi dài.

Khi nối bảng đơn với một bảng chiều theo thành phố bằng sort-merge join,
Spark băm cả hai bên theo `delivery_city`. Toàn bộ đơn TP.HCM dồn về **một**
partition ở phía reduce, nên task xử lý partition đó phải làm việc trên hơn
một triệu dòng trong khi các task khác gần như rảnh. Nó thành straggler kéo
dài cả stage; trên cụm nhiều nhân, các nhân còn lại ngồi không chờ nó.

Vì sao dùng phép JOIN để minh hoạ
---------------------------------
Phép join buộc phải shuffle theo khoá, và mọi biến đổi sau join (ở đây là
băm) chạy ở phía reduce — đúng trên các partition đã lệch. Catalyst không
thể đẩy phần việc đó ngược lên phía đọc nguồn như khi ta chỉ repartition rồi
đếm toàn cục. Nhờ vậy skew hiện đúng bản chất và không bị tối ưu hoá che đi.

Để chi phí lệch nổi rõ, mỗi đơn được gắn một **mã toàn vẹn** bằng băm lặp
(mô phỏng bước làm giàu tốn CPU có thật: checksum, đặc trưng cho từng đơn).
Chính công việc per-row này biến mất cân bằng số dòng thành mất cân bằng
thời gian.

Cách quan sát trên Spark UI
---------------------------
Ở stage sort-merge join, cột "Duration" của task lớn nhất lệch hẳn so với
trung vị — hình ảnh straggler do skew.

Cách xử lý: salting
-------------------
Thêm cột muối `_salt` trị 0..N-1 vào khoá join ở cả hai bên: đơn được gắn
muối ngẫu nhiên, bảng chiều được nhân bản thành N bản mỗi bản một muối. Đơn
TP.HCM được rải đều ra N partition thay vì dồn một chỗ, nên công việc chia
đều cho mọi nhân. Kết quả không đổi vì mỗi đơn vẫn khớp đúng bản ghi chiều
của thành phố nó, chỉ khác qua bản sao muối nào.

Cách chạy
---------
    spark-submit job_skew.py --mode baseline
    spark-submit job_skew.py --mode salted --salt 8
"""

from __future__ import annotations

import argparse

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from common import build_spark, read_bronze, timed

# Số vòng băm lặp cho mỗi đơn. Đủ lớn để công việc per-row nổi trội hơn chi
# phí đọc/ghi, nhờ vậy sự lệch số dòng chuyển thành lệch thời gian thấy rõ.
HASH_ROUNDS = 100

# Bảng chiều theo thành phố: mỗi thành phố kèm một hệ số phụ phí. Cố ý để nhỏ
# và cố định trong mã — điểm mấu chốt không nằm ở nội dung bảng chiều mà ở
# việc phép join bị lệch theo khoá thành phố.
CITY_SURCHARGE = [
    ("Ho Chi Minh", 0.10),
    ("Ha Noi", 0.08),
    ("Da Nang", 0.05),
    ("Hai Phong", 0.04),
    ("Can Tho", 0.03),
    ("Bien Hoa", 0.03),
    ("Nha Trang", 0.03),
    ("Hue", 0.02),
]


def _integrity_hash(base: Column) -> Column:
    """Băm lặp một cột để mô phỏng bước làm giàu dữ liệu tốn CPU trên mỗi đơn.

    Mỗi vòng băm SHA-256 kết quả của vòng trước. Cố tình dùng hàm dựng sẵn
    của Spark (chạy trong JVM) thay vì UDF Python, để tránh chi phí tuần tự
    hoá và giữ cho phép đo phản ánh đúng công việc tính toán.

    Args:
        base: cột chuỗi đầu vào.

    Returns:
        Cột chuỗi băm sau HASH_ROUNDS vòng.
    """
    col = F.sha2(base.cast("string"), 256)
    for _ in range(HASH_ROUNDS - 1):
        col = F.sha2(col, 256)
    return col


def _city_dim(spark: SparkSession) -> DataFrame:
    """Tạo bảng chiều thành phố từ hằng số trong mã.

    Args:
        spark: phiên Spark đang chạy.

    Returns:
        DataFrame hai cột `delivery_city` và `surcharge_rate`.
    """
    return spark.createDataFrame(CITY_SURCHARGE, ["delivery_city", "surcharge_rate"])


def _join_enrich_count(orders: DataFrame, dim: DataFrame, join_cols: list[str]) -> int:
    """Nối đơn với bảng chiều, gắn mã toàn vẹn cho từng đơn rồi đếm.

    Phép băm nằm sau phép join nên chạy ở phía reduce — đúng trên các partition
    đã băm theo thành phố. Đó là nơi skew biểu hiện. Dùng đếm toàn cục làm
    action để buộc mọi dòng chạy qua phép băm mà không phải ghi file.

    Args:
        orders: bảng đơn đã gắn khoá join.
        dim: bảng chiều đã gắn khoá join.
        join_cols: danh sách cột khoá join.

    Returns:
        Số dòng đã xử lý.
    """
    joined = orders.join(dim, on=join_cols, how="inner")
    enriched = joined.withColumn(
        "_integrity",
        _integrity_hash(
            F.concat_ws("|", F.col("order_id"), F.col("total_amount"), F.col("surcharge_rate"))
        ),
    )
    return enriched.filter(F.col("_integrity").isNotNull()).count()


def run_baseline(spark: SparkSession) -> None:
    """Nối theo delivery_city bằng sort-merge join — để lộ straggler do skew.

    Args:
        spark: phiên Spark đang chạy.
    """
    orders = read_bronze(spark, "raw_orders").select(
        "order_id", "delivery_city", "total_amount"
    )
    dim = _city_dim(spark)

    with timed("baseline: sort-merge join theo delivery_city (skew)"):
        n = _join_enrich_count(orders, dim, ["delivery_city"])

    print(f"Đã xử lý {n:,} đơn.", flush=True)


def run_salted(spark: SparkSession, n_salt: int) -> None:
    """Nối theo (thành phố, muối) để rải đều tải của thành phố lệch.

    Args:
        spark: phiên Spark đang chạy.
        n_salt: số bản muối; nên xấp xỉ số nhân của cụm để mỗi nhân nhận một
            phần đều nhau của thành phố lớn nhất.
    """
    orders = read_bronze(spark, "raw_orders").select(
        "order_id", "delivery_city", "total_amount"
    )
    dim = _city_dim(spark)

    with timed(f"salted: sort-merge join theo (delivery_city, salt), N={n_salt}"):
        # Gắn muối ngẫu nhiên vào mỗi đơn.
        orders_salted = orders.withColumn("_salt", (F.rand(seed=42) * n_salt).cast("int"))
        # Nhân bản bảng chiều thành N bản, mỗi bản một giá trị muối.
        salt_range = spark.range(n_salt).withColumnRenamed("id", "_salt")
        dim_salted = dim.crossJoin(salt_range)
        n = _join_enrich_count(orders_salted, dim_salted, ["delivery_city", "_salt"])

    print(f"Đã xử lý {n:,} đơn.", flush=True)


def main() -> None:
    """Điểm vào: đọc tham số dòng lệnh và chạy đúng chế độ."""
    parser = argparse.ArgumentParser(description="Xử lý skew theo thành phố")
    parser.add_argument("--mode", choices=["baseline", "salted"], required=True)
    parser.add_argument("--salt", type=int, default=8, help="Số bản muối cho chế độ salted")
    args = parser.parse_args()

    # Tắt AQE để skew hiện nguyên trạng; tắt broadcast để ép sort-merge join.
    # Nếu để broadcast, bảng chiều bé xíu sẽ được phát tới mọi executor và
    # không có shuffle nào để mà lệch — che mất vấn đề cần minh hoạ.
    spark = build_spark(
        f"phase4_skew_{args.mode}",
        extra_conf={
            "spark.sql.adaptive.enabled": "false",
            "spark.sql.autoBroadcastJoinThreshold": "-1",
            "spark.sql.shuffle.partitions": "48",
        },
    )

    if args.mode == "baseline":
        run_baseline(spark)
    else:
        run_salted(spark, args.salt)

    spark.stop()


if __name__ == "__main__":
    main()
