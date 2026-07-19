"""
Đo lại chất lượng dữ liệu đã sinh và đối chiếu với cấu hình.

Đây chính là sản phẩm nộp cho rubric: đề bài yêu cầu chụp màn hình output
để tóm tắt chất lượng dữ liệu đã sinh, với các mục cụ thể là phân phối
skew, độ đa dạng giá trị, tỷ lệ null do schema evolution, và tỷ lệ trùng
lặp trước và sau khi khử.

Script đọc thẳng từ file parquet đã ghi, không đọc lại biến trong bộ nhớ,
để đảm bảo con số báo cáo là con số thật sự nằm trên đĩa.

Cách chạy:
    python -m data_generator.profile_output --out ./output
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from data_generator.common.config import load_config


def _read_table(root: Path, table: str) -> pd.DataFrame:
    """Đọc toàn bộ partition của một bảng thành một DataFrame.

    Args:
        root: thư mục gốc chứa dữ liệu.
        table: tên bảng cần đọc.

    Returns:
        DataFrame gộp từ mọi file parquet của bảng đó.
    """
    files = sorted((root / table).rglob("*.parquet"))
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def report_skew(df: pd.DataFrame, col: str, cfg_weights: dict) -> pd.DataFrame:
    """So sánh phân phối thực tế với tỷ lệ đã khai trong cấu hình.

    Cột `diff_pct` cho biết sai lệch giữa thực tế và cấu hình. Sai lệch
    nhỏ, dưới 1 đến 2 điểm phần trăm, là bình thường do lấy mẫu ngẫu
    nhiên. Sai lệch lớn là dấu hiệu generator đang chạy sai.

    Args:
        df: bảng dữ liệu cần đo.
        col: tên cột cần đo phân phối.
        cfg_weights: trọng số kỳ vọng lấy từ cấu hình.

    Returns:
        Bảng so sánh tỷ lệ thực tế và tỷ lệ cấu hình.
    """
    actual = df[col].value_counts(normalize=True).sort_values(ascending=False)
    rows = []
    for key, pct in actual.items():
        expected = cfg_weights.get(key, 0.0)
        rows.append(
            {
                col: key,
                "actual_pct": round(pct * 100, 2),
                "config_pct": round(expected * 100, 2),
                "diff_pct": round((pct - expected) * 100, 2),
            }
        )
    return pd.DataFrame(rows)


def report_cardinality(tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Đo số giá trị phân biệt của các cột khoá quan trọng.

    Mục đích là chỉ ra cột nào thuộc nhóm high cardinality như
    customer_id và menu_item_id, cột nào thuộc nhóm thấp như city và
    vehicle_type. Chính sự tương phản này quyết định cột nào nên phân
    vùng, cột nào nên bucketing, và cột nào nên dùng approx_count_distinct
    thay vì đếm chính xác.

    Args:
        tables: ánh xạ tên bảng sang DataFrame.

    Returns:
        Bảng thống kê số giá trị phân biệt và phân loại HIGH hoặc LOW.
    """
    checks = [
        ("order", "customer_id"),
        ("order", "restaurant_id"),
        ("order", "delivery_city"),
        ("order_item", "menu_item_id"),
        ("order_item", "order_id"),
        ("menu_item", "category"),
    ]
    rows = []
    for table, col in checks:
        if table not in tables or col not in tables[table].columns:
            continue
        s = tables[table][col]
        n_distinct = s.nunique()
        rows.append(
            {
                "table": table,
                "column": col,
                "distinct_count": n_distinct,
                "total_rows": len(s),
                "ratio": round(n_distinct / max(len(s), 1), 4),
                "cardinality": "HIGH" if n_distinct > 1000 else "LOW",
            }
        )
    return pd.DataFrame(rows)


def report_schema_evolution(root: Path) -> pd.DataFrame:
    """Kiểm chứng schema evolution bằng cách đọc schema từng file parquet.

    Điểm mấu chốt: không đọc dữ liệu rồi đếm null, mà đọc thẳng phần mô
    tả schema của từng file. Nếu partition cũ thật sự không có cột
    spice_level thì số cột trong file sẽ là 9 thay vì 10 — đó là bằng
    chứng không thể nguỵ tạo rằng schema đã tiến hoá thật.

    Args:
        root: thư mục gốc chứa dữ liệu.

    Returns:
        Bảng tóm tắt số partition theo từng cấu trúc schema.
    """
    rows = []
    for f in sorted((root / "menu_item").rglob("*.parquet")):
        schema = pq.read_schema(f)
        partition = f.parent.name
        rows.append(
            {
                "partition": partition,
                "n_columns": len(schema.names),
                "has_spice_level": "spice_level" in schema.names,
            }
        )
    df = pd.DataFrame(rows)
    summary = (
        df.groupby(["n_columns", "has_spice_level"])
        .agg(
            n_partitions=("partition", "count"),
            first_partition=("partition", "min"),
            last_partition=("partition", "max"),
        )
        .reset_index()
    )
    return summary


def report_null_after_merge(root: Path) -> pd.DataFrame:
    """Đo tỷ lệ null của cột spice_level khi đọc gộp cả hai phiên bản.

    Mô phỏng lại đúng điều Spark sẽ làm khi bật mergeSchema: đọc tất cả
    file và tự điền null cho những dòng đến từ lô cũ. Kết quả cho thấy
    hai loại null tách bạch nhau:

      - Null do cột chưa tồn tại, ở các partition trước mốc chuyển đổi
      - Null do giá trị không áp dụng, ví dụ món không cay ở partition mới

    Args:
        root: thư mục gốc chứa dữ liệu.

    Returns:
        Bảng thống kê tỷ lệ null theo từng nguồn schema.
    """
    frames = []
    for f in sorted((root / "menu_item").rglob("*.parquet")):
        d = pd.read_parquet(f)
        d["_partition"] = f.parent.name
        if "spice_level" not in d.columns:
            d["spice_level"] = None
            d["_source_schema"] = "v1_no_column"
        else:
            d["_source_schema"] = "v2_has_column"
        frames.append(d)

    merged = pd.concat(frames, ignore_index=True)
    out = (
        merged.groupby("_source_schema")
        .agg(
            rows=("menu_item_id", "count"),
            spice_null=("spice_level", lambda s: int(s.isna().sum())),
        )
        .reset_index()
    )
    out["null_pct"] = (out["spice_null"] / out["rows"] * 100).round(2)
    return out


def report_duplicates(orders: pd.DataFrame, cfg) -> pd.DataFrame:
    """Đo tỷ lệ trùng lặp trước và sau khi khử trùng.

    Việc khử trùng mô phỏng đúng logic sẽ dùng ở Phase 4: nhóm theo khoá
    nghiệp vụ `order_id`, sắp xếp theo `ingested_at` giảm dần, giữ dòng
    đầu tiên.

    Args:
        orders: bảng order đã bao gồm dòng trùng.
        cfg: cấu hình đã nạp, dùng để đối chiếu tỷ lệ kỳ vọng.

    Returns:
        Bảng một dòng tóm tắt số dòng trước, sau và tỷ lệ trùng.
    """
    before = len(orders)
    deduped = orders.sort_values("ingested_at", ascending=False).drop_duplicates(
        subset=["order_id"], keep="first"
    )
    after = len(deduped)
    removed = before - after

    return pd.DataFrame(
        [
            {
                "rows_before_dedup": before,
                "rows_after_dedup": after,
                "duplicates_removed": removed,
                "actual_dup_rate_pct": round(removed / after * 100, 3),
                "config_dup_rate_pct": round(cfg.get("defects.offline_duplicate_rate") * 100, 3),
            }
        ]
    )


def report_defects(
    orders: pd.DataFrame, items: pd.DataFrame, reviews: pd.DataFrame, cfg
) -> pd.DataFrame:
    """Đo các lỗi tham chiếu và lỗi nhất quán đã cố ý cài vào dữ liệu.

    Mỗi dòng tương ứng một phép kiểm tra mà Validate stage ở Phase 3 sẽ
    chạy. Con số thực tế phải xấp xỉ con số trong cấu hình. Nếu lệch
    nhiều thì hoặc generator sai, hoặc bước ghi đã làm mất dữ liệu.

    Args:
        orders: bảng order.
        items: bảng order_item.
        reviews: bảng review.
        cfg: cấu hình đã nạp.

    Returns:
        Bảng liệt kê từng phép kiểm tra và số vi phạm tương ứng.
    """
    deduped = orders.drop_duplicates(subset=["order_id"], keep="first")

    # 1. Kiểm tra total_amount có khớp tổng subtotal không
    calc = items.groupby("order_id", sort=False)["subtotal"].sum()
    joined = deduped.set_index("order_id")["total_amount"].to_frame().join(calc.rename("calc"))
    mismatch_amount = int((joined["total_amount"] != joined["calc"]).sum())

    # 2. Kiểm tra review.restaurant_id có khớp đơn gốc không
    rmap = deduped.set_index("order_id")["restaurant_id"]
    rv = reviews.copy()
    rv["expected"] = rv["order_id"].map(rmap)
    mismatch_review = int((rv["restaurant_id"] != rv["expected"]).sum())

    # 3. Kiểm tra đơn huỷ có lý do không. Đây là ràng buộc có điều kiện
    #    sẽ được viết thành data contract ở Phase 9.
    cancelled = deduped[deduped["status"] == "cancelled"]
    missing_reason = int(cancelled["cancelled_reason_code"].isna().sum())

    return pd.DataFrame(
        [
            {
                "check": "total_amount != SUM(subtotal)",
                "violations": mismatch_amount,
                "actual_pct": round(mismatch_amount / len(deduped) * 100, 3),
                "config_pct": round(cfg.get("defects.amount_mismatch_rate") * 100, 3),
            },
            {
                "check": "review.restaurant_id != order.restaurant_id",
                "violations": mismatch_review,
                "actual_pct": round(mismatch_review / max(len(rv), 1) * 100, 3),
                "config_pct": round(cfg.get("defects.review_restaurant_mismatch") * 100, 3),
            },
            {
                "check": "đơn huỷ thiếu reason_code",
                "violations": missing_reason,
                "actual_pct": round(missing_reason / max(len(cancelled), 1) * 100, 3),
                "config_pct": 0.0,
            },
        ]
    )


def report_quantity_outlier(items: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """Kiểm tra đơn số lượng lớn có đúng xuất hiện theo điều kiện nghiệp vụ.

    Kỳ vọng: các dòng có quantity từ 13 trở lên phải tập trung vào giờ
    trưa ngày thường và thuộc nhóm món cơm, bún hoặc trà sữa. Nếu chúng
    xuất hiện rải rác mọi lúc thì giá trị ngoại lai chỉ là nhiễu ngẫu
    nhiên, không còn ý nghĩa phân tích.

    Args:
        items: bảng order_item.
        orders: bảng order, dùng để lấy thời điểm đặt đơn.

    Returns:
        Bảng một dòng tóm tắt đặc điểm của nhóm đơn số lượng lớn.
    """
    joined = items.merge(
        orders[["order_id", "order_time"]].drop_duplicates("order_id"),
        on="order_id",
        how="left",
    )
    joined["hour"] = pd.to_datetime(joined["order_time"]).dt.hour
    joined["is_weekday"] = pd.to_datetime(joined["order_time"]).dt.dayofweek < 5

    bulk = joined[joined["quantity"] >= 13]
    return pd.DataFrame(
        [
            {
                "bulk_rows": len(bulk),
                "bulk_pct_of_items": round(len(bulk) / len(joined) * 100, 3),
                "pct_in_lunch_hours": round((bulk["hour"].isin([11, 12, 13])).mean() * 100, 2)
                if len(bulk)
                else 0.0,
                "pct_on_weekday": round(bulk["is_weekday"].mean() * 100, 2) if len(bulk) else 0.0,
                "max_quantity": int(joined["quantity"].max()),
            }
        ]
    )


def main() -> None:
    """Đọc dữ liệu đã sinh và in lần lượt 11 bảng báo cáo chất lượng."""
    parser = argparse.ArgumentParser(description="Đo chất lượng dữ liệu đã sinh")
    parser.add_argument("--out", default="./output")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = load_config(args.config)
    root = Path(args.out)

    orders = _read_table(root, "order")
    items = _read_table(root, "order_item")
    reviews = _read_table(root, "review")
    customers = pd.read_parquet(root / "_pg" / "customer.parquet")
    restaurants = pd.read_parquet(root / "_pg" / "restaurant.parquet")
    drivers = pd.read_parquet(root / "_pg" / "driver.parquet")
    menu = _read_table(root, "menu_item")

    tables = {"order": orders, "order_item": items, "menu_item": menu}

    def show(title: str, df: pd.DataFrame) -> None:
        """In một bảng báo cáo kèm tiêu đề phân cách."""
        print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
        print(df.to_string(index=False))

    show(
        "1. SKEW - phân phối thành phố (customer)",
        report_skew(customers, "city", cfg.get("skew.city")),
    )
    show(
        "2. SKEW - phân phối nhóm món (restaurant)",
        report_skew(restaurants, "category", cfg.get("skew.category")),
    )
    show(
        "3. SKEW - loại phương tiện (driver)",
        report_skew(drivers, "vehicle_type", cfg.get("enums.vehicle_type")),
    )
    show(
        "4. SKEW - phương thức thanh toán (order)",
        report_skew(
            orders.drop_duplicates("order_id"),
            "payment_method",
            cfg.get("enums.payment_method"),
        ),
    )
    show(
        "5. SKEW - giờ đặt đơn (order)",
        orders.drop_duplicates("order_id")["order_time"]
        .dt.hour.value_counts(normalize=True)
        .mul(100)
        .round(2)
        .rename("pct")
        .reset_index()
        .rename(columns={"index": "hour"})
        .sort_values("pct", ascending=False)
        .head(10),
    )
    show("6. HIGH CARDINALITY - số giá trị phân biệt", report_cardinality(tables))
    show(
        "7. SCHEMA EVOLUTION - schema thực tế trong file parquet",
        report_schema_evolution(root),
    )
    show(
        "8. SCHEMA EVOLUTION - null sau khi hợp nhất schema",
        report_null_after_merge(root),
    )
    show("9. DUPLICATE - trước và sau khi khử trùng", report_duplicates(orders, cfg))
    show(
        "10. DEFECT - vi phạm ràng buộc tham chiếu và nhất quán",
        report_defects(orders, items, reviews, cfg),
    )
    show("11. OUTLIER - đơn số lượng lớn", report_quantity_outlier(items, orders))


if __name__ == "__main__":
    main()
