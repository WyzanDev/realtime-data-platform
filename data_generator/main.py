"""
Điểm vào của bộ sinh dữ liệu.

Cách chạy:
    python -m data_generator.main --mode offline
    python -m data_generator.main --mode offline --scale 0.01
    python -m data_generator.main --mode offline --upload
    python -m data_generator.main --mode upload

Tham số --scale nhân toàn bộ khối lượng với một hệ số, dùng khi cần chạy
thử nhanh mà vẫn giữ nguyên mọi tỷ lệ skew và tỷ lệ lỗi. Các con số thống
kê chất lượng dữ liệu đo được ở bản thu nhỏ vẫn phải khớp với cấu hình —
đây là cách kiểm tra generator hoạt động đúng trước khi chạy bản đầy đủ.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import pandas as pd

from data_generator.common.config import load_config
from data_generator.offline import dimensions, facts, writer


def scale_volume(cfg, factor: float) -> None:
    """Nhân các tham số khối lượng với một hệ số, giữ nguyên mọi tỷ lệ khác.

    Args:
        cfg: cấu hình đã nạp, sẽ bị sửa tại chỗ.
        factor: hệ số thu nhỏ, ví dụ 0.01 nghĩa là chạy 1% khối lượng.
    """
    if factor == 1.0:
        return
    vol = cfg.raw["volume"]
    for key in ("n_customers", "n_restaurants", "n_menu_items", "n_drivers", "n_orders"):
        vol[key] = max(int(vol[key] * factor), 10)


def run_offline(cfg, out_root: Path) -> dict:
    """Sinh toàn bộ dữ liệu offline và ghi ra thư mục tạm.

    Thứ tự sinh bắt buộc phải theo đúng phụ thuộc khoá ngoại: nhóm
    dimension trước, rồi order, rồi order_item vì cần cả order lẫn
    menu_item, cuối cùng là review vì cần order đã hoàn tất. Dữ liệu
    trùng được tiêm sau cùng để không làm sai lệch phép tính total_amount.

    Args:
        cfg: cấu hình đã nạp.
        out_root: thư mục ghi kết quả.

    Returns:
        Từ điển thống kê số dòng của từng bảng và thời gian chạy.
    """
    t0 = time.time()

    print("[1/6] Đang sinh bảng customer ...")
    customers = dimensions.generate_customers(cfg)

    print("[2/6] Đang sinh bảng restaurant ...")
    restaurants = dimensions.generate_restaurants(cfg)

    print("[3/6] Đang sinh bảng driver ...")
    drivers = dimensions.generate_drivers(cfg)

    print("[4/6] Đang sinh bảng menu_item với hai phiên bản schema ...")
    mi_v1, mi_v2 = dimensions.generate_menu_items(cfg, restaurants)

    print("[5/6] Đang sinh bảng order và order_item ...")
    # Gộp hai phiên bản lại chỉ để phục vụ việc join khi sinh order_item.
    # Bản ghi xuống đĩa vẫn tách riêng hai lô để giữ schema khác nhau.
    menu_all = pd.concat([mi_v1.assign(spice_level=None), mi_v2], ignore_index=True)
    orders = facts.generate_orders(cfg, customers, restaurants, drivers)
    items, orders = facts.generate_order_items(cfg, orders, menu_all)

    print("[6/6] Đang sinh bảng review và tiêm dữ liệu trùng ...")
    reviews = facts.generate_reviews(cfg, orders)
    orders_dup = facts.inject_duplicates(cfg, orders)

    # --- Ghi ra đĩa ---
    out_root.mkdir(parents=True, exist_ok=True)
    evo = writer.write_menu_item_two_versions(mi_v1, mi_v2, out_root)
    writer.write_parquet_partitioned(orders_dup, "order", out_root)
    writer.write_parquet_partitioned(items, "order_item", out_root)
    writer.write_parquet_partitioned(reviews, "review", out_root)

    # Ba bảng danh mục ghi ra parquet tạm. Bước đẩy lên PostgreSQL tách
    # riêng để có thể chạy lại độc lập khi container cơ sở dữ liệu chưa
    # sẵn sàng.
    for name, df in (("customer", customers), ("restaurant", restaurants), ("driver", drivers)):
        (out_root / "_pg").mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_root / "_pg" / f"{name}.parquet", index=False)

    return {
        "customers": len(customers),
        "restaurants": len(restaurants),
        "drivers": len(drivers),
        "menu_items_v1": len(mi_v1),
        "menu_items_v2": len(mi_v2),
        "orders_before_dup": len(orders),
        "orders_after_dup": len(orders_dup),
        "order_items": len(items),
        "reviews": len(reviews),
        "schema_evolution": evo,
        "elapsed_sec": round(time.time() - t0, 1),
    }


def run_upload(cfg, out_root: Path) -> dict:
    """Đẩy dữ liệu từ thư mục tạm lên hai hệ thống nguồn.

    Tách riêng khỏi bước sinh dữ liệu để có thể chạy lại độc lập khi
    container chưa sẵn sàng, hoặc khi cần nạp lại mà không muốn sinh lại
    từ đầu — sinh lại sẽ tốn hàng chục phút ở khối lượng đầy đủ.

    Ba bảng danh mục vào PostgreSQL, phần còn lại vào MinIO, mô phỏng dữ
    liệu nằm ở hai hệ thống khác nhau của doanh nghiệp.

    Args:
        cfg: cấu hình đã nạp.
        out_root: thư mục tạm chứa dữ liệu đã sinh.

    Returns:
        Từ điển kết quả gồm số dòng đã nạp và số file đã tải lên.
    """
    stats: dict = {}

    print("\n[Tải lên 1/2] PostgreSQL — các bảng danh mục ...")
    pg_tables = {
        name: pd.read_parquet(out_root / "_pg" / f"{name}.parquet")
        for name in ("customer", "restaurant", "driver")
    }
    stats["postgres"] = writer.write_to_postgres(pg_tables, cfg)

    print("\n[Tải lên 2/2] MinIO — các bảng fact và menu_item ...")
    stats["minio_files"] = writer.upload_to_minio(out_root, cfg)

    return stats


def run_streaming(cfg, args) -> None:
    """Chạy mô phỏng luồng sự kiện và bơm vào Kafka.

    Args:
        cfg: cấu hình đã nạp.
        args: tham số dòng lệnh đã phân tích.
    """
    from data_generator.streaming import producer

    stats = producer.run_stream(
        cfg,
        minutes=args.minutes,
        dry_run=args.dry_run,
        realtime=args.realtime,
        start_hour=args.start_hour,
    )
    print(stats.summary(cfg))


def main() -> None:
    """Phân tích tham số dòng lệnh và điều phối các chế độ chạy."""
    parser = argparse.ArgumentParser(description="Bộ sinh dữ liệu food delivery")
    parser.add_argument(
        "--mode",
        choices=["offline", "streaming", "upload"],
        required=True,
        help="offline: sinh dữ liệu | upload: đẩy lên MinIO và Postgres | "
             "streaming: bơm vào Kafka",
    )
    parser.add_argument("--config", default=None, help="đường dẫn file YAML")
    parser.add_argument("--scale", type=float, default=1.0, help="hệ số thu nhỏ khối lượng")
    parser.add_argument("--out", default="./output", help="thư mục tạm")
    parser.add_argument(
        "--upload",
        action="store_true",
        help="chạy liền bước tải lên ngay sau khi sinh xong, dùng với mode offline",
    )
    # --- Các tham số riêng cho chế độ streaming ---
    parser.add_argument(
        "--minutes",
        type=int,
        default=960,
        help="số phút mô phỏng cần sinh, mặc định 960 tức 16 giờ từ 6h tới 22h",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="chỉ sinh và thống kê, không kết nối Kafka",
    )
    parser.add_argument(
        "--realtime",
        action="store_true",
        help="chờ giữa các phút để mô phỏng tốc độ thật thay vì tua nhanh",
    )
    parser.add_argument(
        "--start-hour",
        type=int,
        default=None,
        help="giờ bắt đầu mô phỏng, mặc định 6 giờ sáng",
    )
    args = parser.parse_args()

    cfg = load_config(args.config)
    scale_volume(cfg, args.scale)

    if args.mode == "offline":
        stats = run_offline(cfg, Path(args.out))
        print("\n=== Thống kê sinh dữ liệu ===")
        for k, v in stats.items():
            print(f"  {k}: {v}")

        if args.upload:
            up = run_upload(cfg, Path(args.out))
            print("\n=== Kết quả tải lên ===")
            for k, v in up.items():
                print(f"  {k}: {v}")

    elif args.mode == "upload":
        up = run_upload(cfg, Path(args.out))
        print("\n=== Kết quả tải lên ===")
        for k, v in up.items():
            print(f"  {k}: {v}")

    else:
        run_streaming(cfg, args)


if __name__ == "__main__":
    main()
