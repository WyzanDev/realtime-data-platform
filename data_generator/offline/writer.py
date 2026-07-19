"""
Ghi dữ liệu offline ra hai đích: MinIO dạng parquet và PostgreSQL dạng bảng.

Việc tách làm hai đích là có chủ ý theo yêu cầu đề bài — mô phỏng tình
huống dữ liệu đang nằm rải rác ở các hệ thống khác nhau của công ty:

  - PostgreSQL: customer, restaurant, driver
    Các bảng danh mục, thay đổi chậm, thường nằm trong cơ sở dữ liệu
    nghiệp vụ của một phòng ban khác. Nạp về bằng JDBC.

  - MinIO: menu_item, order, order_item, review
    Dữ liệu lớn, ghi theo lô, lưu dưới dạng file. Nạp về bằng S3 API.

Nhờ hai nguồn khác loại, pipeline DP1 ở Phase 3 có hai nhánh ingest thật
sự khác nhau về kỹ thuật thay vì lặp lại cùng một cách đọc.
"""

from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

from data_generator.common.config import Config


def _partition_path(table: str, ingested_date: str, bucket_prefix: str) -> str:
    """Dựng đường dẫn partition theo quy ước Hive.

    Ví dụ: raw/order/ingested_date=2026-03-14/part-0000.parquet

    Quy ước `cột=giá_trị` trong tên thư mục cho phép Spark tự nhận ra cột
    phân vùng khi đọc, và bỏ qua các thư mục không liên quan khi truy vấn
    có điều kiện lọc theo ngày.

    Args:
        table: tên bảng.
        ingested_date: ngày nạp dạng chuỗi YYYY-MM-DD.
        bucket_prefix: tiền tố bucket hoặc thư mục gốc.

    Returns:
        Chuỗi đường dẫn đầy đủ tới file parquet.
    """
    return f"{bucket_prefix}/{table}/ingested_date={ingested_date}/part-0000.parquet"


def write_parquet_partitioned(
    df: pd.DataFrame,
    table: str,
    out_root: Path,
    compression: str = "snappy",
    date_col: str = "ingested_at",
) -> list[Path]:
    """Ghi DataFrame thành nhiều file parquet, chia theo ngày nạp.

    Mỗi ngày một thư mục riêng. Cách này quan trọng với cả ba phase sau:
    Phase 3 đọc theo partition, Phase 4 dùng partition pruning để đo hiệu
    năng, Phase 6 đếm số lượng và kích thước file để tối ưu.

    Args:
        df: dữ liệu cần ghi.
        table: tên bảng, dùng làm thư mục cấp một.
        out_root: thư mục gốc, đóng vai trò vùng tạm trước khi đẩy lên MinIO.
        compression: thuật toán nén, mặc định snappy — cân bằng giữa tốc
            độ giải nén và tỷ lệ nén, phù hợp cho tầng Bronze.
        date_col: cột thời gian dùng để chia partition.

    Returns:
        Danh sách đường dẫn file đã ghi.
    """
    written: list[Path] = []
    tmp = df.copy()
    tmp["_pdate"] = pd.to_datetime(tmp[date_col]).dt.date.astype(str)

    for pdate, group in tmp.groupby("_pdate", sort=True):
        target = out_root / table / f"ingested_date={pdate}"
        target.mkdir(parents=True, exist_ok=True)
        path = target / "part-0000.parquet"
        group.drop(columns=["_pdate"]).to_parquet(path, index=False, compression=compression)
        written.append(path)
    return written


def write_menu_item_two_versions(
    df_v1: pd.DataFrame, df_v2: pd.DataFrame, out_root: Path, compression: str = "snappy"
) -> dict[str, int]:
    """Ghi bảng menu_item thành hai lô parquet có số cột khác nhau.

    Đây là bước quyết định tính đúng đắn của phần schema evolution. Hai
    lô được ghi bằng hai lệnh `to_parquet` độc lập:

      - df_v1: không có cột spice_level, file parquet chỉ lưu 9 cột
      - df_v2: có cột spice_level, file parquet lưu 10 cột

    Vì Parquet nhúng schema vào trong từng file, hai thư mục partition
    thật sự khác nhau về cấu trúc. Khi Spark đọc cả hai với tuỳ chọn
    mergeSchema bật, nó sẽ hợp nhất thành 10 cột và tự điền null cho các
    dòng đến từ lô cũ.

    Nếu thay vào đó ta tạo sẵn cột rồi để null cho lô cũ, file parquet
    vẫn chứa đủ 10 cột và mergeSchema sẽ không còn tác dụng gì.

    Args:
        df_v1: lô dữ liệu theo schema phiên bản 1.
        df_v2: lô dữ liệu theo schema phiên bản 2.
        out_root: thư mục gốc.
        compression: thuật toán nén.

    Returns:
        Thống kê số file và số cột của từng phiên bản.
    """
    n1 = len(write_parquet_partitioned(df_v1, "menu_item", out_root, compression))
    n2 = len(write_parquet_partitioned(df_v2, "menu_item", out_root, compression))
    return {
        "v1_files": n1,
        "v2_files": n2,
        "v1_cols": len(df_v1.columns),
        "v2_cols": len(df_v2.columns),
    }


def upload_to_minio(local_root: Path, cfg: Config) -> int:
    """Đẩy toàn bộ cây thư mục parquet lên MinIO, giữ nguyên cấu trúc.

    Dùng boto3 với endpoint trỏ về MinIO. Thông tin kết nối lấy từ biến
    môi trường đã khai trong file .env ở Phase 0, không hardcode trong mã.

    Args:
        local_root: thư mục gốc chứa các file parquet.
        cfg: cấu hình đã nạp.

    Returns:
        Số đối tượng đã tải lên.

    Raises:
        RuntimeError: khi thiếu biến môi trường chứa thông tin đăng nhập.
    """
    import boto3
    from botocore.exceptions import ClientError

    # Khi chạy từ máy host, tên "minio" trong mạng nội bộ của Docker
    # Compose không phân giải được, nên cho phép ghi đè bằng biến môi trường.
    endpoint = os.environ.get("MINIO_ENDPOINT", cfg.get("sinks.minio.endpoint"))
    bucket = cfg.get("sinks.minio.bucket")

    user = os.environ.get("MINIO_ROOT_USER") or os.environ.get("MINIO_ACCESS_KEY")
    pwd = os.environ.get("MINIO_ROOT_PASSWORD") or os.environ.get("MINIO_SECRET_KEY")
    if not user or not pwd:
        raise RuntimeError(
            "Thiếu MINIO_ROOT_USER hoặc MINIO_ROOT_PASSWORD. "
            "Nạp file .env trước khi chạy: set -a && source .env && set +a"
        )

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=user,
        aws_secret_access_key=pwd,
    )

    # Bucket đã được init container tạo ở Phase 0, nhưng vẫn kiểm tra lại
    # để script chạy độc lập được.
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)

    files = sorted(local_root.rglob("*.parquet"))
    # Bỏ qua thư mục _pg: đây là bản tạm của các bảng sẽ nạp vào
    # PostgreSQL, không thuộc phạm vi lưu trên object storage.
    files = [f for f in files if "_pg" not in f.parts]

    count = 0
    for path in files:
        key = str(path.relative_to(local_root))
        client.upload_file(str(path), bucket, key)
        count += 1
        if count % 100 == 0:
            print(f"    ... đã tải lên {count}/{len(files)} file")
    return count


def write_to_postgres(tables: dict[str, pd.DataFrame], cfg: Config) -> dict[str, int]:
    """Ghi các bảng danh mục vào PostgreSQL.

    Dùng `to_sql` với phương thức chèn theo lô. Schema đích được tạo
    trước nếu chưa tồn tại. Đặt riêng một schema `source_system` để tách
    bạch rõ ràng: đây là dữ liệu giả lập từ hệ thống nguồn, không phải
    bảng của kho dữ liệu.

    Args:
        tables: ánh xạ tên bảng sang DataFrame tương ứng.
        cfg: cấu hình đã nạp.

    Returns:
        Số dòng đã ghi cho từng bảng.

    Raises:
        RuntimeError: khi thiếu biến môi trường chứa thông tin đăng nhập.
    """
    from sqlalchemy import create_engine, text

    host = os.environ.get("POSTGRES_HOST", cfg.get("sinks.postgres.host"))
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = cfg.get("sinks.postgres.database")
    schema = cfg.get("sinks.postgres.schema")

    user = os.environ.get("POSTGRES_USER")
    pwd = os.environ.get("POSTGRES_PASSWORD")
    if not user or not pwd:
        raise RuntimeError(
            "Thiếu POSTGRES_USER hoặc POSTGRES_PASSWORD. "
            "Nạp file .env trước khi chạy: set -a && source .env && set +a"
        )

    engine = create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    # Khoá chính của từng bảng, dùng để tạo chỉ mục sau khi nạp. Không có
    # chỉ mục thì bước ingest ở Phase 3 phải quét toàn bảng.
    pk_map = {
        "customer": "customer_id",
        "restaurant": "restaurant_id",
        "driver": "driver_id",
    }

    result: dict[str, int] = {}
    for name, df in tables.items():
        print(f"    ... đang nạp {name} ({len(df):,} dòng)")
        df.to_sql(
            name,
            engine,
            schema=schema,
            if_exists="replace",
            index=False,
            chunksize=10_000,
            method="multi",
        )
        pk = pk_map.get(name)
        if pk:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"CREATE INDEX IF NOT EXISTS idx_{name}_{pk} "
                        f"ON {schema}.{name} ({pk})"
                    )
                )
        result[name] = len(df)
    return result
