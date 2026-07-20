"""
Các hàm nạp dữ liệu từ hệ thống nguồn vào tầng Bronze.

Ba nguồn khác nhau về bản chất nên cần ba cách đọc riêng:

  - MinIO:      tệp parquet chia theo phân vùng ngày, đọc bằng giao thức S3
  - PostgreSQL: bảng quan hệ, đọc bằng truy vấn SQL
  - Kafka:      luồng bản tin JSON, đọc bằng consumer

Nguyên tắc chung của tầng Bronze: **giữ nguyên dữ liệu như nó vốn có**.
Không khử trùng, không sửa lỗi, không chuẩn hoá kiểu dữ liệu. Mọi khiếm
khuyết của nguồn đều được bảo toàn để tầng Silver phía sau xử lý.

Lý do: nếu Bronze đã bị làm sạch thì không còn cách nào truy vết ngược
khi phát hiện logic làm sạch có sai sót. Bronze đóng vai trò bản sao
trung thực của nguồn tại thời điểm nạp.

Riêng ba cột siêu dữ liệu có tiền tố gạch dưới được thêm vào, vì chúng
ghi nhận thông tin về chính lần nạp chứ không sửa đổi dữ liệu nghiệp vụ.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime

from kafka import consumer
import pandas as pd

from dp1.common import (
    BUCKET_BRONZE,
    BUCKET_RAW,
    TableSpec,
    add_ingestion_metadata,
    get_kafka_servers,
    get_postgres_engine,
    get_s3_client,
    list_s3_objects,
    read_parquet_from_s3,
    write_parquet_to_s3,
)

log = logging.getLogger(__name__)

# Số bản tin tối đa đọc từ Kafka trong một lần chạy. Giới hạn này ngăn
# tác vụ chạy vô hạn khi luồng dữ liệu vẫn đang được bơm vào.
KAFKA_MAX_MESSAGES = 200_000

# Thời gian chờ tối đa khi không còn bản tin mới, tính bằng mili giây.
KAFKA_TIMEOUT_MS = 15_000


def ingest_from_minio(spec: TableSpec, **context) -> dict:
    """Nạp một bảng từ MinIO vào tầng Bronze, giữ nguyên cấu trúc phân vùng.

    Đọc từng phân vùng một thay vì gộp toàn bộ vào bộ nhớ. Bảng
    order_item có hơn sáu triệu dòng; đọc gộp sẽ chiếm vài gigabyte và
    có nguy cơ làm vùng chứa Airflow hết bộ nhớ.

    Cách xử lý theo từng phân vùng còn một lợi ích nữa: nếu tác vụ hỏng
    giữa chừng, các phân vùng đã ghi xong vẫn còn nguyên và lần chạy lại
    chỉ cần ghi đè chúng.

    Args:
        spec: mô tả bảng cần nạp.
        context: ngữ cảnh do Airflow truyền vào.

    Returns:
        Từ điển thống kê số phân vùng, số dòng và dung lượng đã ghi.
    """
    client = get_s3_client()
    prefix = f"{spec.source_path}/"

    keys = list_s3_objects(client, BUCKET_RAW, prefix)
    if not keys:
        raise ValueError(
            f"Không tìm thấy tệp nào dưới tiền tố '{prefix}' trong bucket "
            f"'{BUCKET_RAW}'. Kiểm tra lại bộ sinh dữ liệu đã chạy chưa."
        )

    log.info("Bảng %s: tìm thấy %d tệp cần nạp", spec.name, len(keys))

    total_rows = 0
    total_bytes = 0
    partitions: list[str] = []

    for key in keys:
        df = read_parquet_from_s3(client, BUCKET_RAW, key)
        if df.empty:
            continue

        df = add_ingestion_metadata(df, source="minio", table=spec.name)

        # Giữ nguyên tên phân vùng của nguồn. Nhờ vậy Bronze và Raw có
        # cấu trúc thư mục giống hệt nhau, tiện đối chiếu khi cần truy vết.
        partition = key.split("/")[1] if "/" in key else "ingested_date=unknown"
        dest_key = f"{spec.name}/{partition}/part-0000.parquet"

        total_bytes += write_parquet_to_s3(client, df, BUCKET_BRONZE, dest_key)
        total_rows += len(df)
        partitions.append(partition)

    stats = {
        "table": spec.name,
        "source": "minio",
        "partitions": len(partitions),
        "rows": total_rows,
        "size_mb": round(total_bytes / 1024 / 1024, 2),
    }
    log.info("Hoàn tất nạp %s: %s", spec.name, stats)
    return stats


def ingest_from_postgres(spec: TableSpec, **context) -> dict:
    """Nạp một bảng danh mục từ PostgreSQL vào tầng Bronze.

    Ba bảng danh mục có kích thước nhỏ, tổng cộng khoảng một trăm ba
    mươi nghìn dòng, nên đọc trọn vào bộ nhớ được. Với bảng lớn hơn thì
    cần đọc theo lô bằng tham số chunksize.

    Khác với dữ liệu từ MinIO, các bảng này không có sẵn phân vùng theo
    ngày. Chúng được ghi thành một phân vùng duy nhất mang ngày chạy của
    luồng, phản ánh đúng bản chất là ảnh chụp trạng thái tại thời điểm nạp.

    Args:
        spec: mô tả bảng cần nạp.
        context: ngữ cảnh do Airflow truyền vào.

    Returns:
        Từ điển thống kê kết quả nạp.
    """
    engine = get_postgres_engine()
    client = get_s3_client()

    log.info("Bảng %s: đang đọc từ %s", spec.name, spec.source_path)
    df = pd.read_sql(f"SELECT * FROM {spec.source_path}", engine)

    if df.empty:
        raise ValueError(
            f"Bảng nguồn {spec.source_path} rỗng. Kiểm tra bước nạp dữ liệu "
            f"vào PostgreSQL đã chạy chưa."
        )

    df = add_ingestion_metadata(df, source="postgres", table=spec.name)

    # Ngày chạy theo lịch của Airflow, không phải ngày hiện tại. Nhờ vậy
    # khi chạy bù cho một ngày trong quá khứ, dữ liệu vẫn rơi đúng phân vùng.
    logical_date = context.get("logical_date") or datetime.utcnow()
    partition = f"ingested_date={logical_date:%Y-%m-%d}"
    dest_key = f"{spec.name}/{partition}/part-0000.parquet"

    size = write_parquet_to_s3(client, df, BUCKET_BRONZE, dest_key)

    stats = {
        "table": spec.name,
        "source": "postgres",
        "partitions": 1,
        "rows": len(df),
        "size_mb": round(size / 1024 / 1024, 2),
    }
    log.info("Hoàn tất nạp %s: %s", spec.name, stats)
    return stats


def ingest_from_kafka(spec: TableSpec, **context) -> dict:
    """Nạp bản tin từ một topic Kafka vào tầng Bronze.

    Đọc từ đầu hàng đợi với một nhóm tiêu thụ cố định. Nhóm riêng cho
    luồng nạp giúp vị trí đọc không lẫn với các tiến trình khác cũng
    đang tiêu thụ cùng topic.

    Bản tin được giữ nguyên như nhận được, kể cả những bản trùng lặp và
    những bản đến muộn. Đây là điểm cốt lõi của tầng Bronze: chính vì
    giữ nguyên khiếm khuyết mà bước xử lý luồng phía sau mới có dữ liệu
    thật để chứng minh cơ chế khử trùng và xử lý dữ liệu đến muộn hoạt
    động đúng.

    Dữ liệu được chia phân vùng theo ngày của thời điểm sự kiện xảy ra,
    không phải thời điểm bản tin tới nơi. Nhờ vậy một sự kiện đến muộn
    vẫn rơi đúng phân vùng của ngày nó xảy ra.

    Args:
        spec: mô tả bảng cần nạp.
        context: ngữ cảnh do Airflow truyền vào.

    Returns:
        Từ điển thống kê kết quả nạp.
    """
    from kafka import KafkaConsumer, TopicPartition

    servers = get_kafka_servers()
    client = get_s3_client()

    log.info("Đang đọc topic %s từ %s", spec.source_path, servers)

    consumer = KafkaConsumer(
        bootstrap_servers=servers.split(","),
        enable_auto_commit=False,
        consumer_timeout_ms=KAFKA_TIMEOUT_MS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    # Tự gán toàn bộ phân vùng của topic thay vì dùng cơ chế đăng ký theo
    # nhóm tiêu thụ. Nhóm tiêu thụ ghi nhớ vị trí đã đọc, nên lần chạy thứ
    # hai sẽ không thấy bản tin nào, trong khi luồng nạp cần đọc lại toàn
    # bộ topic mỗi lần chạy.
    partition_ids = consumer.partitions_for_topic(spec.source_path)
    if not partition_ids:
        raise ValueError(
            f"Topic '{spec.source_path}' không tồn tại hoặc chưa có phân vùng nào."
        )

    partitions = [TopicPartition(spec.source_path, p) for p in sorted(partition_ids)]
    consumer.assign(partitions)
    consumer.seek_to_beginning(*partitions)

    log.info("Đã gán %d phân vùng của topic %s", len(partitions), spec.source_path)
    records: list[dict] = []
    for msg in consumer:
        rec = dict(msg.value)
        # Ghi lại vị trí bản tin trên hàng đợi. Thông tin này cho phép
        # đọc lại đúng bản tin đó khi cần điều tra một sự kiện cụ thể.
        rec["_kafka_partition"] = msg.partition
        rec["_kafka_offset"] = msg.offset
        records.append(rec)
        if len(records) >= KAFKA_MAX_MESSAGES:
            break

    consumer.close()

    if not records:
        raise ValueError(
            f"Không đọc được bản tin nào từ topic '{spec.source_path}'. "
            f"Kiểm tra bộ sinh dữ liệu luồng đã bơm chưa."
        )

    df = pd.DataFrame(records)
    df = add_ingestion_metadata(df, source="kafka", table=spec.name)

    # Chia phân vùng theo ngày của thời điểm sự kiện xảy ra. Với sự kiện
    # đến muộn, cách này đảm bảo nó vẫn nằm cùng phân vùng với các sự
    # kiện cùng ngày, thay vì rơi vào ngày nạp.
    df["_event_date"] = pd.to_datetime(df["event_time"]).dt.date.astype(str)

    total_bytes = 0

    for event_date, group in df.groupby("_event_date", sort=True):
        dest_key = f"{spec.name}/ingested_date={event_date}/part-0000.parquet"
        total_bytes += write_parquet_to_s3(
            client, group.drop(columns=["_event_date"]), BUCKET_BRONZE, dest_key
        )

    stats = {
        "table": spec.name,
        "source": "kafka",
        "partitions": partitions,
        "rows": len(df),
        "size_mb": round(total_bytes / 1024 / 1024, 2),
    }
    log.info("Hoàn tất nạp %s: %s", spec.name, stats)
    return stats


def ingest_table(spec: TableSpec, **context) -> dict:
    """Điều hướng tới hàm nạp phù hợp theo loại nguồn.

    Args:
        spec: mô tả bảng cần nạp.
        context: ngữ cảnh do Airflow truyền vào.

    Returns:
        Từ điển thống kê kết quả nạp.

    Raises:
        ValueError: khi loại nguồn không được hỗ trợ.
    """
    handlers = {
        "minio": ingest_from_minio,
        "postgres": ingest_from_postgres,
        "kafka": ingest_from_kafka,
    }
    handler = handlers.get(spec.source)
    if handler is None:
        raise ValueError(f"Loại nguồn không được hỗ trợ: {spec.source}")
    return handler(spec, **context)
