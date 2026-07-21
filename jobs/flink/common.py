"""
Tiện ích dùng chung cho các PyFlink job xử lý luồng sự kiện giao hàng.

Module gom ba việc mọi job đều cần:

  1. Dựng nguồn Kafka đọc topic `gps-topic`.
  2. Giải mã bản tin JSON thành Row có kiểu rõ ràng để dùng cho keyBy/window.
  3. Tính khoảng cách địa lý giữa hai toạ độ (phục vụ ước lượng tốc độ).

Sự kiện GPS có hai mốc thời gian tách bạch, đây là điểm mấu chốt của toàn
bộ Phase 5:

  - `event_time`: lúc sự kiện thật sự xảy ra ngoài đời — dùng cho watermark
    và chia cửa sổ.
  - `sent_at`:    lúc bản tin được đẩy vào Kafka — với sự kiện đến muộn, nó
    lệch xa `event_time` tới vài phút.

Không viết cứng địa chỉ Kafka: đọc từ biến môi trường `KAFKA_BOOTSTRAP`,
Airflow hoặc người chạy tay truyền vào lúc submit.
"""

from __future__ import annotations

import json
import math
import os

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

GPS_TOPIC = "gps-topic"

# Thứ tự và kiểu của các trường trong Row sau khi giải mã. Khai báo tường
# minh để PyFlink truyền kiểu qua ranh giới Java–Python cho keyBy và window.
EVENT_FIELD_NAMES = [
    "event_id",
    "order_id",
    "driver_id",
    "event_type",
    "lat",
    "lon",
    "event_time_ms",
    "city",
    "seq",
]
EVENT_FIELD_TYPES = [
    Types.STRING(),   # event_id
    Types.STRING(),   # order_id
    Types.STRING(),   # driver_id
    Types.STRING(),   # event_type
    Types.DOUBLE(),   # lat
    Types.DOUBLE(),   # lon
    Types.LONG(),     # event_time_ms — epoch millisecond, dùng cho watermark
    Types.STRING(),   # city
    Types.INT(),      # seq — sequence_number trong chuyến
]


def event_row_type():
    """Trả về kiểu Row của một sự kiện đã giải mã.

    Returns:
        Types.ROW_NAMED mô tả toàn bộ trường và kiểu.
    """
    return Types.ROW_NAMED(EVENT_FIELD_NAMES, EVENT_FIELD_TYPES)


def _iso_to_millis(iso: str) -> int:
    """Đổi chuỗi thời gian ISO sang epoch millisecond.

    Không dùng thư viện ngoài để hàm chạy được trên TaskManager mà không cần
    cài thêm gói. Chuỗi có dạng `2026-07-21T06:00:01`.

    Args:
        iso: chuỗi thời gian ISO 8601 không kèm múi giờ.

    Returns:
        Số mili giây kể từ epoch.
    """
    from datetime import datetime

    return int(datetime.fromisoformat(iso).timestamp() * 1000)


def parse_event(raw: str):
    """Giải mã một bản tin JSON thành Row sự kiện.

    Args:
        raw: chuỗi JSON một bản tin Kafka.

    Returns:
        Row đúng thứ tự EVENT_FIELD_NAMES.
    """
    from pyflink.common import Row

    d = json.loads(raw)
    return Row(
        d["event_id"],
        d["order_id"],
        d["driver_id"],
        d["event_type"],
        float(d["lat"]),
        float(d["lon"]),
        _iso_to_millis(d["event_time"]),
        d.get("city", ""),
        int(d.get("sequence_number", 0)),
    )


class EventTimestampAssigner(TimestampAssigner):
    """Gán dấu thời gian sự kiện lấy từ trường `event_time_ms`.

    Flink cần biết mốc thời gian sự kiện để tính watermark và xếp sự kiện
    vào đúng cửa sổ. Ta dùng thời điểm sự kiện XẢY RA, không phải thời điểm
    bản tin tới Kafka — nhờ vậy sự kiện đến muộn vẫn vào đúng cửa sổ của nó.
    """

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        """Lấy epoch millisecond từ Row sự kiện.

        Args:
            value: Row sự kiện.
            record_timestamp: dấu thời gian sẵn có của bản ghi (bỏ qua).

        Returns:
            Epoch millisecond của thời điểm sự kiện xảy ra.
        """
        return value[EVENT_FIELD_NAMES.index("event_time_ms")]


def build_env(parallelism: int) -> StreamExecutionEnvironment:
    """Tạo môi trường thực thi luồng với độ song song cho trước.

    Args:
        parallelism: số luồng song song mặc định cho mọi toán tử.

    Returns:
        StreamExecutionEnvironment đã đặt độ song song.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    return env


def kafka_source(group_id: str) -> KafkaSource:
    """Dựng nguồn Kafka đọc toàn bộ topic gps-topic từ đầu.

    Đọc từ đầu (earliest) để mỗi lần chạy đều thấy toàn bộ backlog — cần
    thiết cho việc minh hoạ xử lý khối lượng lớn và tái lập kết quả.

    Args:
        group_id: mã nhóm tiêu thụ, tách biệt vị trí đọc giữa các job.

    Returns:
        KafkaSource đã cấu hình.
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap)
        .set_topics(GPS_TOPIC)
        .set_group_id(group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        # Đọc tới offset cuối tại thời điểm khởi động rồi dừng (bounded). Nhờ
        # vậy job xử lý trọn backlog, mọi cửa sổ đều đóng và job kết thúc —
        # tiện đo thời gian và đối chiếu kết quả. Mọi khái niệm luồng
        # (watermark, cửa sổ, keyed state) vẫn hoạt động y như chế độ vô hạn.
        .set_bounded(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Tính khoảng cách đường chim bay giữa hai toạ độ, đơn vị km.

    Dùng công thức Haversine trên bán kính Trái Đất trung bình. Đủ chính xác
    cho phạm vi nội thành để ước lượng tốc độ giao hàng.

    Args:
        lat1, lon1: toạ độ điểm đầu.
        lat2, lon2: toạ độ điểm cuối.

    Returns:
        Khoảng cách theo km.
    """
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))
