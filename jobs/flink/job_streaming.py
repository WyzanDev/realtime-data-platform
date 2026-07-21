"""
PyFlink job — xử lý luồng sự kiện giao hàng từ `gps-topic`.

Job tính tốc độ giao trung bình của mỗi tài xế trong cửa sổ trượt 5 phút
(theo thời gian sự kiện). Bốn vấn đề dữ liệu luồng được xử lý bằng bốn kỹ
thuật, và job nhận cờ dòng lệnh để bật/tắt từng kỹ thuật — nhờ vậy đo được
tác động của từng cái một cách độc lập:

  - Burst  → `--parallelism N`: mỗi partition Kafka một luồng đọc riêng.
  - Late   → `--late-tolerance-min M`: watermark bounded-out-of-orderness.
             M=0 là baseline (rơi sự kiện muộn); M=5 dung nạp sự kiện trễ.
  - Dup    → `--dedup`: toán tử khử trùng theo `event_id` bằng keyed state.
  - Window → luôn bật: TumblingEventTimeWindows 5 phút + ProcessWindowFunction.

Với dung sai 0, sự kiện đến muộn bị loại trước khi vào cửa sổ; với dung sai 5
phút chúng kịp vào đúng cửa sổ. Tổng số ping gộp trong các cửa sổ vì thế lớn
hơn ở chế độ dung nạp — đó là bằng chứng đo được cho kỹ thuật watermark.

Cách chạy (submit lên cụm):
    # baseline
    flink run -pyfs common.py -py job_streaming.py --parallelism 1 --late-tolerance-min 0
    # tối ưu đầy đủ
    flink run -pyfs common.py -py job_streaming.py --parallelism 6 --late-tolerance-min 5 --dedup
"""

from __future__ import annotations

import argparse

from pyflink.common import Duration, Row, Time, Types, WatermarkStrategy
from pyflink.datastream.functions import KeyedProcessFunction, ProcessWindowFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

from common import (
    EventTimestampAssigner,
    build_env,
    event_row_type,
    haversine_km,
    kafka_source,
    parse_event,
)

# Kiểu Row của kết quả cửa sổ: (thời điểm cuối cửa sổ, tài xế, số ping, tốc độ).
RESULT_TYPE = Types.ROW_NAMED(
    ["window_end_ms", "driver_id", "pings", "avg_speed_kmh"],
    [Types.LONG(), Types.STRING(), Types.INT(), Types.DOUBLE()],
)


class DedupByEventId(KeyedProcessFunction):
    """Khử sự kiện trùng `event_id` bằng trạng thái có khoá.

    Luồng được keyBy theo `event_id` nên mỗi khoá là một sự kiện duy nhất.
    Trạng thái `seen` ghi nhớ đã gặp khoá đó chưa: lần đầu cho đi qua, các
    lần sau bỏ. Đây đúng là "toán tử khử trùng dùng keyed state" mà cơ chế
    at-least-once của app tài xế đòi hỏi (khoảng 1,5% sự kiện gửi lại).
    """

    def open(self, runtime_context):
        """Khởi tạo trạng thái khi toán tử bắt đầu.

        Args:
            runtime_context: ngữ cảnh chạy do Flink cấp.
        """
        self.seen = runtime_context.get_state(
            ValueStateDescriptor("seen", Types.BOOLEAN())
        )

    def process_element(self, value, ctx):
        """Cho đi qua nếu lần đầu gặp event_id, ngược lại loại bỏ.

        Args:
            value: Row sự kiện.
            ctx: ngữ cảnh toán tử.

        Yields:
            Chính Row đó nếu chưa từng thấy event_id này.
        """
        if self.seen.value() is None:
            self.seen.update(True)
            yield value


class AvgSpeedPerDriver(ProcessWindowFunction):
    """Tính tốc độ giao trung bình của một tài xế trong một cửa sổ.

    Nhận toàn bộ ping GPS của tài xế trong cửa sổ 5 phút, sắp theo thời gian
    sự kiện, cộng dồn quãng đường giữa các ping liên tiếp (khoảng cách
    Haversine) rồi chia cho khoảng thời gian giữa ping đầu và ping cuối. Kết
    quả là tốc độ trung bình theo km/h.
    """

    def process(self, key, context, elements):
        """Gộp các ping trong cửa sổ thành một dòng kết quả.

        Args:
            key: driver_id của nhóm.
            context: ngữ cảnh cửa sổ, cho biết mốc cuối cửa sổ.
            elements: các Row sự kiện thuộc cửa sổ.

        Yields:
            Row (mốc cuối cửa sổ, tài xế, số ping, tốc độ km/h).
        """
        pts = sorted(elements, key=lambda r: r[6])  # r[6] = event_time_ms
        if len(pts) < 2:
            return

        distance_km = 0.0
        for i in range(1, len(pts)):
            distance_km += haversine_km(pts[i - 1][4], pts[i - 1][5], pts[i][4], pts[i][5])

        span_seconds = (pts[-1][6] - pts[0][6]) / 1000.0
        speed = distance_km / span_seconds * 3600.0 if span_seconds > 0 else 0.0

        yield Row(context.window().end, key, len(pts), round(speed, 1))


def build_watermark(tolerance_min: int) -> WatermarkStrategy:
    """Chọn chiến lược watermark theo mức dung sai đến muộn.

    Args:
        tolerance_min: số phút dung nạp sự kiện đến muộn. 0 nghĩa là watermark
            bám sát sự kiện mới nhất — mọi sự kiện lệch thứ tự đều bị coi là
            muộn (baseline).

    Returns:
        WatermarkStrategy tương ứng.
    """
    if tolerance_min <= 0:
        return WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
            EventTimestampAssigner()
        )
    return WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_minutes(tolerance_min)
    ).with_timestamp_assigner(EventTimestampAssigner())


def run(parallelism: int, tolerance_min: int, dedup: bool) -> None:
    """Dựng và chạy đồ thị luồng theo cấu hình cờ.

    Args:
        parallelism: số luồng song song.
        tolerance_min: dung sai đến muộn của watermark, tính bằng phút.
        dedup: có bật toán tử khử trùng theo event_id không.
    """
    env = build_env(parallelism)
    tag = f"p{parallelism}_late{tolerance_min}_dedup{int(dedup)}"
    source = kafka_source(group_id=f"flink-phase5-{tag}")

    raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-gps-source")
    events = raw.map(parse_event, output_type=event_row_type())
    events = events.assign_timestamps_and_watermarks(build_watermark(tolerance_min))

    if dedup:
        events = events.key_by(lambda r: r[0], key_type=Types.STRING()).process(
            DedupByEventId(), output_type=event_row_type()
        )

    # Cửa sổ trượt 5 phút theo thời gian sự kiện; r[2] = driver_id. Với
    # watermark dung sai 0 (baseline), sự kiện đến sau khi cửa sổ đã đóng bị
    # loại lặng lẽ trước khi vào cửa sổ; với dung sai 5 phút chúng kịp vào
    # đúng cửa sổ. Hiệu số ping/kết quả giữa hai chế độ chính là số sự kiện
    # muộn được cứu.
    windowed = (
        events.key_by(lambda r: r[2], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .process(AvgSpeedPerDriver(), output_type=RESULT_TYPE)
    )
    windowed.print("WINDOW")

    env.execute(f"phase5_{tag}")


def main() -> None:
    """Điểm vào: đọc tham số dòng lệnh và chạy."""
    # Dùng --par chứ KHÔNG dùng --parallelism: Flink CLI có sẵn tuỳ chọn
    # --parallelism và sẽ nuốt mất trước khi tới script, khiến job luôn chạy
    # độ song song mặc định. Tên khác tránh va chạm này.
    parser = argparse.ArgumentParser(description="Xử lý luồng giao hàng bằng Flink")
    parser.add_argument("--par", type=int, default=1, help="Độ song song")
    parser.add_argument("--late-tolerance-min", type=int, default=0)
    parser.add_argument("--dedup", action="store_true")
    args = parser.parse_args()
    run(args.par, args.late_tolerance_min, args.dedup)


if __name__ == "__main__":
    main()
