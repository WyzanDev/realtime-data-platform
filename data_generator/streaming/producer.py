"""
Bơm sự kiện giao hàng vào Kafka và đo lại đặc tính luồng dữ liệu.

Module này lo phần kết nối và gửi bản tin, còn logic sinh sự kiện nằm ở
`events.py`. Tách như vậy để có thể chạy chế độ khô, tức sinh và thống kê
mà không cần Kafka, tiện kiểm tra tỷ lệ burst và trễ trước khi bơm thật.

Hai topic được sử dụng:

  - gps-topic:   toàn bộ tín hiệu GPS dọc đường, khối lượng lớn
  - order-topic: chỉ các mốc trạng thái quan trọng là nhận hàng và giao xong

Khoá phân vùng của cả hai topic đều là `order_id`. Nhờ vậy mọi sự kiện
của cùng một đơn luôn rơi vào cùng một phân vùng và giữ nguyên thứ tự —
điều kiện cần để Flink ở Phase 5 tính được trạng thái theo từng đơn.
"""

from __future__ import annotations

import json
import os
import time
from collections import Counter
from datetime import datetime, timedelta

import numpy as np

from data_generator.common.config import Config
from data_generator.streaming import events as ev


class StreamStats:
    """Gom số liệu thống kê trong lúc bơm để đối chiếu với cấu hình.

    Đây là nguồn dữ liệu cho phần bằng chứng của rubric: tỷ lệ trễ, tỷ lệ
    trùng và mật độ sự kiện theo giờ đo được phải khớp với các con số đã
    khai trong file YAML.
    """

    def __init__(self) -> None:
        self.total = 0
        self.late = 0
        self.duplicate = 0
        self.by_hour: Counter = Counter()
        self.by_type: Counter = Counter()
        self.by_city: Counter = Counter()
        self.delay_seconds: list[float] = []

    def record(self, event: dict) -> None:
        """Ghi nhận một sự kiện vào bảng thống kê.

        Args:
            event: từ điển sự kiện vừa gửi đi.
        """
        self.total += 1
        et = datetime.fromisoformat(event["event_time"])
        st = datetime.fromisoformat(event["sent_at"])

        self.by_hour[et.hour] += 1
        self.by_type[event["event_type"]] += 1
        self.by_city[event.get("city", "unknown")] += 1

        if event.get("_is_late"):
            self.late += 1
            self.delay_seconds.append((st - et).total_seconds())
        if event.get("_is_duplicate"):
            self.duplicate += 1

    def summary(self, cfg: Config) -> str:
        """Dựng báo cáo dạng văn bản để in ra và lưu làm bằng chứng.

        Args:
            cfg: cấu hình đã nạp, dùng để in kèm tỷ lệ kỳ vọng.

        Returns:
            Chuỗi báo cáo nhiều dòng.
        """
        lines = []
        sep = "=" * 70

        lines.append(f"\n{sep}\nTỔNG QUAN LUỒNG SỰ KIỆN\n{sep}")
        lines.append(f"  Tổng số sự kiện đã gửi: {self.total:,}")

        # --- Tỷ lệ đến muộn ---
        late_pct = self.late / self.total * 100 if self.total else 0
        cfg_late = cfg.get("streaming.late_arrival.rate") * 100
        lines.append(f"\n{sep}\nLATE ARRIVAL — sự kiện đến muộn\n{sep}")
        lines.append(f"  Số sự kiện trễ: {self.late:,}")
        lines.append(f"  Tỷ lệ thực tế:  {late_pct:.3f}%")
        lines.append(f"  Tỷ lệ cấu hình: {cfg_late:.3f}%")
        if self.delay_seconds:
            arr = np.array(self.delay_seconds)
            lo, hi = cfg.get("streaming.late_arrival.delay_seconds")
            lines.append(f"  Độ trễ nhỏ nhất:   {arr.min():.0f} giây")
            lines.append(f"  Độ trễ trung bình: {arr.mean():.0f} giây")
            lines.append(f"  Độ trễ lớn nhất:   {arr.max():.0f} giây")
            lines.append(f"  Khoảng cấu hình:   {lo} đến {hi} giây")

        # --- Tỷ lệ trùng lặp ---
        dup_pct = self.duplicate / self.total * 100 if self.total else 0
        cfg_dup = cfg.get("streaming.duplicate.rate") * 100
        lines.append(f"\n{sep}\nDUPLICATE — sự kiện bị gửi lại\n{sep}")
        lines.append(f"  Số bản sao:     {self.duplicate:,}")
        lines.append(f"  Tỷ lệ thực tế:  {dup_pct:.3f}%")
        lines.append(f"  Tỷ lệ cấu hình: {cfg_dup:.3f}%")

        # --- Mật độ theo giờ, thể hiện burst ---
        peak = set(cfg.get("streaming.burst.peak_hours"))
        lines.append(f"\n{sep}\nBURST — mật độ sự kiện theo giờ\n{sep}")
        lines.append(f"  {'Giờ':>5} {'Số sự kiện':>12} {'Tỷ lệ':>8}  Loại giờ")
        peak_total = 0
        normal_total = 0
        for hour in sorted(self.by_hour):
            n = self.by_hour[hour]
            pct = n / self.total * 100
            kind = "cao điểm" if hour in peak else "bình thường"
            if hour in peak:
                peak_total += n
            else:
                normal_total += n
            lines.append(f"  {hour:>5} {n:>12,} {pct:>7.2f}%  {kind}")

        n_peak_hours = len([h for h in self.by_hour if h in peak])
        n_normal_hours = len([h for h in self.by_hour if h not in peak])
        if n_peak_hours and n_normal_hours:
            avg_peak = peak_total / n_peak_hours
            avg_normal = normal_total / n_normal_hours
            ratio = avg_peak / avg_normal if avg_normal else 0
            lines.append(
                f"\n  Trung bình mỗi giờ cao điểm:    {avg_peak:>10,.0f} sự kiện"
            )
            lines.append(
                f"  Trung bình mỗi giờ bình thường: {avg_normal:>10,.0f} sự kiện"
            )
            lines.append(f"  Hệ số burst thực tế:  {ratio:.2f} lần")
            lines.append(
                f"  Hệ số burst cấu hình: {cfg.get('streaming.burst.multiplier'):.2f} lần"
            )

        # --- Phân bố theo loại sự kiện và thành phố ---
        lines.append(f"\n{sep}\nPHÂN BỐ THEO LOẠI SỰ KIỆN\n{sep}")
        for t, n in self.by_type.most_common():
            lines.append(f"  {t:<14} {n:>12,}  {n / self.total * 100:>6.2f}%")

        lines.append(f"\n{sep}\nPHÂN BỐ THEO THÀNH PHỐ\n{sep}")
        city_cfg = cfg.get("skew.city")
        for c, n in self.by_city.most_common():
            actual = n / self.total * 100
            expected = city_cfg.get(c, 0) * 100
            lines.append(
                f"  {c:<14} {n:>12,}  thực tế {actual:>6.2f}%  cấu hình {expected:>6.2f}%"
            )

        return "\n".join(lines)


def build_producer(cfg: Config):
    """Khởi tạo Kafka producer với cấu hình phù hợp cho khối lượng lớn.

    Bản tin được nén bằng gzip và gom theo lô để giảm số lần gọi mạng.
    Tham số `acks` để mức 1, tức chỉ chờ phân vùng chính xác nhận —
    đánh đổi hợp lý giữa tốc độ và độ an toàn cho dữ liệu mô phỏng.

    Args:
        cfg: cấu hình đã nạp.

    Returns:
        Đối tượng KafkaProducer đã sẵn sàng.
    """
    from kafka import KafkaProducer

    servers = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", cfg.get("streaming.kafka.bootstrap_servers")
    )

    return KafkaProducer(
        bootstrap_servers=servers.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        compression_type="gzip",
        acks=1,
        linger_ms=50,
        batch_size=64 * 1024,
    )


def run_stream(
    cfg: Config,
    minutes: int,
    dry_run: bool = False,
    realtime: bool = False,
    start_hour: int | None = None,
) -> StreamStats:
    """Chạy mô phỏng luồng và bơm sự kiện vào Kafka.

    Thời gian được tua nhanh theo hệ số `time_acceleration` trong cấu
    hình, nên chỉ cần chạy vài chục giây thực tế là đã bao trọn một ngày
    mô phỏng, đủ để thấy rõ chênh lệch giữa giờ cao điểm và giờ thường.

    Args:
        cfg: cấu hình đã nạp.
        minutes: số phút mô phỏng cần sinh.
        dry_run: True thì chỉ sinh và thống kê, không kết nối Kafka.
        realtime: True thì chờ giữa các phút để mô phỏng tốc độ thật.
        start_hour: giờ bắt đầu mô phỏng. Bỏ trống thì bắt đầu từ 6 giờ
            sáng để chuỗi thời gian đi qua đủ cả hai khung cao điểm.

    Returns:
        Bảng thống kê đã thu thập trong suốt quá trình chạy.
    """
    stats = StreamStats()

    producer = None
    if not dry_run:
        producer = build_producer(cfg)

    topic_gps = cfg.get("streaming.kafka.topics.gps")
    topic_order = cfg.get("streaming.kafka.topics.order")

    # Danh sách tài xế và thành phố dùng để gán ngẫu nhiên cho từng chuyến.
    n_drivers = cfg.get("volume.n_drivers")
    drivers = [f"DRV{i:06d}" for i in range(min(n_drivers, 5000))]

    city_cfg = cfg.get("skew.city")
    cities = list(city_cfg.keys())
    weights = np.array([float(v) for v in city_cfg.values()])
    weights = weights / weights.sum()

    hour = 6 if start_hour is None else start_hour
    start = datetime.now().replace(hour=hour, minute=0, second=0, microsecond=0)

    accel = cfg.get("streaming.time_acceleration", 60)
    sleep_per_minute = 60.0 / accel if realtime else 0.0

    print(f"Bắt đầu mô phỏng từ {start:%H:%M} trong {minutes} phút mô phỏng")
    if dry_run:
        print("Chế độ khô: chỉ sinh và thống kê, không gửi Kafka")
    else:
        print(f"Đang gửi tới topic '{topic_gps}' và '{topic_order}'")

    t0 = time.time()
    for now, batch in ev.simulate_window(cfg, start, minutes, drivers, cities, weights):
        for event in batch:
            stats.record(event)

            if producer is not None:
                # Bỏ các trường đánh dấu nội bộ trước khi gửi đi. Chúng
                # chỉ phục vụ việc thống kê, không thuộc lược đồ dữ liệu
                # mà bên tiêu thụ nhìn thấy.
                payload = {k: v for k, v in event.items() if not k.startswith("_")}
                producer.send(topic_gps, key=event["order_id"], value=payload)

                # Chỉ mốc nhận hàng và giao xong mới đi vào topic đơn hàng.
                if event["event_type"] in ("PICKED_UP", "DELIVERED"):
                    producer.send(topic_order, key=event["order_id"], value=payload)

        if now.minute % 30 == 0:
            print(f"  {now:%H:%M} — đã gửi {stats.total:,} sự kiện")

        if sleep_per_minute:
            time.sleep(sleep_per_minute)

    if producer is not None:
        producer.flush()
        producer.close()

    elapsed = time.time() - t0
    print(f"\nHoàn tất sau {elapsed:.1f} giây thực tế")
    if elapsed > 0:
        print(f"Tốc độ trung bình: {stats.total / elapsed:,.0f} sự kiện mỗi giây")

    return stats
