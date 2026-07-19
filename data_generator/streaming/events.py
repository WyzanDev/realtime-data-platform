"""
Sinh sự kiện giao hàng dạng luồng: hành trình GPS và mốc trạng thái đơn.

Module này chỉ lo phần logic sinh sự kiện, hoàn toàn không phụ thuộc
Kafka. Nhờ tách bạch như vậy, có thể kiểm thử toàn bộ tỷ lệ burst, trễ
và trùng lặp mà không cần dựng cụm Kafka.

Ba vấn đề dữ liệu luồng được cài vào đây:

  - Burst: giờ cao điểm sinh sự kiện gấp 4 lần giờ thường
  - Late arrival: khoảng 8% sự kiện đến muộn, `sent_at` lệch xa `event_time`
  - Duplicate: khoảng 1.5% sự kiện bị gửi lại với cùng `event_id`

Điểm cốt lõi của toàn bộ module là tách bạch hai mốc thời gian:

  - `event_time`: lúc sự kiện thật sự xảy ra ngoài đời
  - `sent_at`:    lúc bản tin được đẩy vào Kafka

Với sự kiện bình thường hai mốc chỉ chênh nhau dưới một giây. Với sự
kiện trễ, khoảng chênh lên tới vài phút. Flink ở Phase 5 dùng
`event_time` để đánh dấu watermark và chia cửa sổ, còn hiệu giữa hai mốc
chính là thứ chứng minh cơ chế xử lý dữ liệu đến muộn hoạt động đúng.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import Iterator

import numpy as np

from data_generator.common.config import Config


class DeliveryTrip:
    """Một chuyến giao hàng, sinh ra chuỗi sự kiện theo đúng thứ tự.

    Mỗi chuyến bắt đầu bằng sự kiện nhận hàng tại quán, tiếp theo là một
    loạt tín hiệu GPS dọc đường, và kết thúc bằng sự kiện giao thành công.
    Toạ độ được nội suy tuyến tính giữa điểm đầu và điểm cuối rồi cộng
    thêm nhiễu nhỏ, đủ để đường đi trông tự nhiên mà không cần dịch vụ
    định tuyến thật.

    Attributes:
        order_id: mã đơn hàng.
        driver_id: mã tài xế.
        city: thành phố diễn ra chuyến giao.
        start_time: thời điểm nhận hàng.
        n_pings: số tín hiệu GPS sẽ phát trong chuyến.
    """

    def __init__(
        self,
        cfg: Config,
        order_id: str,
        driver_id: str,
        city: str,
        start_time: datetime,
    ) -> None:
        self.cfg = cfg
        self.order_id = order_id
        self.driver_id = driver_id
        self.city = city
        self.start_time = start_time

        centers = cfg.get("streaming.city_centers")
        radius = cfg.get("streaming.city_radius_degrees")
        lat_c, lon_c = centers.get(city, centers["Ho Chi Minh"])

        # Điểm đầu là quán ăn, điểm cuối là địa chỉ khách. Cả hai nằm
        # trong bán kính cho trước quanh tâm thành phố. Toạ độ làm tròn 6
        # chữ số thập phân, tương đương độ chính xác khoảng 10 cm — thừa
        # cho GPS điện thoại và giúp bản tin JSON gọn hơn.
        self.origin = (
            round(lat_c + np.random.uniform(-radius, radius), 6),
            round(lon_c + np.random.uniform(-radius, radius), 6),
        )
        self.destination = (
            round(lat_c + np.random.uniform(-radius, radius), 6),
            round(lon_c + np.random.uniform(-radius, radius), 6),
        )

        interval = cfg.get("streaming.gps_ping_interval_seconds")
        avg_min = cfg.get("streaming.avg_delivery_minutes")
        # Thời lượng chuyến dao động quanh giá trị trung bình, tối thiểu
        # 5 phút để luôn có ít nhất vài tín hiệu GPS.
        duration_min = max(5, int(np.random.normal(avg_min, avg_min * 0.3)))
        self.duration_seconds = duration_min * 60
        self.n_pings = max(2, self.duration_seconds // interval)
        self.interval = interval
        self.noise = cfg.get("streaming.gps_noise_degrees")

    def _interpolate(self, progress: float) -> tuple[float, float]:
        """Tính toạ độ tại một thời điểm giữa đường.

        Args:
            progress: tỷ lệ quãng đường đã đi, từ 0.0 đến 1.0.

        Returns:
            Cặp (vĩ độ, kinh độ) đã cộng nhiễu ngẫu nhiên.
        """
        lat = self.origin[0] + (self.destination[0] - self.origin[0]) * progress
        lon = self.origin[1] + (self.destination[1] - self.origin[1]) * progress
        return (
            round(lat + np.random.uniform(-self.noise, self.noise), 6),
            round(lon + np.random.uniform(-self.noise, self.noise), 6),
        )

    def generate_events(self) -> list[dict]:
        """Sinh toàn bộ chuỗi sự kiện của chuyến giao theo đúng thứ tự.

        Trường `sequence_number` đánh số tăng dần từ 0. Khi một phần sự
        kiện bị làm trễ và đến sai thứ tự, chính trường này cho phép nhìn
        vào kết quả Flink là biết ngay hệ thống đã sắp xếp lại đúng chưa.

        Returns:
            Danh sách sự kiện, mỗi sự kiện là một từ điển sẵn sàng
            chuyển thành JSON.
        """
        events: list[dict] = []
        seq = 0

        # --- Sự kiện nhận hàng tại quán ---
        events.append(
            self._make_event("PICKED_UP", self.start_time, self.origin, seq)
        )
        seq += 1

        # --- Chuỗi tín hiệu GPS dọc đường ---
        for i in range(1, int(self.n_pings)):
            progress = i / self.n_pings
            ts = self.start_time + timedelta(seconds=i * self.interval)
            events.append(
                self._make_event("EN_ROUTE", ts, self._interpolate(progress), seq)
            )
            seq += 1

        # --- Sự kiện giao thành công ---
        end_time = self.start_time + timedelta(seconds=self.duration_seconds)
        events.append(
            self._make_event("DELIVERED", end_time, self.destination, seq)
        )
        return events

    def _make_event(
        self, event_type: str, event_time: datetime, coords: tuple[float, float], seq: int
    ) -> dict:
        """Đóng gói một sự kiện thành từ điển.

        Trường `sent_at` tạm để trùng `event_time`. Bước tiêm lỗi phía sau
        sẽ đẩy nó ra xa với những sự kiện được chọn làm trễ.

        Args:
            event_type: loại sự kiện.
            event_time: thời điểm sự kiện xảy ra.
            coords: cặp toạ độ vĩ độ và kinh độ.
            seq: số thứ tự trong chuyến.

        Returns:
            Từ điển mô tả sự kiện.
        """
        return {
            "event_id": str(uuid.uuid4()),
            "order_id": self.order_id,
            "driver_id": self.driver_id,
            "event_type": event_type,
            "lat": coords[0],
            "lon": coords[1],
            "event_time": event_time.isoformat(),
            "sent_at": event_time.isoformat(),
            "sequence_number": seq,
            "city": self.city,
        }


def apply_late_arrival(cfg: Config, events: list[dict]) -> tuple[list[dict], int]:
    """Làm một tỷ lệ sự kiện đến muộn bằng cách đẩy `sent_at` ra xa.

    Đây là mô phỏng tình huống rất thật: điện thoại tài xế mất sóng trong
    hầm gửi xe hoặc vùng phủ kém, ứng dụng giữ lại các tín hiệu rồi gửi
    dồn khi có mạng trở lại. Kết quả là sự kiện mang mốc thời gian cũ
    nhưng lại xuất hiện muộn trên hàng đợi.

    Chỉ `sent_at` bị thay đổi, `event_time` giữ nguyên. Nhờ vậy Flink vẫn
    xếp được sự kiện vào đúng cửa sổ thời gian mà nó thuộc về, miễn là
    watermark còn đủ độ trễ cho phép.

    Args:
        cfg: cấu hình đã nạp.
        events: danh sách sự kiện cần xử lý, sẽ bị sửa tại chỗ.

    Returns:
        Cặp (danh sách sự kiện, số sự kiện đã bị làm trễ).
    """
    if not cfg.get("streaming.late_arrival.enabled"):
        return events, 0

    rate = cfg.get("streaming.late_arrival.rate")
    lo, hi = cfg.get("streaming.late_arrival.delay_seconds")

    n_late = 0
    for ev in events:
        if np.random.random() < rate:
            delay = int(np.random.uniform(lo, hi))
            sent = datetime.fromisoformat(ev["event_time"]) + timedelta(seconds=delay)
            ev["sent_at"] = sent.isoformat()
            ev["_is_late"] = True
            n_late += 1
    return events, n_late


def apply_duplicates(cfg: Config, events: list[dict]) -> tuple[list[dict], int]:
    """Nhân bản một tỷ lệ sự kiện, giữ nguyên `event_id`.

    Mô phỏng cơ chế gửi lại của Kafka producer khi không nhận được xác
    nhận: bản tin đã tới nơi nhưng phía gửi tưởng thất bại nên gửi thêm
    lần nữa. Đây chính là lý do hệ thống tiêu thụ phải tự khử trùng thay
    vì tin rằng mỗi bản tin chỉ đến đúng một lần.

    Bản sao giữ nguyên `event_id` để có thể phát hiện, nhưng `sent_at`
    muộn hơn vài giây — giống hệt cơ chế phá hoà của dữ liệu offline.

    Args:
        cfg: cấu hình đã nạp.
        events: danh sách sự kiện gốc.

    Returns:
        Cặp (danh sách đã chèn bản sao, số bản sao đã thêm).
    """
    if not cfg.get("streaming.duplicate.enabled"):
        return events, 0

    rate = cfg.get("streaming.duplicate.rate")
    dups: list[dict] = []

    for ev in events:
        if np.random.random() < rate:
            copy = dict(ev)
            sent = datetime.fromisoformat(ev["sent_at"]) + timedelta(
                seconds=int(np.random.uniform(1, 30))
            )
            copy["sent_at"] = sent.isoformat()
            copy["_is_duplicate"] = True
            dups.append(copy)

    return events + dups, len(dups)


def events_per_second(cfg: Config, hour: int) -> float:
    """Tính số sự kiện mỗi giây tương ứng với giờ trong ngày.

    Giờ cao điểm được nhân với hệ số trong cấu hình, mặc định gấp 4 lần.
    Đây là cách tạo burst: không phải sinh dồn ngẫu nhiên mà bám theo
    đúng khung giờ ăn trưa và ăn tối, giống hành vi thật của người dùng
    ứng dụng đặt đồ ăn.

    Args:
        cfg: cấu hình đã nạp.
        hour: giờ trong ngày, từ 0 đến 23.

    Returns:
        Số sự kiện kỳ vọng mỗi giây.
    """
    base = cfg.get("streaming.burst.base_events_per_second")
    if not cfg.get("streaming.burst.enabled"):
        return float(base)

    peak_hours = cfg.get("streaming.burst.peak_hours")
    multiplier = cfg.get("streaming.burst.multiplier")
    return float(base * multiplier) if hour in peak_hours else float(base)


def simulate_window(
    cfg: Config,
    start: datetime,
    minutes: int,
    drivers: list[str],
    cities: list[str],
    city_weights: list[float],
) -> Iterator[tuple[datetime, list[dict]]]:
    """Sinh sự kiện cho một khoảng thời gian mô phỏng, chia theo từng phút.

    Mỗi phút mô phỏng sẽ khởi tạo một số chuyến giao mới tương ứng mật độ
    của giờ đó, rồi trả về toàn bộ sự kiện phát sinh. Việc chia nhỏ theo
    phút cho phép bên gọi vừa bơm dữ liệu vừa quan sát tiến độ, thay vì
    phải đợi sinh hết mới thấy gì.

    Args:
        cfg: cấu hình đã nạp.
        start: thời điểm bắt đầu mô phỏng.
        minutes: độ dài khoảng mô phỏng tính bằng phút.
        drivers: danh sách mã tài xế để gán ngẫu nhiên.
        cities: danh sách thành phố.
        city_weights: trọng số chọn thành phố, phản ánh phân phối lệch.

    Yields:
        Cặp (thời điểm phút đang mô phỏng, danh sách sự kiện của phút đó).
    """
    interval = cfg.get("streaming.gps_ping_interval_seconds")
    avg_min = cfg.get("streaming.avg_delivery_minutes")
    # Số sự kiện trung bình mà một chuyến sinh ra, dùng để suy ngược ra
    # cần khởi tạo bao nhiêu chuyến mới mỗi phút.
    events_per_trip = max(3, (avg_min * 60) // interval)

    for m in range(minutes):
        now = start + timedelta(minutes=m)
        eps = events_per_second(cfg, now.hour)
        target_events = int(eps * 60)
        n_trips = max(1, target_events // events_per_trip)

        batch: list[dict] = []
        for _ in range(int(n_trips)):
            city = np.random.choice(cities, p=city_weights)
            trip = DeliveryTrip(
                cfg,
                order_id=f"ORD{np.random.randint(0, 2_500_000):09d}",
                driver_id=str(np.random.choice(drivers)),
                city=str(city),
                start_time=now + timedelta(seconds=int(np.random.uniform(0, 60))),
            )
            batch.extend(trip.generate_events())

        batch, _ = apply_late_arrival(cfg, batch)
        batch, _ = apply_duplicates(cfg, batch)

        # Sắp xếp theo thời điểm gửi, mô phỏng đúng thứ tự bản tin thật
        # sự đến hàng đợi. Sự kiện bị làm trễ vì thế sẽ nằm sai vị trí so
        # với `event_time` của nó.
        batch.sort(key=lambda e: e["sent_at"])
        yield now, batch
