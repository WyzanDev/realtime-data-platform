"""
Kiểm chứng dữ liệu đã nằm thật sự trong Kafka.

Script này đọc ngược từ topic, không đọc lại biến trong bộ nhớ của
producer, nên con số báo cáo là con số thật sự có trên hàng đợi. Đây là
bằng chứng cho phần luồng dữ liệu của rubric.

Bốn nội dung được kiểm chứng:

  - Tổng số bản tin và phân bố theo phân vùng
  - Tỷ lệ sự kiện đến muộn, đo bằng hiệu `sent_at` trừ `event_time`
  - Tỷ lệ trùng lặp, đo bằng số `event_id` xuất hiện nhiều hơn một lần
  - Mật độ theo giờ, thể hiện hiện tượng burst

Cách chạy:
    python -m data_generator.verify_kafka --topic gps-topic --max-messages 200000
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime

import pandas as pd

from data_generator.common.config import load_config


def consume_topic(cfg, topic: str, max_messages: int, timeout_ms: int = 10000) -> list[dict]:
    """Đọc bản tin từ một topic, bắt đầu từ đầu hàng đợi.

    Args:
        cfg: cấu hình đã nạp.
        topic: tên topic cần đọc.
        max_messages: số bản tin tối đa sẽ đọc.
        timeout_ms: thời gian chờ tối đa khi không còn bản tin mới.

    Returns:
        Danh sách bản tin đã giải mã từ JSON.
    """
    from kafka import KafkaConsumer

    servers = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS", cfg.get("streaming.kafka.bootstrap_servers")
    )

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=servers.split(","),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        # Nhóm riêng cho việc kiểm chứng, tránh ảnh hưởng vị trí đọc của
        # các job tiêu thụ thật ở Phase 5.
        group_id="verify-tool",
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    messages: list[dict] = []
    partitions: Counter = Counter()

    for msg in consumer:
        rec = dict(msg.value)
        rec["_partition"] = msg.partition
        rec["_offset"] = msg.offset
        messages.append(rec)
        partitions[msg.partition] += 1
        if len(messages) >= max_messages:
            break

    consumer.close()

    print(f"\nĐã đọc {len(messages):,} bản tin từ topic '{topic}'")
    if partitions:
        print("Phân bố theo phân vùng:")
        for p in sorted(partitions):
            print(f"  Phân vùng {p}: {partitions[p]:,} bản tin")

    return messages


def report_late(messages: list[dict], cfg) -> pd.DataFrame:
    """Đo tỷ lệ sự kiện đến muộn dựa trên hiệu hai mốc thời gian.

    Sự kiện được coi là muộn khi `sent_at` cách `event_time` quá 5 giây.
    Ngưỡng này đủ rộng để bỏ qua độ trễ mạng bình thường, và đủ hẹp so
    với khoảng trễ cố ý tiêm vào là từ 30 giây trở lên.

    Args:
        messages: danh sách bản tin đã đọc.
        cfg: cấu hình đã nạp.

    Returns:
        Bảng một dòng tóm tắt tỷ lệ và phân bố độ trễ.
    """
    delays = []
    for m in messages:
        et = datetime.fromisoformat(m["event_time"])
        st = datetime.fromisoformat(m["sent_at"])
        delays.append((st - et).total_seconds())

    s = pd.Series(delays)
    late = s[s > 5]

    return pd.DataFrame(
        [
            {
                "tong_ban_tin": len(s),
                "so_su_kien_tre": len(late),
                "ty_le_thuc_te_pct": round(len(late) / len(s) * 100, 3) if len(s) else 0,
                "ty_le_cau_hinh_pct": round(cfg.get("streaming.late_arrival.rate") * 100, 3),
                "tre_trung_binh_giay": round(late.mean(), 1) if len(late) else 0,
                "tre_lon_nhat_giay": round(late.max(), 1) if len(late) else 0,
            }
        ]
    )


def report_duplicate(messages: list[dict], cfg) -> pd.DataFrame:
    """Đo tỷ lệ trùng lặp bằng cách đếm `event_id` xuất hiện nhiều lần.

    Đây chính là phép đo mà job tiêu thụ ở Phase 5 sẽ phải xử lý: cùng
    một `event_id` xuất hiện hai lần nghĩa là cùng một sự kiện ngoài đời
    bị ghi nhận trùng, cần khử trước khi tính toán.

    Args:
        messages: danh sách bản tin đã đọc.
        cfg: cấu hình đã nạp.

    Returns:
        Bảng một dòng tóm tắt số bản sao và tỷ lệ.
    """
    counts = Counter(m["event_id"] for m in messages)
    dup_ids = {k: v for k, v in counts.items() if v > 1}
    n_extra = sum(v - 1 for v in dup_ids.values())
    n_unique = len(counts)

    return pd.DataFrame(
        [
            {
                "tong_ban_tin": len(messages),
                "so_event_id_duy_nhat": n_unique,
                "so_ban_sao_thua": n_extra,
                "ty_le_thuc_te_pct": round(n_extra / n_unique * 100, 3) if n_unique else 0,
                "ty_le_cau_hinh_pct": round(cfg.get("streaming.duplicate.rate") * 100, 3),
            }
        ]
    )


def report_burst(messages: list[dict], cfg) -> pd.DataFrame:
    """Đo mật độ sự kiện theo giờ để thể hiện hiện tượng burst.

    Args:
        messages: danh sách bản tin đã đọc.
        cfg: cấu hình đã nạp.

    Returns:
        Bảng số sự kiện theo từng giờ, kèm nhãn phân loại giờ.
    """
    peak = set(cfg.get("streaming.burst.peak_hours"))
    by_hour: Counter = Counter()

    for m in messages:
        by_hour[datetime.fromisoformat(m["event_time"]).hour] += 1

    total = sum(by_hour.values())
    rows = []
    for h in sorted(by_hour):
        rows.append(
            {
                "gio": h,
                "so_su_kien": by_hour[h],
                "ty_le_pct": round(by_hour[h] / total * 100, 2),
                "loai_gio": "cao điểm" if h in peak else "bình thường",
            }
        )
    return pd.DataFrame(rows)


def report_ordering(messages: list[dict]) -> pd.DataFrame:
    """Kiểm tra thứ tự sự kiện trong từng đơn có bị đảo lộn không.

    Với mỗi đơn, so sánh thứ tự bản tin xuất hiện trên hàng đợi với giá
    trị `sequence_number` của chúng. Số cặp nghịch thế cho biết mức độ
    đảo lộn do sự kiện đến muộn gây ra — chính là bài toán mà cơ chế
    watermark của Flink phải giải quyết ở Phase 5.

    Args:
        messages: danh sách bản tin theo đúng thứ tự đã đọc.

    Returns:
        Bảng một dòng tóm tắt mức độ đảo thứ tự.
    """
    by_order: dict[str, list[int]] = defaultdict(list)
    for m in messages:
        by_order[m["order_id"]].append(m["sequence_number"])

    n_disordered = 0
    n_checked = 0
    for seqs in by_order.values():
        if len(seqs) < 2:
            continue
        n_checked += 1
        if seqs != sorted(seqs):
            n_disordered += 1

    return pd.DataFrame(
        [
            {
                "so_don_kiem_tra": n_checked,
                "so_don_bi_dao_thu_tu": n_disordered,
                "ty_le_pct": round(n_disordered / n_checked * 100, 2) if n_checked else 0,
            }
        ]
    )


def main() -> None:
    """Đọc dữ liệu từ Kafka và in các bảng kiểm chứng."""
    parser = argparse.ArgumentParser(description="Kiểm chứng dữ liệu trong Kafka")
    parser.add_argument("--config", default=None)
    parser.add_argument("--topic", default=None, help="tên topic, mặc định lấy từ cấu hình")
    parser.add_argument("--max-messages", type=int, default=200_000)
    parser.add_argument("--timeout-ms", type=int, default=10_000)
    args = parser.parse_args()

    cfg = load_config(args.config)
    topic = args.topic or cfg.get("streaming.kafka.topics.gps")

    messages = consume_topic(cfg, topic, args.max_messages, args.timeout_ms)
    if not messages:
        print("Không đọc được bản tin nào. Kiểm tra lại Kafka đã chạy và topic đã có dữ liệu.")
        return

    def show(title: str, df: pd.DataFrame) -> None:
        """In một bảng báo cáo kèm tiêu đề phân cách."""
        print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")
        print(df.to_string(index=False))

    show("1. LATE ARRIVAL — sự kiện đến muộn", report_late(messages, cfg))
    show("2. DUPLICATE — sự kiện bị gửi lại", report_duplicate(messages, cfg))
    show("3. BURST — mật độ sự kiện theo giờ", report_burst(messages, cfg))
    show("4. ORDERING — mức độ đảo thứ tự trong từng đơn", report_ordering(messages))

    print(f"\n{'=' * 70}\nVÍ DỤ MỘT BẢN TIN\n{'=' * 70}")
    sample = {k: v for k, v in messages[0].items() if not k.startswith("_")}
    print(json.dumps(sample, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
