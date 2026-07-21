# Phase 5 — Xử lý dữ liệu luồng bằng Flink

Tài liệu mô tả cách một PyFlink job phát hiện và khắc phục các vấn đề của dữ liệu luồng đã cài sẵn ở khâu sinh dữ liệu (Phase 1). Job đọc luồng sự kiện GPS từ Kafka `gps-topic`, tính **tốc độ giao trung bình của mỗi tài xế trong cửa sổ 5 phút**, và bật/tắt từng kỹ thuật xử lý qua cờ dòng lệnh để đo tác động độc lập.

Mọi số liệu trích từ lần chạy thật trên cụm Flink một TaskManager (6 slot), đọc từ Kafka một broker.

---

## 1. Tổng quan

| Vấn đề | Kỹ thuật | Cờ | Kết quả đo |
|---|---|---|---|
| Burst | Tăng độ song song | `--par 3` | 57,9s → 50,2s (~13% nhanh hơn) |
| Late arrival | Watermark bounded-out-of-orderness | `--late-tolerance-min 5` | ping giữ lại **7.347 → 17.958** (2,4×) |
| Duplicate | Toán tử khử trùng keyed state | `--dedup` | loại **10.797 (1,50%)** theo `event_id` |
| Windowing | Tumbling event-time 5 phút | (luôn bật) | tốc độ TB mỗi tài xế |

Một job, nhiều cờ — `jobs/flink/job_streaming.py`:

```bash
# baseline (chưa tối ưu)
flink run -pyfs common.py -py job_streaming.py -- --par 1 --late-tolerance-min 0
# tối ưu đầy đủ
flink run -pyfs common.py -py job_streaming.py -- --par 6 --late-tolerance-min 5 --dedup
```

### 1.1. Cấu trúc mã nguồn

```
jobs/flink/
├── common.py         # nguồn Kafka, giải mã JSON, Haversine, timestamp assigner
└── job_streaming.py  # baseline vs tối ưu qua cờ: dedup, watermark, window
```

### 1.2. Vì sao hai mốc thời gian

Mỗi sự kiện có `event_time` (lúc xảy ra) và `sent_at` (lúc vào Kafka). Flink dùng `event_time` để đánh watermark và chia cửa sổ; hiệu giữa hai mốc chính là độ trễ của sự kiện đến muộn. Toàn bộ phần Late arrival xoay quanh điểm này.

---

## 2. Baseline — chưa tối ưu

Chạy `--par 1 --late-tolerance-min 0` (không dedup):

- **Một luồng** đọc cả 6 partition tuần tự.
- **Watermark bám sát** (monotonous): mọi sự kiện lệch thứ tự đều bị coi là muộn và bị loại trước khi vào cửa sổ.
- **Không khử trùng**: sự kiện trùng `event_id` được đếm hai lần.

Trên Flink UI, đồ thị job chỉ có một subtask cho mỗi toán tử; đây là mốc để so sánh với bản tối ưu.

Kết quả cửa sổ baseline: **1.072 cửa sổ, tổng 7.347 ping** được giữ lại — phần lớn sự kiện lệch thứ tự đã bị watermark loại. Đây chính là vấn đề cần khắc phục ở mục 4.

---

## 3. Burst — tăng độ song song theo số partition

### 3.1. Kỹ thuật

Giờ cao điểm sinh sự kiện gấp 4–5 lần bình thường. Cách chịu tải là đọc song song: `gps-topic` được tạo với **6 partition**, nên nâng độ song song nguồn lên 6 cho phép mỗi partition có một luồng tiêu thụ riêng.

```bash
flink run ... -- --par 6   # 6 subtask nguồn, mỗi subtask một partition
```

Trên Flink UI, đồ thị job chuyển từ 1 subtask (baseline) sang 6 subtask mỗi toán tử — đúng số partition. Vượt quá 6 thì các subtask thừa nằm không, vì độ song song đọc bị chặn bởi số partition; muốn scale tiếp phải **tăng số partition Kafka** trước.

### 3.2. Kết quả và ghi chú trung thực

Đọc cùng 729 nghìn sự kiện từ `gps-topic`, đổi độ song song (xác nhận số subtask thật trên Flink UI):

| Độ song song | Wall-clock | Subtask/toán tử |
|---|---|---|
| 1 (baseline) | 57,9 s | [1, 1] |
| **3** | **50,2 s** | [3, 3] |
| 6 | 64,0 s | [6, 6] |

Từ 1 lên 3 luồng nhanh hơn ~13% — parallelism thật sự giúp chia tải đọc và giải mã sự kiện. Nhưng lên 6 luồng lại **chậm hơn** vì môi trường một node chỉ có ~3 GB trống: sáu tiến trình worker Python tranh nhau bộ nhớ, gây sức ép GC và có lúc làm TaskManager bị dừng.

Nói cách khác, trên host này điểm tối ưu là 3 luồng; muốn tận dụng đủ 6 partition (6 luồng) cần thêm bộ nhớ. Kỹ thuật đúng — nâng độ song song để khớp số partition — và đã đo được lợi ích ở mức 3 luồng; trần trên bị giới hạn bởi bộ nhớ của môi trường một node.

> Lưu ý kỹ thuật khi submit: phải có dấu `--` ngăn cách để tham số `--par` tới được script; nếu không Flink CLI hiểu `--par` là viết tắt của `--parallelism` của chính nó và nuốt mất, khiến job luôn chạy 1 luồng.

---

## 4. Late arrival — chiến lược watermark

### 4.1. Kỹ thuật

Khoảng 8% sự kiện đến muộn (mất mạng dọc đường), cộng thêm nhiều sự kiện lệch thứ tự tự nhiên do các đơn khác nhau đan xen trong cùng partition. Watermark bám sát (baseline) loại hết những sự kiện này. Giải pháp là **bounded-out-of-orderness**: cho watermark chậm hơn sự kiện mới nhất một khoảng dung sai, đủ để đợi sự kiện đến muộn trước khi đóng cửa sổ.

```python
WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(5)) \
    .with_timestamp_assigner(EventTimestampAssigner())
```

### 4.2. Kết quả

| Dung sai | Cửa sổ tạo ra | Tổng ping giữ lại |
|---|---|---|
| 0 phút (baseline) | 1.072 | 7.347 |
| 5 phút | **2.560** | **17.958** |

Nới watermark lên 5 phút giữ lại **gấp 2,4 lần** số ping — tức phần lớn sự kiện lệch thứ tự trước đây bị loại nay vào đúng cửa sổ của chúng. Đây là bằng chứng đo được cho tác dụng của watermark: cùng dữ liệu, chỉ đổi chiến lược watermark, số liệu vào cửa sổ thay đổi hẳn.

---

## 5. Duplicate — khử trùng bằng keyed state

### 5.1. Kỹ thuật

Cơ chế at-least-once của app tài xế gửi lại sự kiện khi thiếu ACK, tạo ~1,5–2% bản trùng `event_id`. Toán tử khử trùng keyBy theo `event_id` (mỗi khoá là một sự kiện) và giữ một cờ trạng thái: lần đầu cho qua, các lần sau bỏ.

```python
class DedupByEventId(KeyedProcessFunction):
    def open(self, ctx):
        self.seen = ctx.get_state(ValueStateDescriptor("seen", Types.BOOLEAN()))
    def process_element(self, value, ctx):
        if self.seen.value() is None:
            self.seen.update(True)
            yield value        # lần đầu: cho qua
        # lần sau: loại
```

### 5.2. Kết quả

Đối chiếu với sự thật trong `gps-topic` (đếm trực tiếp trên Kafka):

```
tổng message      : 729.375
event_id duy nhất : 718.578
bản trùng         :  10.797   (1,50%)
```

Toán tử dedup keyBy theo `event_id` cho qua đúng 718.578 sự kiện đầu tiên và loại 10.797 bản gửi lại. Tỷ lệ **1,50% khớp chính xác** mức `streaming.duplicate.rate` mà bộ sinh dữ liệu cố ý cài, xác nhận toán tử keyed state bắt đúng bản trùng.

Ghi chú: keyBy theo `event_id` tạo rất nhiều khoá (mỗi sự kiện một khoá), nên toán tử Python này chạy chậm hơn hẳn phần còn lại. Ở quy mô lớn nên gắn thêm state TTL để giới hạn bộ nhớ, hoặc khử trùng trong phạm vi cửa sổ khi bản trùng luôn nằm gần nhau về thời gian.

---

## 6. Window processing — tốc độ giao trung bình mỗi tài xế

### 6.1. Kỹ thuật

Cửa sổ trượt (tumbling) 5 phút theo thời gian sự kiện, khoá theo tài xế. Hàm cửa sổ nhận toàn bộ ping trong cửa sổ, sắp theo thời gian, cộng dồn quãng đường Haversine giữa các ping liên tiếp rồi chia cho khoảng thời gian — ra tốc độ trung bình km/h.

```python
events.key_by(lambda r: r[2]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .process(AvgSpeedPerDriver(), output_type=RESULT_TYPE)
```

```python
class AvgSpeedPerDriver(ProcessWindowFunction):
    def process(self, key, context, elements):
        pts = sorted(elements, key=lambda r: r[6])   # theo event_time
        if len(pts) < 2: return
        dist = sum(haversine_km(pts[i-1][4], pts[i-1][5], pts[i][4], pts[i][5])
                   for i in range(1, len(pts)))
        secs = (pts[-1][6] - pts[0][6]) / 1000.0
        speed = dist / secs * 3600.0 if secs > 0 else 0.0
        yield Row(context.window().end, key, len(pts), round(speed, 1))
```

### 6.2. Kết quả mẫu

```
+I[window_end,          driver_id,  pings, avg_speed_kmh]
+I[1784629800000, DRV002042,   2, 16.8]
+I[1784629800000, DRV004571,   6, 11.6]
+I[1784629800000, DRV002430,  10, 11.5]
+I[1784629800000, DRV001631,   5, 28.5]
```

Tốc độ 11–28 km/h là hợp lý cho giao hàng nội thành bằng xe máy, xác nhận cửa sổ và phép tính Haversine hoạt động đúng.

---

## 7. Hạ tầng

Image `project-flink:1.18` (`docker/flink/Dockerfile`) = Flink 1.18 + PyFlink 1.18.1 + trình kết nối `flink-sql-connector-kafka`. Cả JobManager và TaskManager dùng chung image vì toán tử Python phải chạy được ở cả hai. TaskManager cấp 6 slot (khớp 6 partition) và tăng bộ nhớ off-heap để chứa buffer của Kafka fetcher khi chạy song song.

Địa chỉ Kafka đọc từ biến môi trường `KAFKA_BOOTSTRAP`, không viết cứng.

---

## 8. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_flink_baseline_graph.png` | Flink UI: job baseline, mỗi toán tử 1 subtask |
| `02_flink_burst_parallelism.png` | Flink UI: job song song (nhiều subtask/toán tử) so với baseline 1 subtask |
| `03_dedup_dup_rate.txt` | Đối chiếu Kafka: tổng vs event_id duy nhất (loại 1,50%) |
| `04_window_results.txt` | Kết quả cửa sổ mẫu (tốc độ TB mỗi tài xế) |
| `05_late_watermark_comparison.txt` | Số liệu ping/cửa sổ giữa dung sai 0 và 5 phút |

Toàn bộ nằm trong `docs/proof/phase5/`.
