# DP3 — Bảng feature offline (chuẩn bị feature store)

Tài liệu mô tả luồng DP3: tính các bảng feature từ tầng Gold, phục vụ mô hình ML ở coursework sau và đặt nền cho feature store (Feast). Mọi số liệu trích từ lần chạy thật, lưu trong `docs/proof/phase8/`.

---

## 1. Tổng quan

Ba bảng feature (`feat_*`), mỗi bảng một thực thể:

| Bảng feature | Thực thể | Giá trị | Số dòng |
|---|---|---|---|
| `feat_customer_order_freq_7d` | customer | số đơn 7 ngày gần nhất | 63.937 |
| `feat_restaurant_avg_prep_time_30d` | restaurant | thời gian chuẩn bị TB 30 ngày | 7.998 |
| `feat_driver_acceptance_rate_7d` | driver | tỷ lệ hoàn tất đơn 7 ngày | 5.000 |

Mỗi bảng có **đúng hai cột bắt buộc theo chuẩn Feast**:

- `event_timestamp` — mốc thời gian mà giá trị feature có hiệu lực (as-of).
- `created` — thời điểm feature được tính ra.

Giống DP2, feature ghi ra hai nơi: **Delta trên lakehouse** (Trino truy vấn) và **mirror PostgreSQL** (DBeaver xem).

### 1.1. Về "N ngày gần nhất" trên dữ liệu ảnh chụp

Dữ liệu là ảnh chụp 180 ngày cố định, nên feature "7 ngày / 30 ngày gần nhất" được tính **tính đến ngày mốc** — ngày đơn mới nhất trong kho: `2026-07-18`. Mỗi thực thể cho một dòng, `event_timestamp` bằng ngày mốc đó. Khi đưa vào Feast, đây chính là điểm-thời-gian để truy vấn feature đúng bối cảnh (point-in-time correctness).

---

## 2. Ba feature

### 2.1. feat_customer_order_freq_7d
Số đơn của mỗi khách trong 7 ngày trước ngày mốc — tín hiệu mức độ hoạt động gần đây, hữu ích cho dự đoán huỷ đơn / churn.

```python
orders.filter(order_time >= ref - 7 days)
      .groupBy("customer_id").agg(count("*").alias("order_freq_7d"))
```

### 2.2. feat_restaurant_avg_prep_time_30d
Thời gian chuẩn bị trung bình của mỗi quán, tính trên các đơn trong 30 ngày. Đặc trưng phía nhà hàng cho bài toán dự đoán ETA.

### 2.3. feat_driver_acceptance_rate_7d
Tỷ lệ đơn hoàn tất trên tổng đơn được gán cho tài xế trong 7 ngày. Ví dụ trích thật:

```
driver_id   acceptance_rate_7d  orders_7d  event_timestamp        created
DRV003485   0.9459              37         2026-07-18 23:59:00    2026-07-21 22:06:00
DRV002219   0.8571              35         2026-07-18 23:59:00    2026-07-21 22:06:00
```

---

## 3. Luồng DP3 trên Airflow

DAG `dp3_compute_features` có hai giai đoạn:

```
start → [ INGEST STAGE: build_features → refresh_trino ]
      → [ VALIDATE STAGE: validate_features ]
      → end
```

- **Ingest stage**: `build_features` (Spark tính 3 feature từ Gold → ghi Delta + mirror Postgres) → `refresh_trino` (làm mới cache).
- **Validate stage**: `validate_features` kiểm tra số dòng, **đủ hai cột `event_timestamp` + `created`**, và khoá thực thể không rỗng.

Spark chạy qua `docker exec`; khoá MinIO/Postgres lấy từ Airflow Connection bằng Jinja, không viết cứng.

### 3.1. Cấu trúc mã nguồn
```
jobs/spark/dp3_build_features.py   # Spark tính 3 feature từ Gold (dùng lại helper của DP2)
dags/dp3_compute_features.py       # DAG hai giai đoạn
dags/dp3/validate.py               # kiểm tra event_timestamp/created + số dòng
```

---

## 4. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_airflow_dag_dp3.png` | Airflow UI: DAG `dp3_compute_features` với hai giai đoạn Ingest/Validate |
| `02_feature_tables.txt` | Số dòng, cấu trúc (có `event_timestamp`+`created`), mẫu giá trị 3 bảng feature |
| `03_dbeaver_feature.png` | DBeaver: bảng feature với đúng hai cột bắt buộc `event_timestamp` + `created` |
| `04_airflow_dag_run.txt` | Trạng thái các task của lần chạy DAG DP3 (thành công) |

Tệp `.txt` đã sinh sẵn. Ảnh `.png` chụp từ Airflow UI (`localhost:8080`) và DBeaver (PostgreSQL `warehouse`, schema `gold`, các bảng `feat_*`).
