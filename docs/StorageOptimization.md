# Phase 6 — Tối ưu tầng lưu trữ

Tài liệu mô tả hai kỹ thuật tối ưu nơi dữ liệu được lưu, kèm số liệu đo trước/sau. Hai tầng lưu trữ:

- **Lakehouse** (Delta Lake trên MinIO): compaction gom tệp nhỏ + Z-order để bỏ qua tệp không cần đọc.
- **Datawarehouse** (PostgreSQL): chỉ mục để tránh quét tuần tự toàn bảng.

Mọi số liệu trích từ lần chạy thật, lưu trong `docs/proof/phase6/`.

---

## 1. Lakehouse — compaction + Z-order (Delta Lake)

### 1.1. Vấn đề

Ghi dữ liệu theo lô nhỏ hoặc theo luồng tạo ra rất nhiều tệp nhỏ. Mỗi tệp là một lần mở/đóng và một lần đọc metadata khi truy vấn, nên hàng trăm tệp nhỏ làm chậm hẳn dù tổng dung lượng không lớn. Ngoài ra, nếu dữ liệu nằm rải rác không theo trật tự nào, truy vấn lọc phải quét mọi tệp.

Job `jobs/spark/job_storage_lakehouse.py` cố ý ghi bảng `raw_orders` (2,55 triệu đơn) thành **200 tệp nhỏ** để mô phỏng tình huống này.

### 1.2. Kỹ thuật

Delta Lake cung cấp lệnh `OPTIMIZE`:

```python
from delta.tables import DeltaTable

DeltaTable.forPath(spark, DELTA_PATH) \
    .optimize() \
    .executeZOrderBy("delivery_city", "restaurant_id")
```

- **Compaction**: gom các tệp nhỏ thành tệp lớn (mặc định tới ~128 MB/tệp).
- **Z-order**: sắp lại dữ liệu theo đường cong lấp đầy không gian trên `delivery_city` và `restaurant_id`, để các dòng có giá trị gần nhau nằm chung tệp. Kết hợp thống kê min/max mà Delta lưu cho mỗi tệp, truy vấn lọc theo hai cột này **bỏ qua được phần lớn tệp** (data skipping).

### 1.3. Kết quả

Truy vấn đối chứng: lọc `delivery_city = 'Ho Chi Minh' AND restaurant_id = 'RST003000'`.

| | Số tệp | Thời gian truy vấn |
|---|---|---|
| Trước tối ưu | 200 | 6,0 giây |
| Sau OPTIMIZE + ZORDER | **1** | **2,5 giây** |

- Compaction gom **200 tệp → 1 tệp** (200 tệp ~250 KiB gộp thành 1 tệp 42 MiB — kiểm chứng trực tiếp trên MinIO).
- Truy vấn nhanh hơn **~2,4 lần** nhờ đọc ít tệp hơn và data skipping của Z-order.
- Kết quả truy vấn không đổi (209 dòng khớp ở cả hai lần), xác nhận tối ưu chỉ đổi cách lưu chứ không đổi dữ liệu.

> Chi phí một lần: OPTIMIZE + ZORDER mất 14,7 giây. Đây là thao tác chạy định kỳ (ví dụ hằng đêm), đổi một lần tốn kém lấy nhiều truy vấn nhanh về sau.

---

## 2. Datawarehouse — indexing (PostgreSQL)

### 2.1. Vấn đề

PostgreSQL đóng vai kho dữ liệu phân tích. Không có chỉ mục, truy vấn lọc phải **quét tuần tự** (Seq Scan) toàn bộ bảng để tìm các dòng thoả điều kiện — thời gian tỉ lệ với kích thước bảng.

Script `jobs/dwh/dwh_indexing.py` nạp 800.000 đơn vào `public.dwh_orders` rồi đo.

### 2.2. Kỹ thuật

Tạo chỉ mục phức hợp trên đúng các cột hay dùng để lọc:

```sql
CREATE INDEX idx_dwh_orders_city_time
    ON public.dwh_orders (delivery_city, order_time);
```

Dùng `EXPLAIN (ANALYZE)` để lấy cả kiểu quét lẫn thời gian thực thi làm bằng chứng khách quan.

### 2.3. Kết quả

Truy vấn đối chứng: `delivery_city = 'Ho Chi Minh'` trong khung trưa ngày 2026-02-10 (cửa sổ hẹp, chọn lọc cao).

| | Kiểu quét | Thời gian thực thi |
|---|---|---|
| Trước (chưa index) | Parallel **Seq Scan** | 29,79 ms |
| Sau (có index) | **Bitmap Heap Scan** (qua index) | **1,26 ms** |

- Nhanh hơn **~24 lần** (29,79 → 1,26 ms).
- Quan trọng hơn con số: kế hoạch truy vấn đổi từ **Seq Scan** (quét cả 800.000 dòng) sang **Bitmap Heap Scan** dùng chỉ mục (nhảy thẳng tới vùng dữ liệu cần) — cùng trả về 1.379 dòng.
- Với bảng càng lớn, khoảng cách này càng giãn ra: Seq Scan tăng tuyến tính theo số dòng, còn Index Scan gần như không đổi.

---

## 3. Cách chạy lại

**Lakehouse (Delta):**
```bash
docker exec -e HOME=/tmp \
  -e LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so \
  -e NSS_WRAPPER_PASSWD=/opt/bitnami/spark/tmp/nss_passwd \
  -e NSS_WRAPPER_GROUP=/opt/bitnami/spark/tmp/nss_group \
  -e MINIO_ENDPOINT=http://minio:9000 -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=<secret> \
  spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /opt/spark-jobs/job_storage_lakehouse.py
```

**DWH (Postgres index):**
```bash
docker cp jobs/dwh/dwh_indexing.py airflow-worker:/tmp/dwh_indexing.py
docker exec -e PG_HOST=postgres -e PG_USER=admin -e PG_PASSWORD=<secret> -e PG_DB=warehouse \
  -e MINIO_ENDPOINT=http://minio:9000 -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=<secret> \
  airflow-worker python /tmp/dwh_indexing.py
```

---

## 4. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_lakehouse_delta_optimize.txt` | Số tệp và thời gian truy vấn trước/sau OPTIMIZE + ZORDER |
| `02_dwh_indexing_explain.txt` | EXPLAIN ANALYZE trước/sau khi tạo chỉ mục (Seq Scan → Index) |

Toàn bộ nằm trong `docs/proof/phase6/`.
