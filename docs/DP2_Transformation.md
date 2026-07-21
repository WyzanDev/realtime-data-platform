# DP2 — Biến đổi Bronze → Silver/Gold (mô hình chiều/sự kiện SCD2)

Tài liệu mô tả luồng DP2: biến dữ liệu thô ở tầng Bronze thành mô hình dữ liệu chuẩn hình sao (star schema) với chiều SCD2, đặt trên **lakehouse Delta** và truy vấn qua **Trino**. Mọi số liệu trích từ lần chạy thật, lưu trong `docs/proof/phase7/`.

---

## 1. Kiến trúc lưu trữ (lakehouse + mirror)

DP2 ghi Silver/Gold ra **hai nơi**, mỗi nơi một mục đích:

```
                    ┌──────────────────────────────┐
   Bronze (MinIO)   │  Spark (dp2_build_gold.py)    │
   raw_* parquet ──▶│  dedup → SCD2 → star schema   │
                    └───────────┬──────────┬────────┘
                                │          │
              (nguồn sự thật)   ▼          ▼   (cho ERD)
                   Delta trên MinIO     PostgreSQL (schema gold)
                   s3a://lakehouse/…    dim_*, fact_* + khoá ngoại
                        │                     │
                        ▼                     ▼
                     Trino  ◀── DBeaver ──▶  (đọc quan hệ FK)
```

- **Lakehouse (Delta trên MinIO)**: nguồn sự thật của Silver/Gold. Bảng được ghi bằng `saveAsTable` nên vừa là Delta trên MinIO vừa đăng ký vào **Hive Metastore**; **Trino** đọc qua đó. Đây là kho phân tích chính, mở rộng được.
- **Bản mirror trong PostgreSQL**: Trino/Delta không có khoá ngoại, nên không tự vẽ được quan hệ dim–fact. Bản Postgres có **khoá chính + khoá ngoại thật** để DBeaver export ERD.

### 1.1. Thành phần hạ tầng thêm ở phase này
| Dịch vụ | Vai trò |
|---|---|
| `hive-metastore` | Danh mục bảng (schema, đường dẫn s3a://…), lưu metadata trong PostgreSQL. Chỉ giữ metadata — Spark ghi và Trino đọc mới chạm MinIO. |
| `trino` | Công cụ truy vấn SQL trên lakehouse. Catalog `delta` đọc Delta trên MinIO; catalog `postgres` đọc bản mirror. DBeaver kết nối vào đây. |

---

## 2. Tầng Silver — khử trùng

| Bảng Silver | Nguồn | Xử lý |
|---|---|---|
| `stg_orders` | `raw_orders` | Khử 2% đơn trùng: `row_number` theo `order_id`, giữ bản `ingested_at` mới nhất |
| `stg_delivery_events` | `raw_delivery_events` | Khử 1.5% sự kiện trùng: một bản mỗi `event_id` |

`fact_orders` 2.500.000 dòng (từ 2,55 triệu bản ghi Bronze) và `fact_delivery_events` 197.079 dòng xác nhận khử trùng hoạt động đúng.

---

## 3. Tầng Gold — mô hình hình sao

### 3.1. Bốn bảng chiều SCD2

Mỗi `dim_*` có khoá thay thế `<thực thể>_sk` (PK), khoá nghiệp vụ `<thực thể>_id`, và ba cột SCD2:

```
customer_sk    bigint      NOT NULL   -- khoá thay thế (PK)
customer_id    text                   -- khoá nghiệp vụ
...thuộc tính...
valid_from_ts  timestamp   NOT NULL   -- hiệu lực từ
valid_to_ts    timestamp              -- hết hiệu lực (NULL = còn hiệu lực)
is_current     boolean     NOT NULL   -- có phải phiên bản hiện hành
```

Bất biến SCD2 (kiểm chứng trên Trino): mỗi khoá nghiệp vụ có **đúng một** dòng `is_current`.

```
dim_customer: 120.000 dòng | 120.000 customer_sk phân biệt | 120.000 is_current  ✓
```

Số dòng các chiều: `dim_customer` 120.000, `dim_restaurant` 8.000, `dim_driver` 5.000, `dim_menu_item` 45.000.

`dim_menu_item` đọc bằng `mergeSchema` nên có đủ cột `spice_level` từ các phân vùng v2 (xử lý schema evolution đã nêu ở Phase 4).

### 3.2. Hai bảng sự kiện

`fact_orders` và `fact_delivery_events` tham chiếu tới chiều qua **khoá thay thế** của phiên bản hiện hành (nối lúc dựng). Quan hệ được đóng thành **khoá ngoại thật** trong bản Postgres:

```
fact_orders.customer_sk    → dim_customer.customer_sk
fact_orders.restaurant_sk  → dim_restaurant.restaurant_sk
fact_orders.driver_sk      → dim_driver.driver_sk     (NULL với đơn chưa gán tài xế)
fact_delivery_events.driver_sk → dim_driver.driver_sk
```

Gắn được toàn bộ 6 khoá chính + 4 khoá ngoại mà không vi phạm ràng buộc nào — xác nhận mô hình hình sao toàn vẹn tham chiếu.

### 3.3. Truy vấn hình sao trên lakehouse (Trino)

```sql
SELECT d.city, count(*) AS orders
FROM delta.gold.fact_orders f
JOIN delta.gold.dim_customer d ON f.customer_sk = d.customer_sk
GROUP BY d.city ORDER BY orders DESC;
```
```
Ho Chi Minh 1.120.603 | Ha Noi 755.527 | Da Nang 251.394 | Hai Phong 101.646 | ...
```

Trino nối fact với dim ngay trên Delta ở MinIO — đúng vai trò kho phân tích của lakehouse.

---

## 4. Luồng DP2 trên Airflow

DAG `dp2_transform_gold` có hai giai đoạn:

```
start → [ INGEST STAGE: build_lakehouse_gold → (add_pg_constraints, refresh_trino) ]
      → [ VALIDATE STAGE: validate_gold ]
      → end
```

- **Ingest stage**: `build_lakehouse_gold` (Spark dựng Silver/Gold Delta + mirror Postgres) → `add_pg_constraints` (gắn PK/FK cho ERD) + `refresh_trino` (làm mới cache).
- **Validate stage**: `validate_gold` kiểm tra số dòng, bất biến SCD2 (một phiên bản hiện hành mỗi khoá), và đủ ba cột SCD2.

Spark chạy qua `docker exec` (dùng lại cơ chế của Phase 4). Khoá MinIO và Postgres lấy từ Airflow Connection (`minio_s3`, `postgres_warehouse`) bằng Jinja, **không viết cứng**.

### 4.1. Cấu trúc mã nguồn
```
jobs/spark/dp2_build_gold.py   # Spark: dedup → SCD2 → star → Delta + mirror Postgres
dags/dp2_transform_gold.py     # DAG hai giai đoạn
dags/dp2/common.py             # kết nối MinIO/Postgres (từ Airflow Connection)
dags/dp2/validate.py           # gắn PK/FK + kiểm tra SCD2/số dòng/tham chiếu
```

---

## 5. Xử lý kỹ thuật đáng chú ý

- **Timestamp nanosecond**: vài cột thời gian ở Bronze do pandas ghi ở độ chính xác nanosecond mà Spark không đọc trực tiếp được. Bật `spark.sql.legacy.parquet.nanosAsLong` để đọc dưới dạng long rồi chuyển về timestamp.
- **Hive Metastore + S3**: khi Spark tạo database có location trên MinIO, HMS cần S3A để kiểm tra thư mục — nên HMS được cấp `hadoop-aws` + `core-site.xml` trỏ MinIO.
- **Ghi đè Delta qua catalog Hive**: `DeltaCatalog` không hỗ trợ truncate khi ghi đè bảng có sẵn, nên mỗi bảng được `DROP TABLE` trước rồi `saveAsTable` lại để có kết quả sạch.

---

## 6. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_airflow_dag_dp2.png` | Airflow UI: DAG `dp2_transform_gold` với hai giai đoạn Ingest/Validate |
| `02_dbeaver_erd.png` | DBeaver: sơ đồ quan hệ dim–fact (khoá ngoại) của schema `gold` |
| `03_trino_lakehouse_query.txt` | Trino truy vấn Delta trên MinIO: bảng Gold, bất biến SCD2, star query |
| `04_postgres_scd2_fk.txt` | Cột SCD2 của dim + toàn bộ khoá chính/ngoại (quan hệ dim–fact) |

Hai tệp `.txt` đã sinh sẵn. Hai ảnh `.png` chụp từ Airflow UI (`localhost:8080`) và DBeaver (kết nối PostgreSQL `warehouse`, schema `gold`, hoặc Trino `localhost:8085`).
