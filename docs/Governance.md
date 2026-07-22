# Phase 9 — Quản trị dữ liệu với DataHub

Tài liệu mô tả cách đưa **lineage**, **data validation** và **data contract** của ba pipeline DP1/DP2/DP3 lên DataHub. Mọi số liệu trích từ lần chạy thật, lưu trong `docs/proof/phase9/`.

---

## 1. Kiến trúc

DataHub được triển khai như một cụm riêng (compose project `datahub`) cạnh cụm chính. Vì nặng RAM (Elasticsearch + GMS + frontend + kafka/schema-registry riêng), trên host chật ta tạm dừng Trino/HMS/Spark khi chạy DataHub — dữ liệu Delta/Postgres của Phase 7/8 vẫn còn nguyên.

```
docker/datahub/dh-v0.14.1.yml   # compose quickstart (không neo4j) — ES + GMS + frontend + mysql + kafka
docker/datahub/datahub.env      # ghim version v0.14.1 + remap port tránh trùng cụm chính
governance/datahub_emit.py      # script đẩy dataset + lineage + assertion + data contract
```

DataHub UI: `localhost:9002`. GMS API: `localhost:8083`.

Metadata được đẩy bằng thư viện `datahub` sẵn có trong container `datahub-actions`, nối tới GMS nội bộ. Cách này mô hình hoá các DAG Airflow thành **DataFlow/DataJob** đúng như plugin Airflow của DataHub tạo ra, nên lineage hiện dưới nền tảng `airflow`.

---

## 2. Lineage — pipeline nối tới bảng

Mỗi pipeline được tạo thành một DataFlow (DAG) + DataJob (task đại diện), với danh sách bảng đầu vào/đầu ra. Toàn bộ chuỗi:

```
Nguồn ngoài (postgres source_system, MinIO raw, kafka gps-topic)
      │  DP1 (dp1_ingest_bronze)
      ▼
Bronze (8 bảng raw_*)
      │  DP2 (dp2_transform_gold)
      ▼
Silver (stg_*) + Gold (dim_*, fact_*)
      │  DP3 (dp3_compute_features)
      ▼
Feature (feat_*)
```

Số liệu lineage đã đẩy (kiểm chứng trên GMS):

| Pipeline | Bảng vào | Bảng ra |
|---|---|---|
| `dp1_ingest_bronze` | 8 | 8 |
| `dp2_transform_gold` | 8 | 9 |
| `dp3_compute_features` | 7 | 3 |

Trên DataHub UI, mở một bảng bất kỳ hoặc một pipeline → tab **Lineage** thể hiện đồ thị nối pipeline với các bảng liên quan — đúng mục "Lineage between the pipeline and tables".

---

## 3. Data validation — assertion

Các luật kiểm tra dữ liệu quan trọng của mỗi DP được tạo thành **Assertion** kèm kết quả chạy **SUCCESS**:

| Assertion | Bảng | Luật |
|---|---|---|
| `dp1-raw-orders-orderid` | bronze.raw_orders | `order_id` không rỗng |
| `dp2-dim-customer-sk` | gold.dim_customer | `customer_sk` (khoá) không rỗng |
| `dp2-dim-customer-iscurrent` | gold.dim_customer | `is_current` (SCD2) không rỗng |
| `dp3-feat-customer-eventts` | gold.feat_customer_order_freq_7d | `event_timestamp` không rỗng |
| `dp3-feat-customer-created` | gold.feat_customer_order_freq_7d | `created` không rỗng |

Các luật này phản ánh đúng những gì Validate stage của DP1/DP2/DP3 kiểm tra thật (số dòng, khoá không rỗng, bất biến SCD2, cột Feast). DataHub đóng vai khung theo dõi kết quả kiểm tra.

---

## 4. Data contract — cam kết dữ liệu

Mỗi DP có một **Data Contract** trên một bảng đại diện, gói các assertion thành cam kết chính thức mà pipeline phải bảo đảm:

| Data Contract | Bảng | Gồm assertion |
|---|---|---|
| `dp1-bronze-orders` | bronze.raw_orders | order_id không rỗng |
| `dp2-gold-customer` | gold.dim_customer | customer_sk + is_current không rỗng |
| `dp3-feat-customer` | gold.feat_customer_order_freq_7d | event_timestamp + created không rỗng |

Trên DataHub UI, mở bảng tương ứng → tab **Quality/Contract** thể hiện contract và trạng thái ACTIVE.

---

## 5. Cách triển khai lại

```bash
# 1) Nhường RAM
docker stop trino hive-metastore spark-worker spark-master

# 2) Dựng DataHub
export HOME=/home/ubuntu
docker compose -p datahub --env-file docker/datahub/datahub.env \
  -f docker/datahub/dh-v0.14.1.yml up -d
# chờ datahub-datahub-gms-1 healthy, mở localhost:9002

# 3) Đẩy metadata governance
docker cp governance/datahub_emit.py datahub-datahub-actions-1:/tmp/
docker exec datahub-datahub-actions-1 python /tmp/datahub_emit.py

# 4) Khi xong, khôi phục cụm chính
docker compose -p datahub -f docker/datahub/dh-v0.14.1.yml down
docker start trino hive-metastore spark-master spark-worker
```

---

## 6. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_datahub_lineage.png` | DataHub UI: đồ thị lineage pipeline ↔ bảng (DP1/DP2/DP3) |
| `02_datahub_assertion_contract.png` | DataHub UI: assertion (validation) + data contract trên một bảng |
| `03_datahub_metadata.txt` | Kiểm chứng lineage/assertion/contract đã lên GMS (đã có sẵn) |

Tệp `.txt` đã sinh sẵn. Ảnh `.png` chụp từ DataHub UI (`localhost:9002`).
