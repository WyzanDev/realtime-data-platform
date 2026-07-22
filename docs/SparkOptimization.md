# Phase 4 — Xử lý dữ liệu offline bằng Spark

Tài liệu mô tả cách các Spark job phát hiện và khắc phục bốn vấn đề dữ liệu offline đã được cài sẵn ở khâu sinh dữ liệu (Phase 1). Mỗi vấn đề có một job chạy được hai chế độ — **baseline** (chưa tối ưu, để lộ vấn đề) và bản **đã tối ưu** — nhằm đặt cạnh nhau con số trước và sau. Mọi số liệu dưới đây trích từ lần chạy thật trên cụm Spark một worker 6 nhân / 4 GB (host 8 nhân / 11 GB), đọc dữ liệu Bronze từ MinIO.

---

## 1. Tổng quan

| Vấn đề | Job | Kỹ thuật xử lý | Baseline → Tối ưu |
|---|---|---|---|
| Skew theo thành phố | `job_skew.py` | Salting khoá join | straggler **52,4s → 17,2s** (~3,0×) |
| High cardinality | `job_cardinality.py` | `approx_count_distinct` + broadcast join | xem mục 3 |
| Schema evolution | `job_schema_evolution.py` | `mergeSchema` + phân loại null | mất cột → giữ đủ + gắn cờ |
| Duplicate | `job_dedup.py` | `row_number` theo `ingested_at` | loại đúng 50.000 bản trùng |

Toàn bộ được điều phối bởi Airflow DAG `spark_offline_processing` (mục 7), không chạy `spark-submit` tay.

### 1.1. Cấu trúc mã nguồn

```
jobs/spark/
├── common.py                 # SparkSession + cấu hình S3A + đo thời gian
├── job_skew.py               # skew  → salting
├── job_cardinality.py        # high cardinality → approx_count_distinct + broadcast
├── job_schema_evolution.py   # schema evolution → mergeSchema
└── job_dedup.py              # duplicate → row_number
```

`common.py` gom cấu hình kết nối MinIO (endpoint, khoá đọc từ biến môi trường, **không viết cứng**) để mọi job dùng chung một bộ. Mỗi job nhận tham số `--mode` để chọn chạy baseline hay bản tối ưu.

### 1.2. Vì sao tắt AQE ở các job có skew/join

Spark 3.5 bật sẵn Adaptive Query Execution (AQE), có cơ chế tự chia nhỏ partition lệch và tự đổi kiểu join. Rất tốt cho sản xuất, nhưng ở đây AQE sẽ **che mất** vấn đề mà bài tập muốn minh hoạ và tự tay khắc phục. Nên hai job `job_skew` và `job_cardinality` tắt AQE (`spark.sql.adaptive.enabled=false`) để baseline lộ đúng vấn đề, rồi bản tối ưu chứng minh kỹ thuật thủ công (salting, broadcast) thật sự có tác dụng.

---

## 2. Skew — salting khoá theo thành phố

### 2.1. Vấn đề

Cột `delivery_city` phân phối cực lệch: TP.HCM ~44% tổng đơn, Hà Nội ~30%, phần còn lại là đuôi dài.

```
Ho Chi Minh  1.117.922   (44%)
Ha Noi         757.131   (30%)
Da Nang        258.388
Hai Phong      110.269
Can Tho         94.271
...
```

Nối `raw_orders` (2,55 triệu dòng) với một bảng chiều theo thành phố bằng sort-merge join, rồi gắn cho mỗi đơn một **mã toàn vẹn** (băm SHA-256 lặp 100 vòng — mô phỏng bước làm giàu dữ liệu tốn CPU có thật). Spark băm dữ liệu theo `delivery_city`; toàn bộ đơn TP.HCM dồn về **một** partition ở phía reduce. Phép băm nặng chạy ngay trên partition đó, nên một task phải băm hơn 1,1 triệu dòng trong khi các task khác gần như rảnh — nó thành straggler kéo dài cả stage.

> **Vì sao dùng join.** Phép join buộc shuffle theo khoá, và phép băm nằm sau join nên chạy ở phía reduce — đúng trên các partition đã lệch. Nếu chỉ `repartition` rồi đếm toàn cục, Catalyst sẽ đẩy phép băm ngược lên phía đọc nguồn (cân bằng) và che mất skew. Tắt broadcast (`autoBroadcastJoinThreshold=-1`) để ép sort-merge join; nếu để broadcast, bảng chiều bé xíu được phát tới mọi executor và không có shuffle nào để mà lệch.

### 2.2. Quan sát trên Spark UI

Bằng chứng cốt lõi của skew là **phân bố thời gian task** ở stage sort-merge join (proof `01_skew_baseline_stage.png`, `03_skew_task_distribution.txt`):

| | Baseline | Salted (N=24) |
|---|---|---|
| Số task (stage join) | 48 | 48 |
| Thời gian task — trung vị | 18 ms | 4.559 ms |
| Thời gian task — **max (straggler)** | **52.383 ms** | **17.238 ms** |
| Độ lệch max / trung vị | ~2.900× | ~3,8× |

Một task chạy **52 giây** trong khi trung vị chỉ **18 ms** — chính là straggler ôm trọn đơn TP.HCM.

### 2.3. Cách xử lý — salting

Thêm cột muối ngẫu nhiên `_salt` trị `0..N-1` vào khoá join, đồng thời nhân bản bảng chiều thành N bản, mỗi bản mang một giá trị muối:

```python
orders_salted = orders.withColumn("_salt", (F.rand(seed=42) * n_salt).cast("int"))
dim_salted = dim.crossJoin(spark.range(n_salt).withColumnRenamed("id", "_salt"))
joined = orders_salted.join(dim_salted, on=["delivery_city", "_salt"])
```

Đơn TP.HCM giờ rải đều ra N=24 partition thay vì dồn một chỗ. Kết quả **không đổi** vì mỗi đơn vẫn khớp đúng bản ghi chiều của thành phố nó, chỉ khác qua bản sao muối nào.

### 2.4. Kết quả

| | Straggler (task chậm nhất) | Wall-clock |
|---|---|---|
| Baseline | **52,4 giây** | 60,8 giây |
| Salted, N=24 | **17,2 giây** | 55,9 giây |

Salting giảm straggler **~3,0 lần** (52,4 → 17,2 giây) và đưa phân bố task từ lệch ~2.900× về ~3,8×.

**Ghi chú trung thực về wall-clock.** Trên cụm nhỏ chỉ 6 nhân, phép băm là compute-bound nên tổng thời gian bị chặn dưới bởi *tổng-công-việc / số-nhân*; mức cải thiện wall-clock vì thế khiêm tốn (60,8 → 55,9 giây) dù straggler giảm 3 lần. Salting cho tăng tốc wall-clock tỷ lệ thuận khi số nhân **lớn hơn nhiều** số khoá lệch — lúc đó straggler mới là nút thắt duy nhất. Đây là lý do bằng chứng đúng của skew phải đọc ở **phân bố thời gian task**, không phải ở một con số wall-clock đơn lẻ.

---

## 3. High cardinality — approx_count_distinct + broadcast join

### 3.1. Vấn đề

Vài cột có lực lượng rất lớn: `customer_id` (~120 nghìn UUID), `menu_item_id` (45 nghìn), `restaurant_id` (8 nghìn). Hai nhu cầu cùng bị lực lượng lớn chi phối: đếm phân biệt, và join theo khoá lực lượng lớn.

### 3.2. Phần A — đếm phân biệt

`countDistinct` gom toàn bộ giá trị phân biệt qua shuffle (chính xác nhưng nặng). `approx_count_distinct` dùng HyperLogLog, chỉ giữ một bản phác thảo nhỏ.

| Cột | Chính xác | Xấp xỉ (HLL) | Sai số |
|---|---|---|---|
| `customer_id` | 120.000 | 127.179 | +6,0% |
| `restaurant_id` | 8.000 | 8.115 | +1,4% |
| `menu_item_id` | 45.000 | 43.833 | −2,6% |
| **Thời gian** | **14,5 giây** | **8,2 giây** | ~1,8× nhanh hơn |

Với báo cáo giám sát, sai số vài phần trăm hoàn toàn chấp nhận được để đổi lấy tốc độ và bộ nhớ.

### 3.3. Phần B — join

Nối `raw_order_items` (6,25 triệu dòng) với `raw_menu_items` (45 nghìn dòng) theo `menu_item_id`.

| | Thời gian | Kế hoạch thực thi |
|---|---|---|
| Baseline (sort-merge join) | **11,8 giây** | shuffle cả 6,25 triệu dòng bảng lớn |
| Broadcast join | **8,1 giây** | phát bảng 45 nghìn dòng, không shuffle bảng lớn |

Trên Spark UI, baseline hiện `SortMergeJoin` kèm hai Exchange lớn; bản tối ưu hiện `BroadcastHashJoin` không có Exchange cho bảng lớn (proof `03_*`). Kết quả doanh thu theo nhóm món **khớp từng đồng** giữa hai chế độ.

### 3.4. Khi nào broadcast sai chỗ — và bucketing

Broadcast chỉ đúng khi bảng được phát đủ nhỏ để nằm gọn trong bộ nhớ mỗi executor. Nếu **cả hai** bảng đều lớn (ví dụ nối `raw_orders` 2,55 triệu với `raw_order_items` 6,25 triệu theo `order_id`), không bảng nào broadcast được; ép broadcast sẽ làm tràn bộ nhớ driver. Lúc đó cách đúng là **bucketing** cả hai bảng theo cùng khoá:

```python
df.write.bucketBy(64, "order_id").sortBy("order_id").saveAsTable("...")
```

Hai bảng đã bucket theo cùng khoá, cùng số bucket sẽ join mà không cần shuffle lại — đặc biệt đáng giá khi phép join đó lặp lại nhiều lần. Với quy mô hiện tại của dự án, broadcast bảng danh mục đã đủ, nên bucketing được ghi nhận như hướng đi khi khối lượng tăng.

---

## 4. Schema evolution — mergeSchema

### 4.1. Vấn đề

Các phân vùng `raw_menu_items` nạp **trước** mốc `2026-04-20` không có cột `spice_level`; các phân vùng sau thì có. Parquet lưu lược đồ trong từng file nên hai nhóm phân vùng thật sự khác cấu trúc.

Đọc không bật `mergeSchema`, Spark suy lược đồ từ một tập file bất kỳ. Lần chạy thật cho thấy Spark chọn phải nhóm cũ và **bỏ qua âm thầm** cột `spice_level` — mất toàn bộ dữ liệu độ cay mà không có cảnh báo nào:

```
Có cột spice_level trong lược đồ suy ra? False
  → Cột spice_level đã bị BỎ QUA âm thầm, mất toàn bộ dữ liệu độ cay.
```

Đây là loại lỗi nguy hiểm nhất: không nổ, chỉ cho kết quả thiếu.

### 4.2. Cách xử lý

**Bước 1** — bật `mergeSchema=true` để hợp nhất lược đồ mọi phân vùng, cột `spice_level` luôn hiện diện, dòng phân vùng cũ mang null.

**Bước 2** — phân biệt hai loại null khác hẳn bản chất:

| Loại null | Nguyên nhân | Xử lý |
|---|---|---|
| Do schema evolution | dòng ở phân vùng cũ, cột chưa từng tồn tại | gắn cờ `is_legacy_schema=true`, điền `-1` (không rõ) |
| Hợp lệ | dòng phân vùng mới, món không thuộc nhóm cay | giữ nguyên null |

Dùng cột phân vùng `ingested_date < 2026-04-20` để tách hai loại. Nếu để lẫn, mọi thống kê độ cay về sau sẽ sai vì gộp "chưa có dữ liệu" với "không áp dụng".

### 4.3. Kết quả

```
Tổng dòng sau merge: 45.000
  Dòng lược đồ cũ (spice_level điền -1): 20.143   (~45%, khớp tỷ lệ v1 của bộ sinh)
  Null hợp lệ ở phân vùng mới (món không cay): 9.037
```

---

## 5. Duplicate — khử trùng bằng row_number

### 5.1. Vấn đề

Cổng thanh toán retry khi thiếu ACK, tạo ~2% đơn trùng `order_id`. Bản trùng giống hệt bản gốc, chỉ khác `ingested_at` muộn hơn vài giây đến vài phút.

```
Tổng dòng: 2.550.000 | order_id phân biệt: 2.500.000 | trùng: 50.000 (2,00%)
```

### 5.2. Vì sao không dùng dropDuplicates

`dropDuplicates(["order_id"])` giữ **một bản bất kỳ**, không đảm bảo là bản mới nhất. Với dữ liệu retry, bản đến sau mới phản ánh trạng thái cuối, nên giữ nhầm bản cũ có thể mất thông tin đã cập nhật.

### 5.3. Cách xử lý

Đánh số trong mỗi nhóm `order_id` theo `ingested_at` giảm dần rồi giữ dòng số 1 — bản nạp muộn nhất. Cách này cho kết quả **xác định**, không phụ thuộc thứ tự Spark đọc file:

```python
window = Window.partitionBy("order_id").orderBy(F.col("ingested_at").desc())
deduped = orders.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")
```

### 5.4. Kết quả

```
Trước: 2.550.000 dòng | Sau khử trùng: 2.500.000 dòng | Đã loại: 50.000 bản trùng
```

Loại đúng 50.000 bản đã tiêm, khớp tuyệt đối với tỷ lệ 2% của bộ sinh dữ liệu.

---

## 6. Cách chạy tay từng job

Cụm Spark chạy bằng UID không có tên trong `/etc/passwd`, nên `docker exec` phải truyền lại ba biến `nss_wrapper` mà entrypoint đặt lúc chạy, cùng khoá MinIO:

```bash
docker exec \
  -e HOME=/tmp \
  -e LD_PRELOAD=/opt/bitnami/common/lib/libnss_wrapper.so \
  -e NSS_WRAPPER_PASSWD=/opt/bitnami/spark/tmp/nss_passwd \
  -e NSS_WRAPPER_GROUP=/opt/bitnami/spark/tmp/nss_group \
  -e MINIO_ENDPOINT=http://minio:9000 \
  -e MINIO_ACCESS_KEY=minio -e MINIO_SECRET_KEY=<secret> \
  spark-master spark-submit --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  /opt/spark-jobs/job_skew.py --mode baseline    # hoặc --mode salted --salt 24
```

---

## 7. Tích hợp vào Airflow

DAG `spark_offline_processing` chạy các job đã tối ưu theo thứ tự:

```
start → dedup_orders → process_skew → process_cardinality → resolve_schema → end
```

Airflow không nhồi Spark vào image của mình mà ra lệnh cho container `spark-master` chạy hộ qua `docker exec` (`BashOperator`). Ưu điểm: tận dụng image Spark đã có sẵn gói kết nối MinIO, image Airflow gần như không phình.

Khoá MinIO **không viết cứng**: lấy từ Airflow Connection `minio_s3` ngay trong lệnh bằng Jinja `{{ conn.minio_s3.login }}` / `{{ conn.minio_s3.password }}`, đồng nhất nguyên tắc của DP1. Trong nhật ký Airflow, giá trị bí mật được che thành `***`.

Hạ tầng cần cho tích hợp (đã cấu hình sẵn trong `docker-compose.yml`):
- `airflow-worker` mount `/var/run/docker.sock` và có `group_add: ["115"]` (gid group `docker` của host) để gọi được `docker`.
- Image `project-airflow` thêm một tệp thực thi `docker` (client CLI tĩnh), không kèm daemon.

Chi tiết proof: xem mục 8.

---

## 8. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_skew_baseline_stage.png` | Spark UI stage sort-merge join baseline: task lớn nhất 52s lệch hẳn (straggler) |
| `02_skew_salted_stage.png` | Sau salting: task chậm nhất còn ~20s, phân bố cân bằng hơn |
| `03_skew_task_distribution.txt` | Số liệu phân bố thời gian task baseline vs salted |
| `04_job_timings.txt` | Thời gian chạy baseline vs tối ưu của cả bốn job |
| `05_cardinality_join_plan.png` | So sánh SortMergeJoin (baseline) và BroadcastHashJoin (tối ưu) |
| `06_airflow_dag_success.png` | Airflow UI: DAG `spark_offline_processing` chạy thành công |
| `07_airflow_task_command.png` | Log task cho thấy lệnh docker exec + credential lấy từ Connection (được che) |
| `08_history_server_apps.png` | Spark History Server liệt kê các app đã chạy |

Toàn bộ nằm trong `docs/proof/phase4/`.
```
