# DP1 — Luồng nạp dữ liệu thô vào tầng Bronze

Tài liệu mô tả thiết kế và kết quả chạy của luồng nạp dữ liệu đầu tiên trong hệ thống. Mọi số liệu được trích từ nhật ký chạy thật, lưu trong `docs/proof/phase3/`.

---

## 1. Tổng quan

DP1 chịu trách nhiệm đưa dữ liệu từ các hệ thống nguồn vào tầng Bronze của hồ dữ liệu, kèm bước kiểm tra chất lượng ngay sau khi nạp.

Luồng gồm hai giai đoạn nối tiếp:

```
start
  │
  ▼
┌──────────────────────────────────────────────┐
│  INGEST STAGE                                │
│  8 tác vụ chạy song song                     │
│    MinIO    → raw_orders, raw_order_items,   │
│               raw_menu_items, raw_reviews    │
│    Postgres → raw_customers, raw_restaurants,│
│               raw_drivers                    │
│    Kafka    → raw_delivery_events            │
└──────────────────┬───────────────────────────┘
                   ▼
┌──────────────────────────────────────────────┐
│  VALIDATE STAGE                              │
│  8 tác vụ kiểm tra song song                 │
│    → validation_summary (gom báo cáo)        │
└──────────────────┬───────────────────────────┘
                   ▼
                  end
```

Tổng cộng 19 tác vụ, 2 nhóm tác vụ.

### 1.1. Vì sao hai giai đoạn phải nối tiếp

Toàn bộ giai đoạn nạp phải hoàn tất trước khi giai đoạn kiểm tra bắt đầu. Lý do nằm ở phép kiểm tra trùng lặp: nó cần nhìn toàn bộ dữ liệu của một bảng mới cho kết quả đúng. Nếu chạy khi mới nạp được một phần, tỷ lệ trùng đo được sẽ sai lệch.

Ngược lại, bên trong mỗi giai đoạn các tác vụ chạy song song vì chúng độc lập với nhau. Tám bảng nạp cùng lúc rút ngắn đáng kể thời gian so với chạy tuần tự.

### 1.2. Cấu trúc mã nguồn

```
dags/
├── dp1_ingest_bronze.py     # Định nghĩa luồng và thứ tự tác vụ
└── dp1/
    ├── common.py            # Cấu hình bảng, kết nối, tiện ích đọc ghi
    ├── ingest.py            # Ba hàm nạp cho ba loại nguồn
    └── validate.py          # Bốn phép kiểm tra chất lượng
```

Việc tách mã ra khỏi tệp định nghĩa luồng có hai lợi ích. Thứ nhất, tệp DAG chỉ còn phần khai báo thứ tự, đọc là hiểu ngay cấu trúc. Thứ hai, các hàm nạp và kiểm tra có thể chạy thử độc lập bằng Python thường, không cần dựng cả cụm Airflow — điều này rút ngắn đáng kể vòng lặp sửa lỗi.

---

## 2. Nguồn dữ liệu và bảng đích

Tám bảng được nạp từ ba hệ thống nguồn khác nhau về bản chất, nên cần ba cách đọc riêng.

| Bảng Bronze | Nguồn | Vị trí ở nguồn | Cách đọc |
|---|---|---|---|
| `raw_orders` | MinIO | `raw/order/` | Giao thức S3, đọc parquet |
| `raw_order_items` | MinIO | `raw/order_item/` | Giao thức S3, đọc parquet |
| `raw_menu_items` | MinIO | `raw/menu_item/` | Giao thức S3, đọc parquet |
| `raw_reviews` | MinIO | `raw/review/` | Giao thức S3, đọc parquet |
| `raw_customers` | PostgreSQL | `source_system.customer` | Truy vấn SQL |
| `raw_restaurants` | PostgreSQL | `source_system.restaurant` | Truy vấn SQL |
| `raw_drivers` | PostgreSQL | `source_system.driver` | Truy vấn SQL |
| `raw_delivery_events` | Kafka | `gps-topic` | Consumer, đọc JSON |

Mọi bảng đều theo quy ước đặt tên `raw_*`, đánh dấu rõ đây là tầng thấp nhất của kiến trúc dữ liệu.

### 2.1. Nguyên tắc của tầng Bronze

**Giữ nguyên dữ liệu như nó vốn có.** Không khử trùng, không sửa lỗi, không chuẩn hoá kiểu dữ liệu. Mọi khiếm khuyết của nguồn đều được bảo toàn.

Lý do: nếu Bronze đã bị làm sạch thì không còn cách nào truy vết ngược khi phát hiện logic làm sạch có sai sót. Bronze đóng vai trò bản sao trung thực của nguồn tại thời điểm nạp — nó là điểm tựa để đối chiếu khi có nghi vấn ở các tầng trên.

Ngoại lệ duy nhất là ba cột siêu dữ liệu được thêm vào mỗi bảng:

| Cột | Ý nghĩa |
|---|---|
| `_bronze_ingested_at` | Thời điểm dòng dữ liệu được nạp |
| `_bronze_source` | Hệ thống nguồn: `minio`, `postgres` hoặc `kafka` |
| `_bronze_table` | Tên bảng ở tầng Bronze |

Ba cột này không sửa đổi dữ liệu nghiệp vụ mà ghi nhận thông tin về chính lần nạp. Khi phát hiện sai lệch ở tầng trên, có thể lần ngược về lô nạp nào, từ hệ thống nào, vào lúc nào. Đây cũng là phần thông tin dòng dõi dữ liệu sẽ khai báo cho DataHub ở giai đoạn sau.

Riêng dữ liệu từ Kafka có thêm hai cột `_kafka_partition` và `_kafka_offset`, ghi lại vị trí bản tin trên hàng đợi. Nhờ đó có thể đọc lại đúng bản tin khi cần điều tra một sự kiện cụ thể.

### 2.2. Chiến lược phân vùng

Ba nguồn có ba cách phân vùng khác nhau, phản ánh đúng bản chất dữ liệu.

**MinIO — giữ nguyên phân vùng của nguồn.** Dữ liệu ở tầng Raw đã được chia theo `ingested_date`, và Bronze giữ y nguyên cấu trúc đó. Hai tầng có cây thư mục giống hệt nhau, tiện đối chiếu khi cần truy vết.

**PostgreSQL — một phân vùng theo ngày chạy.** Ba bảng danh mục không có sẵn phân vùng theo thời gian. Chúng được ghi thành một phân vùng mang ngày chạy của luồng, phản ánh đúng bản chất là ảnh chụp trạng thái tại thời điểm nạp.

Ngày dùng ở đây là ngày theo lịch của Airflow chứ không phải ngày hiện tại. Nhờ vậy khi chạy bù cho một ngày trong quá khứ, dữ liệu vẫn rơi đúng phân vùng.

**Kafka — phân vùng theo thời điểm sự kiện xảy ra.** Đây là điểm cần cân nhắc kỹ. Có hai lựa chọn: chia theo thời điểm bản tin tới nơi, hoặc theo thời điểm sự kiện thật sự xảy ra.

Luồng này chọn phương án thứ hai, dùng cột `event_time`. Với sự kiện đến muộn — chiếm khoảng tám phần trăm theo thiết kế của bộ sinh dữ liệu — cách này đảm bảo nó vẫn nằm cùng phân vùng với các sự kiện cùng ngày, thay vì rơi vào ngày nạp. Đây là nền tảng để bước xử lý luồng ở giai đoạn sau tính toán đúng theo cửa sổ thời gian.

---

## 3. Giai đoạn kiểm tra chất lượng

Bốn phép kiểm tra được thực hiện trên mỗi bảng sau khi nạp.

| Phép kiểm tra | Nội dung |
|---|---|
| Lược đồ | Các cột bắt buộc có mặt đầy đủ không |
| Số dòng | Có đạt ngưỡng tối thiểu kỳ vọng không |
| Giá trị rỗng | Cột bắt buộc có bị rỗng ở đâu không |
| Trùng lặp | Khoá nghiệp vụ lặp lại bao nhiêu |

### 3.1. Hai mức nghiêm trọng

Đây là điểm thiết kế quan trọng nhất của giai đoạn này: **không phải phép kiểm tra nào thất bại cũng làm dừng luồng**.

| Mức | Trường hợp | Hành vi |
|---|---|---|
| Nghiêm trọng | Thiếu cột bắt buộc, bảng rỗng, cột khoá bị rỗng | Dừng luồng |
| Cảnh báo | Tỷ lệ trùng lặp cao, số dòng thấp hơn kỳ vọng | Ghi nhận, chạy tiếp |

Phép kiểm tra trùng lặp **không bao giờ** làm dừng luồng, dù tỷ lệ có cao đến đâu. Lý do nằm ở vai trò của tầng Bronze như đã nêu ở mục 2.1: nó phải giữ nguyên dữ liệu như nhận được, kể cả bản trùng. Việc khử trùng thuộc về tầng Silver.

Nếu dừng luồng vì phát hiện trùng lặp, tầng Silver sẽ không bao giờ có dữ liệu để chứng minh cơ chế khử trùng hoạt động đúng.

Phân biệt được hai mức này là điều kiện để bước kiểm tra có ích thay vì trở thành vật cản. Một hệ thống báo động với mọi thứ cũng vô dụng ngang một hệ thống không báo động gì.

### 3.2. Phân biệt hai loại giá trị rỗng

Tương tự, phép kiểm tra giá trị rỗng cũng chia hai mức tuỳ theo cột nào bị rỗng.

Cột khoá nghiệp vụ bị rỗng là lỗi nghiêm trọng, vì không thể định danh được bản ghi thuộc về thực thể nào. Không có khoá thì mọi phép nối bảng và khử trùng phía sau đều vô nghĩa.

Các cột bắt buộc khác bị rỗng chỉ là cảnh báo. Chúng có thể rỗng vì nguồn thiếu dữ liệu chứ không phải vì lỗi nạp — chẳng hạn khách hàng chưa cung cấp địa chỉ thư điện tử.

---

## 4. Kết quả chạy

Số liệu trích từ `04_validation_summary.txt`, lượt chạy ngày 20 tháng 7 năm 2026.

```
========================================================================
TỔNG HỢP KIỂM TRA CHẤT LƯỢNG TẦNG BRONZE
========================================================================
Bảng                        Số dòng   Số cột        Đạt   Cảnh báo
------------------------------------------------------------------------
raw_orders                2,550,000       18       4/4          0
raw_order_items           6,250,053       10       4/4          0
raw_menu_items               45,000       11       4/4          0
raw_reviews                 637,574       11       4/4          0
raw_customers               120,000       12       4/4          0
raw_restaurants               8,000       13       4/4          0
raw_drivers                   5,000       11       4/4          0
raw_delivery_events         200,000       15       4/4          0
------------------------------------------------------------------------
Tổng cộng: 9,815,627 dòng trên 8 bảng, 0 cảnh báo
```

Toàn bộ 32 phép kiểm tra đều đạt, không có cảnh báo nào.

### 4.1. Đối chiếu với dữ liệu nguồn

| Bảng | Số dòng ở nguồn | Số dòng ở Bronze | Khớp |
|---|---|---|---|
| `raw_orders` | 2.550.000 | 2.550.000 | Có |
| `raw_order_items` | 6.250.053 | 6.250.053 | Có |
| `raw_menu_items` | 45.000 | 45.000 | Có |
| `raw_reviews` | 637.574 | 637.574 | Có |
| `raw_customers` | 120.000 | 120.000 | Có |
| `raw_restaurants` | 8.000 | 8.000 | Có |
| `raw_drivers` | 5.000 | 5.000 | Có |

Mọi bảng khớp chính xác tới từng dòng. Đường đi dữ liệu từ hệ thống nguồn tới tầng Bronze không làm mất mát hay nhân bản bản ghi nào.

Riêng `raw_delivery_events` có 200.000 dòng trong khi topic chứa 729.375 bản tin. Đây là giới hạn có chủ ý: tham số `KAFKA_MAX_MESSAGES` ngăn tác vụ chạy vô hạn khi luồng dữ liệu vẫn đang được bơm vào. Với mục đích của dự án, hai trăm nghìn bản tin đã đủ để minh hoạ mọi vấn đề của dữ liệu luồng.

### 4.2. Thời gian chạy

| Chỉ số | Giá trị |
|---|---|
| Thời gian chạy dài nhất | 3 phút 43 giây |
| Thời gian chạy trung bình | 2 phút 02 giây |
| Thời gian chạy ngắn nhất | 22 giây |

Tác vụ nặng nhất là `ingest_raw_order_items` và `validate_raw_order_items`, do bảng này có hơn sáu triệu dòng.

---

## 5. Bốn sự cố gặp phải và cách chẩn đoán

Quá trình đưa luồng vào hoạt động gặp bốn sự cố, mỗi cái thuộc một loại khác nhau. Ghi lại vì cách chẩn đoán có giá trị lâu dài hơn bản thân lỗi.

### 5.1. Tệp DAG thiếu quyền đọc

**Triệu chứng.** Lệnh `airflow dags list` không trả về gì, mà cũng không báo lỗi nhập mã. Giao diện không hiện luồng nào.

**Chẩn đoán.** Kiểm tra tệp bên trong vùng chứa:

```bash
docker exec airflow-scheduler ls -la /opt/airflow/dags/
```

Kết quả cho thấy quyền `-rw-------`: chỉ chủ sở hữu đọc được. Tiến trình Airflow chạy bằng tài khoản khác nên không đọc được nội dung, và bộ lập lịch **âm thầm bỏ qua** thay vì báo lỗi.

**Xử lý.** `chmod 644` cho toàn bộ tệp DAG.

**Bài học.** Khi một thành phần im lặng bỏ qua thay vì báo lỗi, kiểm tra quyền đọc trước tiên. Đây là loại lỗi khó truy vết nhất vì không có thông báo nào để tìm kiếm.

### 5.2. Ảnh Airflow thiếu thư viện

**Triệu chứng.** Bảy tác vụ nạp thành công, riêng `ingest_raw_delivery_events` thất bại.

**Chẩn đoán.** Nhật ký chỉ rõ:

```
File "/opt/airflow/dags/dp1/ingest.py", line 189, in ingest_from_kafka
    from kafka import KafkaConsumer
ModuleNotFoundError: No module named 'kafka'
```

Tệp khai báo thư viện của ảnh Airflow chỉ liệt kê `boto3`, `pandas`, `pyarrow` và `PyYAML`. Trình điều khiển Kafka của Airflow dùng thư viện `confluent-kafka`, trong khi mã nạp gọi trực tiếp `KafkaConsumer` của `kafka-python` — hai thư viện khác nhau.

**Xử lý.** Thêm `kafka-python-ng` vào `docker/airflow/requirements-airflow.txt` rồi dựng lại ảnh.

**Bài học.** Ảnh tự dựng phải bao gồm **mọi** thư viện mà mã trong luồng sử dụng, không chỉ những gì trình điều khiển của Airflow mang theo.

### 5.3. Tệp cấu hình chưa trỏ tới ảnh mới

**Triệu chứng.** Đã dựng lại ảnh và tạo lại vùng chứa, nhưng lỗi thiếu thư viện vẫn còn nguyên.

**Chẩn đoán.** Kiểm tra ảnh mà vùng chứa đang dùng:

```bash
docker inspect airflow-worker --format='{{.Config.Image}}'
```

Kết quả trả về `apache/airflow:2.9.3` — ảnh gốc, không phải ảnh tự dựng. Tệp cấu hình Docker Compose vẫn trỏ tới ảnh gốc, nên ảnh mới dựng xong nằm đó không ai dùng.

**Xử lý.** Sửa `image` trong `x-airflow-common` thành `project-airflow:2.9.3`, thêm khối `build`, và xoá biến `_PIP_ADDITIONAL_REQUIREMENTS` vốn đã trở nên thừa.

**Bài học.** Dựng ảnh và sử dụng ảnh là hai việc tách rời. Sau khi dựng, luôn kiểm tra vùng chứa thật sự đang chạy ảnh nào.

### 5.4. Nhóm tiêu thụ Kafka ghi nhớ vị trí đã đọc

**Triệu chứng.** Thư viện đã có, kết nối Kafka thành công, gia nhập nhóm tiêu thụ thành công — nhưng đọc được **không bản tin nào**.

**Chẩn đoán.** Nhật ký cho thấy consumer chỉ được gán một phân vùng trong tổng số sáu, và không nhận được dữ liệu. Nguyên nhân là tham số `auto_offset_reset="earliest"` chỉ có tác dụng khi nhóm tiêu thụ **chưa từng tồn tại**. Nhóm đã được tạo ở lần chạy trước, nên Kafka đưa nó về vị trí đã ghi nhận, tức cuối hàng đợi.

**Xử lý.** Bỏ hẳn cơ chế nhóm tiêu thụ. Thay vào đó tự gán toàn bộ phân vùng và tua về đầu:

```python
partitions = [TopicPartition(topic, p) for p in sorted(consumer.partitions_for_topic(topic))]
consumer.assign(partitions)
consumer.seek_to_beginning(*partitions)
```

**Bài học.** Nhóm tiêu thụ phù hợp với tiến trình chạy liên tục, nơi việc ghi nhớ vị trí là mong muốn. Với luồng nạp theo lô cần đọc lại toàn bộ mỗi lần chạy, tự gán phân vùng là cách đúng.

### 5.5. Phân vùng tích luỹ qua nhiều lượt chạy

**Triệu chứng.** Sau khi chạy luồng hai lượt, ba bảng danh mục có số dòng gấp đôi: `raw_customers` báo 240.000 thay vì 120.000.

**Chẩn đoán.** Hàm nạp từ PostgreSQL ghi vào phân vùng mang ngày chạy. Hai lượt chạy có ngày khác nhau nên tạo hai phân vùng, mỗi phân vùng chứa một bản đầy đủ. Bước kiểm tra đọc gộp toàn bộ phân vùng nên thấy dữ liệu gấp đôi.

**Xử lý.** Xoá dữ liệu Bronze cũ rồi chạy lại một lượt sạch.

**Điểm đáng chú ý.** Chính phép kiểm tra trùng lặp đã phát hiện ra vấn đề này, và nó được xếp ở mức cảnh báo nên luồng vẫn chạy tới cùng. Đây là ví dụ cho thấy bước kiểm tra hoạt động đúng như thiết kế: phát hiện được vấn đề thật, nhưng không chặn luồng vì đây không phải lỗi dữ liệu hỏng.

Với bảng danh mục thay đổi chậm, cách ghi tích luỹ theo ngày có thể không phải lựa chọn tốt nhất. Phương án thay thế là ghi đè phân vùng cũ mỗi lần chạy. Điều này được ghi nhận là việc cần cân nhắc ở mục 7.

---

## 6. Cách chạy

Điều kiện tiên quyết: các vùng chứa PostgreSQL, MinIO, Kafka và Airflow đang chạy, và dữ liệu nguồn đã được sinh ở giai đoạn trước.

```bash
# Bật luồng và kích hoạt một lượt chạy
docker exec airflow-scheduler airflow dags unpause dp1_ingest_bronze
docker exec airflow-scheduler airflow dags trigger dp1_ingest_bronze

# Theo dõi trạng thái
docker exec airflow-scheduler airflow dags list-runs -d dp1_ingest_bronze
```

Hoặc qua giao diện tại `localhost:8080`.

Lấy báo cáo tổng hợp sau khi chạy xong:

```bash
docker exec airflow-worker bash -c \
  "ls -t /opt/airflow/logs/dag_id=dp1_ingest_bronze/run_id=*/task_id=validate_stage.validation_summary/*.log \
   | head -1 | xargs cat" | grep -A 20 "TỔNG HỢP"
```

Xoá dữ liệu Bronze để chạy lại từ đầu:

```bash
set -a && source .env && set +a
docker exec minio mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
docker exec minio mc rm --recursive --force local/bronze/
```

---

## 7. Việc cần cân nhắc

Ba điểm được ghi nhận nhưng chưa thực hiện.

**Ghi đè thay vì tích luỹ với bảng danh mục.** Như đã phân tích ở mục 5.5, ba bảng danh mục hiện tạo phân vùng mới mỗi lần chạy. Với dữ liệu thay đổi chậm, ghi đè phân vùng cũ sẽ hợp lý hơn và tránh được nhầm lẫn khi đọc gộp.

**Xử lý bảng lớn theo lô.** Bước kiểm tra hiện đọc toàn bộ bảng vào bộ nhớ. Với `raw_order_items` hơn sáu triệu dòng, việc này tốn đáng kể bộ nhớ của vùng chứa. Phép kiểm tra trùng lặp cần nhìn toàn bộ dữ liệu nên không thể chia nhỏ trực tiếp, nhưng có thể thay bằng cấu trúc dữ liệu tiết kiệm bộ nhớ hơn, chẳng hạn tập băm chỉ lưu khoá.

**Chuyển sang Spark cho khối lượng lớn hơn.** Luồng hiện dùng thư viện xử lý dữ liệu chạy trong tiến trình Airflow. Cách này đơn giản và đủ dùng ở quy mô hiện tại, nhưng không mở rộng được. Khi khối lượng tăng, nên chuyển các tác vụ nặng sang cụm Spark.

---

## 8. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_dag_graph.png` | Sơ đồ luồng, hai nhóm tác vụ đã bung, thấy rõ tám tác vụ nạp song song và thứ tự thực thi |
| `02_dag_success.png` | Lưới trạng thái, toàn bộ 19 tác vụ hoàn tất |
| `03_validation_summary.png` | Nhật ký tác vụ tổng hợp, hiển thị bảng kết quả kiểm tra |
| `04_validation_summary.txt` | Bản văn bản của cùng báo cáo, tiện trích dẫn |

Toàn bộ tệp nằm trong `docs/proof/phase3/`.
