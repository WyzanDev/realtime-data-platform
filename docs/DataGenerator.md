# Data Generator — Tài liệu thiết kế và kiểm chứng

Tài liệu này mô tả bộ sinh dữ liệu mô phỏng cho nền tảng dữ liệu giao đồ ăn, bao gồm cả nhánh dữ liệu tĩnh (offline) và nhánh dữ liệu luồng (streaming). Toàn bộ số liệu trong tài liệu được đo lại từ dữ liệu thật đã sinh, không phải con số ước lượng.

---

## 1. Tổng quan

Bộ sinh dữ liệu tạo ra hai loại dữ liệu tách biệt, mô phỏng đúng cách một doanh nghiệp giao đồ ăn thật sự lưu trữ và truyền dữ liệu:

| Nhánh | Nội dung | Đích lưu | Khối lượng |
|---|---|---|---|
| Offline | 7 bảng nghiệp vụ, lịch sử 180 ngày | MinIO (parquet) + PostgreSQL | 9,4 triệu dòng |
| Streaming | Sự kiện GPS và mốc trạng thái đơn | Kafka (2 topic) | 729 nghìn sự kiện |

Điểm cốt lõi của thiết kế: **mọi tham số sinh dữ liệu đều nằm trong một file cấu hình YAML duy nhất**, không có giá trị nào bị viết cứng trong mã nguồn. Nhờ vậy có thể đối chiếu trực tiếp giữa tỷ lệ đã khai báo và tỷ lệ đo được — đây chính là cách kiểm chứng generator hoạt động đúng.

### 1.1. Cấu trúc mã nguồn

```
data_generator/
├── main.py                  # Điểm vào, ba chế độ: offline, upload, streaming
├── profile_output.py        # Đo chất lượng dữ liệu offline
├── verify_sinks.py          # Kiểm chứng dữ liệu trên MinIO và PostgreSQL
├── verify_kafka.py          # Kiểm chứng dữ liệu trong Kafka
├── config/
│   └── generator.yaml       # Toàn bộ tham số sinh dữ liệu
├── common/
│   └── config.py            # Nạp cấu hình, tiện ích lấy mẫu có trọng số
├── offline/
│   ├── dimensions.py        # 4 bảng dimension
│   ├── facts.py             # 3 bảng fact
│   └── writer.py            # Ghi parquet, đẩy lên MinIO và PostgreSQL
└── streaming/
    ├── events.py            # Sinh chuyến giao và sự kiện GPS
    └── producer.py          # Bơm vào Kafka, thống kê luồng
```

### 1.2. Cách chạy

```bash
# Nạp biến môi trường
set -a && source .env && set +a
export MINIO_ENDPOINT=http://localhost:9000
export POSTGRES_HOST=localhost
export KAFKA_BOOTSTRAP_SERVERS=localhost:9094

# Sinh dữ liệu offline và đẩy lên hai hệ thống nguồn
python -m data_generator.main --mode offline --out ./output --upload

# Đo chất lượng dữ liệu
python -m data_generator.profile_output --out ./output

# Kiểm chứng dữ liệu đã nằm trên MinIO và PostgreSQL
python -m data_generator.verify_sinks

# Bơm dữ liệu luồng vào Kafka (240 phút mô phỏng)
python -m data_generator.main --mode streaming --minutes 240

# Kiểm chứng dữ liệu trong Kafka
python -m data_generator.verify_kafka --max-messages 200000
```

Tham số `--scale` cho phép chạy thử với khối lượng thu nhỏ mà vẫn giữ nguyên mọi tỷ lệ. Ví dụ `--scale 0.005` sinh 0,5% khối lượng, chạy trong chưa tới 3 giây — dùng để kiểm tra logic trước khi chạy bản đầy đủ.

Tham số `--dry-run` ở chế độ streaming chỉ sinh và thống kê, không kết nối Kafka. Hữu ích khi cần kiểm tra tỷ lệ burst, trễ và trùng lặp mà chưa muốn dựng cụm Kafka.

---

## 2. Lược đồ dữ liệu

### 2.1. Nhóm dimension — thay đổi chậm

Bốn bảng mô tả thực thể có thật, thay đổi chậm theo thời gian.

**`customer`** — 120.000 dòng

| Cột | Kiểu | Vai trò |
|---|---|---|
| `customer_id` | UUID | Khoá chính. Nguồn high cardinality chính |
| `name` | text | Họ tên |
| `phone` | text | Có 1% sai định dạng, làm đầu vào cho Validate stage |
| `email` | text | Có 3% null hợp lệ (khách chưa cung cấp) |
| `city` | text | **Cột gây skew chính**: phân phối lệch nặng |
| `district` | text | Phân cấp dưới city, phục vụ drill-down |
| `signup_date` | date | Dùng cho SCD2 và đặc trưng tuổi tài khoản |
| `segment` | text | `new` / `regular` / `vip`. Thuộc tính thay đổi, dùng demo SCD2 |
| `ingested_at` | timestamp | Khoá phân vùng và cột phá hoà khi khử trùng |

**`restaurant`** — 8.000 dòng

| Cột | Kiểu | Vai trò |
|---|---|---|
| `restaurant_id` | text | Khoá chính. Cardinality trung bình, làm khoá bucketing |
| `name` | text | Tên quán |
| `city`, `district` | text | Địa bàn |
| `category` | text | **Cột gây skew thứ hai**, độc lập với skew theo city |
| `rating` | float | 1.0 đến 5.0, lệch phải |
| `open_hour`, `close_hour` | int | Ràng buộc giờ mở cửa |
| `prep_time_minutes` | int | Đầu vào để tính thời gian giao dự kiến |

**`menu_item`** — 45.000 dòng

| Cột | Kiểu | Vai trò |
|---|---|---|
| `menu_item_id` | text | Khoá chính. Nguồn high cardinality thứ hai |
| `restaurant_id` | text | Khoá ngoại |
| `name`, `category` | text | Tên và nhóm món |
| `price` | int | Phân phối log-normal, 15.000 đến 500.000 VND |
| `is_available` | bool | 5% không còn phục vụ |
| **`spice_level`** | int, nullable | **Chỉ tồn tại từ 2026-04-20 trở đi** — xem mục 3.3 |
| `ingested_at` | timestamp | Khoá phân vùng |

**`driver`** — 5.000 dòng

| Cột | Kiểu | Vai trò |
|---|---|---|
| `driver_id` | text | Khoá chính. Cardinality thấp, làm đối chứng |
| `vehicle_type` | text | `motorbike_gas` 78%, `motorbike_electric` 20%, `car` 2% |
| `city` | text | Tài xế chỉ nhận đơn trong địa bàn |
| `rating` | float | Đặc trưng cho mô hình dự đoán trễ |

Loại xe ảnh hưởng tốc độ giao thông qua hệ số `speed_factor` trong cấu hình, nên đây không phải cột trang trí mà là đầu vào thật cho bài toán dự đoán ở Phase 8.

### 2.2. Nhóm fact — ghi nhận sự kiện

**`order`** — 2.550.000 dòng (đã bao gồm 2% bản trùng)

| Cột | Kiểu | Vai trò |
|---|---|---|
| `order_id` | text | Khoá nghiệp vụ, dùng để khử trùng |
| `customer_id`, `restaurant_id`, `driver_id` | text | Khoá ngoại. `driver_id` null với đơn huỷ trước khi gán tài xế |
| `order_time` | timestamp | **Dồn cụm vào giờ cao điểm**, tạo skew thời gian |
| `status` | text | `completed` 85%, `cancelled` 8%, `failed` 7% |
| `total_amount` | int | Bằng tổng subtotal, trừ 0,3% cố ý làm lệch |
| `payment_method` | text | `cash` 45%, `ewallet` 40%, `bank_transfer` 15% |
| `delivery_city` | text | Denormalize có chủ ý — xem mục 2.3 |
| `distance_km` | float | Quãng đường giao |
| `estimated_delivery_time` | timestamp | Tính từ prep_time, quãng đường, loại xe, giờ cao điểm |
| `actual_delivery_time` | timestamp, nullable | Null với đơn huỷ hoặc thất bại |
| `cancelled_reason_code` | text, nullable | Chỉ có giá trị khi `status = cancelled` |
| `cancelled_reason_text` | text, nullable | Chỉ có khi mã lý do là `other` |
| `ingested_at` | timestamp | Khoá phân vùng, bám theo `order_time` |

**`order_item`** — 6.250.053 dòng (bảng lớn nhất)

Mỗi đơn có 1 đến 4 món. Phép join giữa bảng này và `menu_item` là chỗ shuffle nặng nhất, nơi baseline job ở Phase 4 sẽ lộ vấn đề rõ nhất.

**`review`** — 637.574 dòng (bảng thưa)

Chỉ 30% đơn hoàn tất có đánh giá. Hai cột `customer_id` và `restaurant_id` là dư thừa về mặt chuẩn hoá, được giữ lại có chủ ý để tạo lỗi tham chiếu chéo.

### 2.3. Vì sao denormalize `delivery_city`

Cột này chép sẵn thành phố giao vào bảng `order`, thay vì phải join sang `customer` để lấy. Có hai lý do:

**Lý do nghiệp vụ.** Khách đăng ký ở một thành phố vẫn có thể đặt giao sang thành phố khác — đi công tác, đặt hộ người thân. Generator cố ý tạo 3% đơn như vậy. Địa chỉ giao là thuộc tính của **đơn hàng**, không phải bản sao thừa của thuộc tính khách hàng.

**Lý do kỹ thuật.** Để minh hoạ kỹ thuật salting ở Phase 4, cần gom nhóm theo thành phố. Nếu phải join sang `customer` trước, Spark UI sẽ hiện *hai* vấn đề chồng lên nhau — chi phí shuffle của phép join và độ lệch của phép gom nhóm — rất khó chỉ ra nguyên nhân trong ảnh chụp màn hình. Có cột sẵn thì baseline job chỉ còn một vấn đề duy nhất, và mức cải thiện sau khi salting nhìn rõ ràng.

---

## 3. Các vấn đề dữ liệu được cài vào

Mỗi vấn đề đều có một cột cụ thể chịu trách nhiệm. Không cột nào tồn tại chỉ để cho đủ.

| Vấn đề | Cột phụ trách | Tỷ lệ cấu hình |
|---|---|---|
| Skew theo địa bàn | `customer.city` | HCM 45% |
| Skew theo nhóm món | `restaurant.category` | Cơm 30% |
| Skew theo thời gian | `order.order_time` | 62% dồn vào 6 giờ cao điểm |
| High cardinality | `customer_id`, `menu_item_id` | 120k và 45k |
| Schema evolution | `menu_item.spice_level` | Mốc 2026-04-20 |
| Trùng lặp offline | `order.order_id` | 2,0% |
| Lệch số tiền | `order.total_amount` | 0,3% |
| Sai tham chiếu | `review.restaurant_id` | 0,5% |
| Giá trị ngoại lai | `order_item.quantity` | 3% đơn văn phòng |
| Trễ (streaming) | `event_time` vs `sent_at` | 8,0% |
| Trùng lặp (streaming) | `delivery_event.event_id` | 1,5% |
| Burst (streaming) | Mật độ theo giờ | Gấp 4 lần |

### 3.1. Skew — phân phối lệch

Ba loại skew độc lập nhau, cho phép minh hoạ ba tình huống khác nhau trên cùng tập dữ liệu.

**Skew theo địa bàn.** Thành phố lớn nhất chiếm gần một nửa dữ liệu. Khi Spark gom nhóm theo `delivery_city`, phân vùng chứa thành phố đó ôm gần 45% khối lượng trong khi các phân vùng khác nhàn rỗi — toàn bộ job phải đợi task chậm nhất.

Kết quả đo được (`01_profile_full.txt`, bảng 1):

```
       city  actual_pct  config_pct  diff_pct
Ho Chi Minh       44.90        45.0     -0.10
     Ha Noi       30.10        30.0      0.10
    Da Nang       10.05        10.0      0.05
  Hai Phong        4.10         4.0      0.10
    Can Tho        3.45         3.5     -0.05
   Bien Hoa        3.03         3.0      0.03
  Nha Trang        2.42         2.5     -0.08
        Hue        1.94         2.0     -0.06
```

Sai lệch lớn nhất là 0,10 điểm phần trăm. Cột `diff_pct` chính là bằng chứng generator đọc tham số từ cấu hình chứ không viết cứng.

**Skew theo nhóm món.** Sai lệch lớn nhất 1,01 điểm phần trăm (`01_profile_full.txt`, bảng 2). Đây là skew ở cấp độ nghiệp vụ, khác với skew địa lý.

**Skew theo thời gian.** Sáu giờ cao điểm mỗi giờ chiếm khoảng 10%, các giờ còn lại khoảng 3,2% — chênh nhau hơn ba lần:

```
 order_time   pct
         12 10.68
         13 10.67
         11 10.64
         18 10.02
         20 10.01
         19 10.00
          6  3.18
         21  3.18
          8  3.17
         17  3.17
```

Sự dồn cụm này làm cho các partition theo ngày có kích thước không đều, và là nguyên nhân trực tiếp khiến task Spark chạy lệch nhau.

### 3.2. High cardinality — độ đa dạng giá trị cao

Cardinality là số giá trị phân biệt trong một cột. Cột có cardinality cao tạo ra rất nhiều nhóm nhỏ khi shuffle, tốn bộ nhớ giữ bảng băm, và làm phép đếm chính xác trở nên đắt đỏ.

Kết quả đo được (`01_profile_full.txt`, bảng 6):

```
     table        column  distinct_count  total_rows  ratio cardinality
     order   customer_id          120000     2550000 0.0471        HIGH
     order restaurant_id            8000     2550000 0.0031        HIGH
     order delivery_city               8     2550000 0.0000         LOW
order_item  menu_item_id           45000     6250053 0.0072        HIGH
order_item      order_id         2500000     6250053 0.4000        HIGH
 menu_item      category               8       45000 0.0002         LOW
```

Hai dòng `LOW` được giữ lại có chủ ý làm đối chứng. Chính sự tương phản này quyết định chiến lược lưu trữ:

- `delivery_city` (8 giá trị) — thích hợp làm khoá phân vùng
- `restaurant_id` (8.000 giá trị) — quá nhiều để phân vùng, thích hợp làm khoá bucketing
- `customer_id` (120.000 giá trị) — nên dùng `approx_count_distinct` thay vì `countDistinct` khi thống kê

### 3.3. Schema evolution — lược đồ tiến hoá

Đây là phần đòi hỏi cẩn thận nhất về mặt kỹ thuật.

**Cách làm sai thường gặp:** tạo sẵn cột `spice_level` ngay từ đầu, để null cho dữ liệu cũ. Cách này **không phải** schema evolution, vì file parquet cũ vẫn chứa đầy đủ cột — chỉ là giá trị null. Tuỳ chọn `mergeSchema` của Spark sẽ không có gì để hợp nhất.

**Cách làm đúng:** ghi hai lô parquet bằng hai lệnh độc lập, với số cột thật sự khác nhau. Vì Parquet nhúng lược đồ vào trong từng file, hai thư mục partition sẽ khác nhau về cấu trúc ở mức file.

Kết quả đo được bằng cách đọc thẳng metadata của từng file parquet (`01_profile_full.txt`, bảng 7):

```
 n_columns  has_spice_level  n_partitions          first_partition           last_partition
         7            False            90 ingested_date=2026-01-20 ingested_date=2026-04-19
         8             True            90 ingested_date=2026-04-20 ingested_date=2026-07-18
```

90 partition trước mốc 2026-04-20 có 7 cột, 90 partition sau đó có 8 cột. Ranh giới đúng bằng giá trị `schema_evolution.v2_start_date` trong cấu hình. Đây là bằng chứng ở mức file, không thể nguỵ tạo.

**Hai loại null khác bản chất.** Khi đọc gộp cả hai lô với `mergeSchema` bật, Spark hợp nhất thành 8 cột và tự điền null cho các dòng đến từ lô cũ. Kết quả (`01_profile_full.txt`, bảng 8):

```
_source_schema  rows  spice_null  null_pct
  v1_no_column 20143       20143    100.00
 v2_has_column 24857        9037     36.36
```

- **100% null ở lô v1** — cột chưa tồn tại tại thời điểm đó. Đây là null do lược đồ.
- **36,36% null ở lô v2** — món không thuộc nhóm có độ cay. Đây là null do giá trị không áp dụng.

Cùng hiển thị là `NULL`, nhưng cách xử lý hoàn toàn khác nhau. Loại đầu có thể điền giá trị mặc định theo nhóm món; loại sau là thông tin thật, không được điền bừa. Phân biệt được hai loại này là điều kiện để viết data contract có ý nghĩa ở Phase 9.

### 3.4. Trùng lặp và cột phá hoà

Generator nhân bản 2% số đơn để mô phỏng lỗi ghi trùng khi nạp dữ liệu. Dòng nhân bản **giữ nguyên `order_id`** nhưng có `ingested_at` muộn hơn từ 1 giây đến 10 phút.

Chênh lệch thời gian này là bắt buộc. Nếu hai dòng giống hệt nhau từng cột thì không có căn cứ nào để chọn giữ dòng nào. Có cột phá hoà thì việc khử trùng trở nên xác định:

```sql
row_number() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) = 1
```

Kết quả đo được (`01_profile_full.txt`, bảng 9):

```
 rows_before_dedup  rows_after_dedup  duplicates_removed  actual_dup_rate_pct  config_dup_rate_pct
           2550000           2500000               50000                  2.0                  2.0
```

Đúng 2,0% so với cấu hình 2,0%.

### 3.5. Lỗi tham chiếu và nhất quán

Ba phép kiểm tra sẽ được chạy ở Validate stage của Phase 3. Dữ liệu phải **có lỗi thật** thì bước kiểm tra mới có ý nghĩa — nếu mọi kiểm tra luôn xanh thì Validate stage chỉ là hình thức.

Kết quả đo được (`01_profile_full.txt`, bảng 10):

```
                                      check  violations  actual_pct  config_pct
              total_amount != SUM(subtotal)        7500         0.3         0.3
review.restaurant_id != order.restaurant_id        3186         0.5         0.5
                  đơn huỷ thiếu reason_code           0         0.0         0.0
```

Phép kiểm tra thứ ba luôn cho kết quả 0 — đó là ràng buộc **có điều kiện**: nếu `status = cancelled` thì `cancelled_reason_code` bắt buộc phải có giá trị. Ràng buộc dạng này thuyết phục hơn hẳn ràng buộc kiểu "cột này cho phép null", và sẽ được viết thành data contract ở Phase 9.

### 3.6. Giá trị ngoại lai có logic nghiệp vụ

Khoảng 3% dòng `order_item` có số lượng từ 13 đến 25 phần — mô phỏng đơn đặt tập thể của văn phòng. Nhưng giá trị ngoại lai này không được sinh ngẫu nhiên: nó chỉ xuất hiện khi thoả đồng thời ba điều kiện là món thuộc nhóm cơm, bún hoặc trà sữa; đặt trong khung 11 đến 13 giờ; và rơi vào ngày trong tuần.

Kết quả đo được (`01_profile_full.txt`, bảng 11):

```
 bulk_rows  bulk_pct_of_items  pct_in_lunch_hours  pct_on_weekday  max_quantity
     29875              0.478               100.0           100.0            25
```

100% đơn số lượng lớn nằm trong giờ trưa ngày thường. Giá trị ngoại lai có logic nghiệp vụ khác hẳn nhiễu ngẫu nhiên: nó giải thích được, và tạo ra một đuôi dài trong phân phối `subtotal` mà ta có thể phân tích.

---

## 4. Lưu trữ dữ liệu offline

### 4.1. Hai hệ thống nguồn khác loại

Dữ liệu được ghi vào hai nơi khác nhau, mô phỏng tình huống thật là dữ liệu doanh nghiệp nằm rải rác ở nhiều hệ thống:

| Hệ thống | Bảng | Cách nạp về sau này |
|---|---|---|
| PostgreSQL, schema `source_system` | `customer`, `restaurant`, `driver` | JDBC |
| MinIO, bucket `raw` | `menu_item`, `order`, `order_item`, `review` | S3 API |

Nhờ hai nguồn khác loại, pipeline ingest ở Phase 3 có hai nhánh thật sự khác nhau về kỹ thuật thay vì lặp lại cùng một cách đọc.

### 4.2. Phân vùng theo ngày

Mọi bảng trên MinIO đều phân vùng theo `ingested_date` theo quy ước Hive:

```
raw/order/ingested_date=2026-03-14/part-0000.parquet
```

Quy ước `cột=giá_trị` trong tên thư mục cho phép Spark tự nhận ra cột phân vùng khi đọc, và bỏ qua các thư mục không liên quan khi truy vấn có điều kiện lọc theo ngày.

Điểm quan trọng trong thiết kế: **`ingested_at` bám theo thời điểm nghiệp vụ**, không phải thời điểm chạy generator. Đơn phát sinh ngày nào thì được nạp vào kho ngày đó, trễ từ 5 phút đến 12 giờ. Nếu dùng thời điểm chạy generator thì toàn bộ dữ liệu dồn vào đúng một partition duy nhất, và mất hẳn khả năng minh hoạ partition pruning ở Phase 4 lẫn tối ưu lưu trữ ở Phase 6.

### 4.3. Kết quả kiểm chứng

Đọc ngược từ MinIO và PostgreSQL (`02_verify_sinks.txt`):

```
     table  files  partitions  size_mb
 menu_item    180         180     1.68
     order    181         181   154.10
order_item    181         181   131.45
    review    183         183    41.26

Tổng dung lượng: 328.49 MB trên 725 file
```

```
     table   rows  columns
  customer 120000        9
    driver   5000        8
restaurant   8000       10
```

Ảnh chụp màn hình: `03_minio_raw_buckets.png` và `04_postgres_source_system.png`.

**Một quan sát cho Phase 6.** Bảng `menu_item` có 180 partition nhưng tổng chỉ 1,68 MB, tức trung bình khoảng 9 KB mỗi file. Đây chính là hiện tượng "small files problem" kinh điển: quá nhiều file nhỏ khiến chi phí mở file và đọc metadata vượt xa chi phí đọc dữ liệu thật.

Đối lập với nó, bảng `order` có khoảng 850 KB mỗi file — kích thước hợp lý hơn nhiều. Sự tương phản này là nguyên liệu sẵn có để bàn về gộp file và chọn kích thước partition ở Phase 6.

---

## 5. Dữ liệu luồng

### 5.1. Mô hình sự kiện

Mỗi chuyến giao hàng sinh ra một chuỗi sự kiện theo đúng thứ tự:

1. `PICKED_UP` — tài xế nhận hàng tại quán
2. `EN_ROUTE` — tín hiệu GPS phát mỗi 30 giây trong suốt hành trình
3. `DELIVERED` — giao thành công

Một chuyến trung bình 18 phút sinh khoảng 36 sự kiện. Đây là lý do luồng dữ liệu có khối lượng lớn hơn hẳn dữ liệu tĩnh: một đơn chỉ là một dòng trong bảng `order`, nhưng là hàng chục dòng trong luồng sự kiện.

Toạ độ GPS được nội suy tuyến tính giữa điểm đầu (quán) và điểm cuối (địa chỉ khách), cộng thêm nhiễu nhỏ khoảng 0,0002 độ để đường đi trông tự nhiên. Cách này đủ thực tế cho mục đích phân tích mà không cần dịch vụ định tuyến thật.

Cấu trúc một bản tin:

```json
{
  "event_id": "85865d51-70d9-4fae-8402-b3a812698114",
  "order_id": "ORD001107054",
  "driver_id": "DRV002186",
  "event_type": "PICKED_UP",
  "lat": 10.822082,
  "lon": 106.738432,
  "event_time": "2026-07-19T06:00:12",
  "sent_at": "2026-07-19T06:00:12",
  "sequence_number": 0,
  "city": "Ho Chi Minh"
}
```

### 5.2. Vì sao tách `event_time` và `sent_at`

Đây là quyết định thiết kế quan trọng nhất của nhánh streaming.

- **`event_time`** — thời điểm sự kiện thật sự xảy ra ngoài đời
- **`sent_at`** — thời điểm bản tin được đẩy vào Kafka

Với sự kiện bình thường, hai mốc chênh nhau dưới một giây. Với sự kiện đến muộn, khoảng chênh lên tới vài phút.

Không tách hai mốc này thì **không thể** minh hoạ cơ chế watermark và xử lý dữ liệu đến muộn ở Phase 5. Flink dùng `event_time` để xếp sự kiện vào đúng cửa sổ thời gian mà nó thuộc về, bất kể bản tin đến sớm hay muộn. Còn hiệu giữa hai mốc chính là thứ chứng minh cơ chế đó hoạt động đúng.

Trường `sequence_number` đánh số tăng dần trong từng chuyến. Khi sự kiện đến sai thứ tự, chính trường này cho phép nhìn vào kết quả xử lý là biết ngay hệ thống đã sắp xếp lại đúng chưa.

### 5.3. Định tuyến hai topic

| Topic | Nội dung | Số phân vùng |
|---|---|---|
| `gps-topic` | Toàn bộ sự kiện, khối lượng lớn | 6 |
| `order-topic` | Chỉ mốc `PICKED_UP` và `DELIVERED` | 3 |

Khoá phân vùng của cả hai topic đều là `order_id`. Nhờ vậy mọi sự kiện của cùng một đơn luôn rơi vào cùng một phân vùng và giữ nguyên thứ tự tương đối — điều kiện cần để Flink tính được trạng thái theo từng đơn.

### 5.4. Ba vấn đề của dữ liệu luồng

**Burst — dồn tải theo giờ.** Giờ cao điểm sinh sự kiện gấp 4 lần giờ thường. Đây không phải dồn ngẫu nhiên mà bám đúng khung giờ ăn trưa và ăn tối, giống hành vi thật của người dùng ứng dụng đặt đồ ăn.

Kết quả chạy đủ 16 giờ mô phỏng ở chế độ khô:

```
    Giờ   Số sự kiện    Tỷ lệ  Loại giờ
      6      152,741    2.46%  bình thường
     10      183,268    2.95%  bình thường
     11      645,123   10.39%  cao điểm
     12      728,873   11.74%  cao điểm
     13      730,244   11.76%  cao điểm
     14      270,926    4.36%  bình thường
     18      644,113   10.37%  cao điểm
     19      729,342   11.74%  cao điểm
     20      730,494   11.76%  cao điểm

  Trung bình mỗi giờ cao điểm:       701,365 sự kiện
  Trung bình mỗi giờ bình thường:    181,977 sự kiện
  Hệ số burst thực tế:  3.85 lần
  Hệ số burst cấu hình: 4.00 lần
```

Hệ số đo được 3,85 thấp hơn 4,00 một chút vì các giờ chuyển tiếp (14 giờ và 21 giờ) vẫn còn sự kiện của chuyến bắt đầu trong giờ cao điểm — chuyến giao kéo dài nên sự kiện tràn sang giờ sau. Đây là hành vi đúng, không phải sai lệch.

**Late arrival — sự kiện đến muộn.** Khoảng 8% sự kiện có `sent_at` muộn hơn `event_time` từ 30 giây đến 7 phút. Mô phỏng tình huống rất thật: điện thoại tài xế mất sóng trong hầm gửi xe, ứng dụng giữ lại tín hiệu rồi gửi dồn khi có mạng trở lại.

Kết quả đo tại nguồn (`05_streaming_stats.txt`):

```
  Số sự kiện trễ: 58,078
  Tỷ lệ thực tế:  7.963%
  Tỷ lệ cấu hình: 8.000%
  Độ trễ nhỏ nhất:   30 giây
  Độ trễ trung bình: 224 giây
  Độ trễ lớn nhất:   443 giây
```

**Duplicate — bản tin bị gửi lại.** Khoảng 1,5% sự kiện bị nhân bản với cùng `event_id`. Mô phỏng cơ chế gửi lại của Kafka producer khi không nhận được xác nhận: bản tin đã tới nơi nhưng phía gửi tưởng thất bại nên gửi thêm lần nữa.

Kết quả: 10.797 bản sao, tỷ lệ 1,480% so với cấu hình 1,500%.

### 5.5. Kết quả kiểm chứng từ Kafka

Đọc ngược 200.000 bản tin từ `gps-topic` (`06_verify_kafka.txt`):

```
Phân bố theo phân vùng:
  Phân vùng 0: 39,748 bản tin
  Phân vùng 1: 39,897 bản tin
  Phân vùng 2: 39,587 bản tin
  Phân vùng 3: 21,182 bản tin
  Phân vùng 4: 19,838 bản tin
  Phân vùng 5: 39,748 bản tin
```

Ba phân vùng nhận khoảng 39.700 bản tin, hai phân vùng còn lại chỉ khoảng 20.000. Đây **không phải lỗi phân phối khoá**: hàm băm phân tán `order_id` đều nhau, nhưng mỗi đơn sinh ra số tín hiệu GPS khác nhau tuỳ độ dài chuyến. Một chuyến 25 phút sinh 50 sự kiện, chuyến 8 phút chỉ sinh 16. Vì thế tổng số bản tin trên mỗi phân vùng không thể bằng nhau tuyệt đối.

Chênh lệch khoảng 2 lần giữa phân vùng nặng nhất và nhẹ nhất là ví dụ thực tế cho vấn đề partition skew ảnh hưởng tới khả năng song song hoá — sẽ được bàn ở Phase 5.

```
 tong_ban_tin  so_event_id_duy_nhat  so_ban_sao_thua  ty_le_thuc_te_pct  ty_le_cau_hinh_pct
       200000                197058             2942              1.493                 1.5
```

Tỷ lệ trùng lặp đo từ Kafka là 1,493%, khớp với 1,5% trong cấu hình.

**Mức độ đảo thứ tự** — kết quả đáng chú ý nhất của toàn bộ phần streaming:

```
 so_don_kiem_tra  so_don_bi_dao_thu_tu  ty_le_pct
            5474                  5004      91.41
```

91,41% số đơn có sự kiện đến Kafka **không theo đúng thứ tự** `sequence_number`. Con số này chứng minh dữ liệu luồng không thể xử lý tuần tự theo thứ tự đến; bắt buộc phải dùng cơ chế watermark dựa trên `event_time` để sắp xếp lại. Đây chính là bài toán mà Phase 5 phải giải quyết.

Ảnh chụp màn hình Kafka UI: `07_kafka_ui_topics.png` — `gps-topic` 729.375 bản tin trên 6 phân vùng, `order-topic` 40.493 bản tin trên 3 phân vùng.

---

## 6. Đối chiếu tổng hợp

Bảng dưới tổng hợp mọi tỷ lệ đã khai báo trong cấu hình và tỷ lệ đo được từ dữ liệu thật.

| Chỉ số | Cấu hình | Đo được | Sai lệch |
|---|---|---|---|
| Skew thành phố lớn nhất | 45,00% | 44,90% | −0,10 pp |
| Skew nhóm món lớn nhất | 30,00% | 31,01% | +1,01 pp |
| Loại xe phổ biến nhất | 78,00% | 78,16% | +0,16 pp |
| Thanh toán tiền mặt | 45,00% | 45,04% | +0,04 pp |
| Trùng lặp offline | 2,000% | 2,000% | 0,000 pp |
| Lệch số tiền | 0,300% | 0,300% | 0,000 pp |
| Sai tham chiếu review | 0,500% | 0,500% | 0,000 pp |
| Đơn số lượng lớn | 3,00% | 2,99% | −0,01 pp |
| Late arrival (streaming) | 8,000% | 7,963% | −0,037 pp |
| Duplicate (streaming) | 1,500% | 1,480% | −0,020 pp |
| Hệ số burst | 4,00 lần | 3,85 lần | −0,15 |

Mọi sai lệch đều dưới 1,1 điểm phần trăm, phần lớn dưới 0,2. Đây là bằng chứng generator đọc tham số từ cấu hình chứ không viết cứng giá trị trong mã.

Tất cả kết quả đều tái tạo được nhờ seed cố định (`meta.seed: 42`), áp dụng cho cả `random` lẫn `numpy`. Chạy lại generator sẽ cho ra đúng bộ dữ liệu cũ — điều kiện cần để các số liệu trong tài liệu này giữ nguyên giá trị.

---

## 7. Hạn chế đã biết

Ba điểm chưa hoàn thiện, ghi lại để minh bạch.

**Phân phối giờ ngoài khung cao điểm còn phẳng.** Hiện tại 12 giờ không thuộc khung cao điểm được chia đều nhau, mỗi giờ khoảng 3,17%. Thực tế 6 giờ sáng và 21 giờ tối không thể có lượng đơn bằng 17 giờ chiều. Cách khắc phục là thay việc chọn đều bằng một hàm trọng số giảm dần về hai đầu ngày. Chưa thực hiện do ưu tiên thời gian.

**Tỷ lệ trễ đo từ Kafka cao hơn tỷ lệ thật.** Script kiểm chứng đọc 200.000 bản tin đầu tiên và báo 9,002%, trong khi tỷ lệ toàn cục đo tại nguồn là 7,963%. Nguyên nhân: sự kiện bị làm trễ có `sent_at` muộn nên bị đẩy về sau trong hàng đợi; cửa sổ đọc đầu tiên vô tình chứa cả sự kiện trễ *của các phút trước đó*. Con số đúng là con số tại nguồn. Muốn đo chính xác từ Kafka thì phải đọc toàn bộ topic thay vì cắt cửa sổ.

**Bảng `menu_item` có quá nhiều file nhỏ.** 180 partition cho 45.000 dòng, trung bình 9 KB mỗi file. Với bảng dimension thay đổi chậm, phân vùng theo ngày là quá mịn. Hướng xử lý là gộp về phân vùng theo tháng hoặc không phân vùng. Vấn đề này được giữ nguyên có chủ ý để làm ví dụ đối chiếu ở Phase 6.

---

## 8. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_profile_full.txt` | 11 bảng đo chất lượng dữ liệu offline |
| `02_verify_sinks.txt` | Kiểm chứng đọc ngược từ MinIO và PostgreSQL |
| `03_minio_raw_buckets.png` | MinIO console, bucket `raw`, danh sách partition của bảng `order` |
| `04_postgres_source_system.png` | DBeaver, schema `source_system`, ba bảng và số dòng |
| `05_streaming_stats.txt` | Thống kê luồng tại nguồn khi bơm vào Kafka |
| `06_verify_kafka.txt` | Kiểm chứng đọc ngược từ Kafka |
| `07_kafka_ui_topics.png` | Kafka UI, hai topic với số phân vùng và số bản tin |

Toàn bộ tệp nằm trong `docs/proof/phase1/`.
