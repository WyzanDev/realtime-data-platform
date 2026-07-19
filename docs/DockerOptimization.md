# Docker Optimization — Tối ưu ảnh vùng chứa

Tài liệu này mô tả các kỹ thuật đã dùng để giảm dung lượng ảnh Docker của bộ sinh dữ liệu, kèm số liệu đo được trước và sau khi tối ưu.

---

## 1. Bối cảnh

Hệ thống dùng Docker Compose để khởi động 14 vùng chứa từ 9 ảnh khác nhau. Việc tối ưu được chia thành bốn nhóm, theo mức độ can thiệp và mức rủi ro:

| Nhóm | Ảnh | Cách làm |
|---|---|---|
| Tự dựng, áp dụng multistage | `data-generator` | Tách tầng dựng và tầng chạy |
| Tự dựng, nhúng sẵn thư viện | `project-airflow` | Thay cách cài lúc chạy |
| Thay bằng phiên bản rút gọn | `redis`, `postgres` | Đổi sang bản Alpine |
| Ghim phiên bản | `minio`, `mc`, `kafka-ui` | Bỏ nhãn `latest` |

Ba ảnh còn lại — Spark, Flink, Kafka — được giữ nguyên, lý do trình bày ở mục 4.

Để đo được hiệu quả của multistage, dự án giữ lại hai tệp Dockerfile cho bộ sinh dữ liệu:

| Tệp | Mục đích |
|---|---|
| `docker/data-generator/Dockerfile.naive` | Phiên bản viết theo cách phổ biến nhất, dùng làm mốc |
| `docker/data-generator/Dockerfile` | Phiên bản đã tối ưu, dùng để chạy thật |

---

## 2. Kết quả đo

Số liệu lấy từ `docs/proof/phase2/01_image_size.txt`, đo ngày 20 tháng 7 năm 2026. Dung lượng ghi ở đây là dung lượng nén của ảnh, khác với con số `docker images` hiển thị.

### 2.1. Ảnh tự dựng, áp dụng multistage

| Ảnh | Trước | Sau | Giảm |
|---|---|---|---|
| `data-generator` | 644,8 MB | 164,0 MB | **480,8 MB — 74,5%** |
| Số lớp | 11 | 9 | |

### 2.2. Ảnh thay bằng phiên bản rút gọn

| Ảnh | Trước | Sau | Giảm |
|---|---|---|---|
| `redis` | 41,5 MB | 15,5 MB | 26,0 MB — 62,6% |
| `postgres` | 150,7 MB | 109,8 MB | 40,9 MB — 27,1% |

### 2.3. Ảnh Airflow

| Ảnh | Dung lượng |
|---|---|
| `apache/airflow:2.9.3` (gốc) | 387,6 MB |
| `project-airflow:2.9.3` | 389,6 MB |

Ảnh này **không giảm dung lượng**, thậm chí tăng 2 MB vì thư viện được nhúng sẵn thay vì tải về lúc chạy. Lợi ích nằm ở chỗ khác, trình bày ở mục 3.7.

### 2.4. Ảnh giữ nguyên

| Ảnh | Dung lượng | Lý do giữ |
|---|---|---|
| `bitnamilegacy/spark:3.5` | 825,3 MB | Không có bản rút gọn chính thức |
| `flink:1.18-scala_2.12` | 549,6 MB | Bản `java11` nhẹ hơn nhưng dễ vỡ job |
| `bitnamilegacy/kafka:3.7` | 367,6 MB | Đổi image phải cấu hình lại KRaft |

### 2.5. Tổng hợp

**Tổng dung lượng tiết kiệm: 547,7 MB.**

Trong đó phần lớn đến từ một ảnh duy nhất là `data-generator` với 480,8 MB, chiếm gần 88% tổng mức giảm. Điều này phản ánh đúng bản chất của multistage build: kỹ thuật này chỉ áp dụng được cho ảnh tự dựng, và hiệu quả tỷ lệ thuận với lượng công cụ biên dịch mà ảnh gốc mang theo.

Ba phép kiểm chứng ảnh chạy được đều đạt:

```
Tài khoản chạy tiến trình: appuser
Python 3.11.15, pandas 3.0.3, numpy 2.4.6, pyarrow 25.0.0
Không còn gcc trong ảnh cuối
Provider Kafka nạp được trong ảnh Airflow
```

Lệnh dựng và đo:

```bash
bash docker/build-and-compare.sh
```

---

## 3. Các kỹ thuật đã áp dụng

### 3.1. Ảnh nền rút gọn thay cho ảnh đầy đủ

```dockerfile
# Trước
FROM python:3.11

# Sau
FROM python:3.11-slim
```

Ảnh `python:3.11` dựa trên Debian đầy đủ, kèm theo trình biên dịch, công cụ phát triển, tài liệu hướng dẫn và hàng trăm gói hệ thống. Ảnh `python:3.11-slim` chỉ giữ những gì cần để chạy Python.

Đây là thay đổi một dòng nhưng cho mức giảm lớn nhất trong toàn bộ danh sách. Lý do là phần lớn dung lượng của ảnh đầy đủ nằm ở những thứ chỉ dùng lúc phát triển, không dùng lúc chạy.

Có một lựa chọn nhỏ hơn nữa là `python:3.11-alpine`, dựa trên Alpine Linux. Dự án không chọn phương án này vì Alpine dùng thư viện chuẩn C khác (musl thay vì glibc), khiến nhiều gói Python có phần mã biên dịch sẵn — như `pandas`, `numpy`, `pyarrow` — phải dựng lại từ mã nguồn. Thời gian dựng tăng từ vài phút lên hàng chục phút, và ảnh kết quả đôi khi còn nặng hơn bản slim vì phải kéo theo công cụ biên dịch.

### 3.2. Tách tầng dựng và tầng chạy

Đây là kỹ thuật cốt lõi của multistage build.

```dockerfile
FROM python:3.11-slim AS builder
RUN apt-get install -y build-essential libpq-dev
RUN pip install --prefix=/install -r requirements.txt

FROM python:3.11-slim AS runtime
COPY --from=builder /install /usr/local
```

Một số gói Python cần trình biên dịch C lúc cài đặt — `psycopg2` là ví dụ điển hình, nó phải dịch phần mã kết nối PostgreSQL. Nhưng sau khi cài xong, trình biên dịch không còn cần nữa.

Nếu chỉ có một tầng, `build-essential` và `libpq-dev` sẽ nằm lại vĩnh viễn trong ảnh cuối. Xoá chúng bằng một lệnh `RUN` phía sau cũng không giúp gì, vì Docker chỉ đánh dấu tệp là đã xoá ở lớp mới chứ không thu hồi dung lượng đã chiếm ở lớp cũ.

Cách tách hai tầng giải quyết triệt để: tầng dựng được phép nặng vì nó bị loại bỏ hoàn toàn, chỉ phần thư viện đã cài xong mới đi tiếp.

Điểm cần chú ý: tầng chạy vẫn cần `libpq5` — thư viện chia sẻ mà `psycopg2` gọi tới lúc kết nối cơ sở dữ liệu. Đây là gói khác với `libpq-dev`, vốn kèm theo các tệp tiêu đề chỉ dùng lúc biên dịch. Nhầm hai gói này sẽ dẫn tới lỗi thiếu thư viện lúc chạy.

### 3.3. Cài thư viện vào thư mục riêng

```dockerfile
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt
```

Tuỳ chọn `--prefix` gom toàn bộ thư viện đã cài vào một thư mục duy nhất, thay vì để chúng nằm rải rác trong hệ thống. Nhờ vậy tầng chạy chỉ cần một lệnh `COPY --from=builder /install /usr/local` là lấy được đủ.

Không có tuỳ chọn này thì phải chép nhiều đường dẫn riêng lẻ và dễ bỏ sót — chẳng hạn các tệp thực thi trong `bin/` hoặc thư viện chia sẻ nằm ngoài thư mục `site-packages`.

Tuỳ chọn `--no-cache-dir` yêu cầu pip không lưu lại các tệp tải về. Với danh sách thư viện của dự án, phần cache này chiếm khoảng vài chục megabyte và hoàn toàn vô dụng sau khi cài xong.

### 3.4. Sắp xếp thứ tự lệnh để tận dụng cache

```dockerfile
# Trước — sai thứ tự
COPY . .
RUN pip install -r requirements.txt

# Sau — đúng thứ tự
COPY requirements.txt .
RUN pip install --prefix=/install -r requirements.txt
COPY data_generator/ ./data_generator/
```

Docker lưu cache theo từng lớp. Khi một lớp thay đổi, mọi lớp phía sau đều phải dựng lại.

Ở phiên bản chưa tối ưu, `COPY . .` đứng trước lệnh cài đặt. Chỉ cần sửa một dòng chú thích trong mã nguồn là lớp đó thay đổi, kéo theo lệnh `pip install` phải chạy lại toàn bộ — bước tốn nhiều thời gian nhất trong cả quá trình dựng.

Ở phiên bản đã tối ưu, chỉ tệp `requirements.txt` được chép trước. Tệp này hiếm khi thay đổi, nên lớp cài đặt thư viện gần như luôn dùng lại được từ cache. Mã nguồn — thứ thay đổi thường xuyên nhất — được chép ở bước gần cuối.

Kỹ thuật này không làm giảm dung lượng ảnh, nhưng rút ngắn đáng kể thời gian dựng lại trong lúc phát triển.

### 3.5. Gộp lệnh và dọn cache trong cùng một lớp

```dockerfile
# Trước — ba lớp, cache của apt nằm lại trong ảnh
RUN apt-get update
RUN apt-get install -y build-essential
RUN rm -rf /var/lib/apt/lists/*

# Sau — một lớp, cache bị xoá trước khi lớp được đóng
RUN apt-get update \
    && apt-get install --no-install-recommends -y build-essential \
    && rm -rf /var/lib/apt/lists/*
```

Mỗi chỉ thị `RUN` tạo ra một lớp riêng. Nếu tách rời, lớp thứ nhất chứa danh sách gói mà `apt-get update` tải về — khoảng 40 megabyte — và lớp thứ ba xoá nó đi. Nhưng như đã nói ở mục 3.2, việc xoá ở lớp sau không thu hồi được dung lượng ở lớp trước.

Gộp cả ba vào một chỉ thị thì việc xoá diễn ra **trước khi** lớp được đóng lại, nên phần cache không bao giờ được ghi vào ảnh.

Tuỳ chọn `--no-install-recommends` yêu cầu apt chỉ cài đúng gói được yêu cầu, bỏ qua các gói được gợi ý kèm theo. Với `build-essential`, danh sách gợi ý gồm cả tài liệu hướng dẫn và công cụ gỡ lỗi.

### 3.6. Chạy bằng tài khoản thường

```dockerfile
RUN useradd --create-home --shell /bin/bash appuser
COPY --chown=appuser:appuser data_generator/ ./data_generator/
USER appuser
```

Mặc định vùng chứa chạy bằng quyền quản trị. Nếu tiến trình bên trong bị chiếm quyền điều khiển, kẻ tấn công có toàn quyền trong vùng chứa và có thêm cơ hội thoát ra máy chủ qua các lỗ hổng đã biết của nhân hệ điều hành.

Thay đổi này không giảm dung lượng nhưng thuộc nhóm thực hành cơ bản khi đóng gói ứng dụng. Tuỳ chọn `--chown` trong lệnh `COPY` gán quyền sở hữu ngay lúc sao chép, tránh phải thêm một lệnh `chown` riêng vốn sẽ nhân đôi dung lượng của thư mục vừa chép.

---

### 3.7. Nhúng sẵn thư viện vào ảnh Airflow

Đây là thay đổi duy nhất trong tài liệu này **không** nhằm giảm dung lượng.

Cấu hình ban đầu dùng biến môi trường của Airflow để cài thêm provider lúc chạy:

```yaml
# Trước
image: apache/airflow:2.9.3
environment:
  - _PIP_ADDITIONAL_REQUIREMENTS=apache-airflow-providers-apache-kafka==1.4.0
```

Cách này có ba vấn đề.

**Cài lại mỗi lần khởi động.** Bốn container Airflow — init, webserver, scheduler, worker — đều dùng chung khối cấu hình `x-airflow-common`, nên cả bốn cùng chạy `pip install` với danh sách y hệt mỗi lần `docker compose up`. Thời gian khởi động kéo dài thêm vài phút cho công việc lặp lại vô ích.

**Phụ thuộc mạng lúc chạy.** Mất kết nối tới kho gói PyPI thì container không lên được, dù ảnh đã nằm sẵn trên máy. Đây là điểm hỏng không đáng có với một thành phần lẽ ra chỉ cần đọc từ đĩa.

**Không tái lập được.** Đây là vấn đề nghiêm trọng nhất. Chỉ có provider Kafka được ghim phiên bản; nếu sau này thêm thư viện mà quên ghim, hai lần khởi động cách nhau vài tuần có thể cho ra hai môi trường khác nhau.

Cách làm mới dựng sẵn thành một ảnh riêng:

```yaml
# Sau
image: project-airflow:2.9.3
build:
  context: .
  dockerfile: docker/airflow/Dockerfile
```

Thư viện được cài đúng một lần lúc dựng ảnh. Container khởi động là chạy ngay, không cần mạng, và mọi lần chạy đều dùng đúng bộ thư viện đó.

Dockerfile dùng tệp ràng buộc phiên bản do chính Airflow phát hành, nhưng phải tách làm **hai lệnh cài riêng biệt**:

```dockerfile
# Bước 1 — các thư viện thông thường, có tệp ràng buộc
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
    -r /tmp/requirements-airflow.txt

# Bước 2 — provider Kafka, KHÔNG dùng tệp ràng buộc
RUN pip install --no-cache-dir --no-deps \
    apache-airflow-providers-apache-kafka==1.4.0
```

Tệp ràng buộc liệt kê phiên bản của mọi thư viện đã được kiểm thử tương thích với đúng bản Airflow 2.9.3. Không dùng nó thì pip có thể tự nâng cấp một thư viện lõi lên bản mới và làm hỏng Airflow theo cách rất khó chẩn đoán.

Lý do phải tách hai bước nằm ở một mâu thuẫn bất ngờ, trình bày ở mục 3.10.

Cần lưu ý tệp ràng buộc phải khớp phiên bản Python của ảnh. Ảnh `apache/airflow:2.9.3` dùng Python 3.12, nên đường dẫn kết thúc bằng `constraints-3.12.txt`. Dùng nhầm tệp của phiên bản Python khác sẽ khiến pip báo không giải được phụ thuộc, vì các phiên bản được ghim trong đó không tồn tại cho Python 3.12.

### 3.8. Thay ảnh nền bằng phiên bản Alpine

Hai ảnh có phiên bản dựa trên Alpine Linux, nhẹ hơn đáng kể so với bản dựa trên Debian:

```yaml
redis:7        →  redis:7-alpine
postgres:15    →  postgres:15-alpine
```

Với Redis, việc đổi không cần điều chỉnh gì thêm. Redis là chương trình đơn lẻ, không phụ thuộc công cụ hệ thống nào ngoài thư viện chuẩn C. Lệnh kiểm tra sức khoẻ `redis-cli ping` vẫn hoạt động y nguyên.

Với PostgreSQL thì cần một thay đổi nhỏ. Ảnh Alpine không cài sẵn `bash`, trong khi tệp khởi tạo cơ sở dữ liệu của dự án khai báo:

```sh
#!/bin/bash
```

Nếu giữ nguyên dòng này, script sẽ không chạy được và cơ sở dữ liệu `airflow` không được tạo — Airflow sẽ hỏng ở bước di trú lược đồ. Đây là loại lỗi khó truy vết vì thông báo lỗi xuất hiện ở container khác với nơi gây ra nguyên nhân.

Kiểm tra lại nội dung script cho thấy nó chỉ dùng cú pháp POSIX, không có tính năng riêng của bash. Vì vậy chỉ cần đổi dòng đầu:

```sh
#!/bin/sh
```

Sau thay đổi này, script chạy được trên cả hai loại ảnh.

### 3.9. Ghim phiên bản thay cho nhãn `latest`

Ba dịch vụ ban đầu dùng nhãn `latest`:

```yaml
minio/minio:latest
minio/mc:latest
provectuslabs/kafka-ui:latest
```

Thay đổi này **không giảm một byte nào**, nhưng đáng ghi lại hơn cả việc đổi sang Alpine.

Nhãn `latest` không phải một phiên bản cố định mà là con trỏ tới bản mới nhất tại thời điểm tải về. Người chấm bài chạy `docker compose pull` sau vài tuần có thể nhận được bản khác hoàn toàn so với bản dùng lúc phát triển — API thay đổi, biến môi trường bị đổi tên, hoặc hành vi mặc định khác đi. Khi đó đồ án không còn tái lập được, và lỗi phát sinh gần như không thể chẩn đoán vì mã nguồn hoàn toàn không đổi.

MinIO là ví dụ cụ thể: dự án này từng gặp trường hợp các bản MinIO gần đây thay đổi cách khai báo lệnh kiểm tra sức khoẻ, khiến `mc ready local` không còn hợp lệ ở một số bản.

Nguyên tắc chung: mọi ảnh trong tệp compose đều phải ghim phiên bản cụ thể. Nhãn `latest` chỉ phù hợp khi thử nghiệm nhanh trên máy cá nhân.

---

### 3.10. Hai sự cố gặp phải trong quá trình tối ưu

Cả hai đều thuộc loại triệu chứng xuất hiện ở một nơi nhưng nguyên nhân nằm ở nơi khác, nên ghi lại cách chẩn đoán.

**Tệp ràng buộc mâu thuẫn với ràng buộc thực tế của provider Kafka.**

Lệnh cài đặt thất bại với thông báo `ResolutionImpossible`. Phần quan trọng nằm ở giữa nhật ký, dễ bị bỏ sót nếu chỉ xem vài dòng cuối:

```
The conflict is caused by:
    The user requested apache-airflow-providers-apache-kafka==1.4.0
    The user requested (constraint) apache-airflow-providers-apache-kafka==1.5.0
```

Tệp ràng buộc chính thức của Airflow 2.9.3 ghim provider này ở 1.5.0. Nhưng thực nghiệm ở giai đoạn dựng hạ tầng cho thấy bản 1.5.0 yêu cầu Airflow từ 2.11 trở lên, và pip sẽ tự nâng cấp Airflow theo — đúng điều mà tệp ràng buộc lẽ ra phải ngăn chặn.

Cách xử lý là tách provider ra khỏi danh sách thư viện thông thường và cài riêng với tuỳ chọn `--no-deps`. Tuỳ chọn này yêu cầu pip chỉ cài đúng gói được chỉ định, không đụng tới bất kỳ phụ thuộc nào, nhờ vậy Airflow 2.9.3 được giữ nguyên.

Kiểm chứng sau khi dựng xong:

```bash
docker run --rm --entrypoint python project-airflow:2.9.3 -c \
  "import airflow, airflow.providers.apache.kafka; print(airflow.__version__)"
```

Kết quả trả về `2.9.3` xác nhận Airflow không bị nâng cấp, và provider vẫn nạp được.

Cách chẩn đoán: khi gặp `ResolutionImpossible`, tìm cụm từ `The conflict is caused by` trong nhật ký thay vì đọc dòng cuối. Pip luôn in ra chính xác hai bên đang mâu thuẫn.

**Tệp khởi tạo cơ sở dữ liệu thiếu quyền đọc.**

Triệu chứng xuất hiện ở container Airflow:

```
sqlalchemy.exc.OperationalError: database "airflow" does not exist
```

Nhưng nguyên nhân nằm ở container PostgreSQL. Kiểm tra nhật ký của nó cho thấy:

```
/docker-entrypoint-initdb.d/01-init-multiple-dbs.sh: Permission denied
```

Tệp khởi tạo có quyền `-rwx--x--x`: chủ sở hữu đọc và chạy được, nhưng nhóm và người dùng khác chỉ có quyền chạy mà không có quyền đọc. Tiến trình PostgreSQL chạy bằng tài khoản riêng, không phải chủ sở hữu tệp, nên không đọc được nội dung để thực thi.

Điểm dễ nhầm: lệnh `chmod +x` chỉ thêm quyền thực thi mà không thêm quyền đọc. Phải dùng `chmod 755` để cấp đủ cả hai.

Còn một điểm nữa khiến việc sửa lỗi này dễ tưởng nhầm là không hiệu quả: các tệp trong `/docker-entrypoint-initdb.d/` **chỉ chạy khi thư mục dữ liệu còn rỗng**. Sau lần khởi động đầu tiên, PostgreSQL coi như đã khởi tạo xong và bỏ qua hoàn toàn thư mục này, kể cả khi tệp đã được sửa đúng. Vì vậy sau mỗi lần sửa phải xoá ổ đĩa ảo rồi khởi động lại:

```bash
docker compose down
docker volume rm project_postgres_data
docker compose up -d
```

Cách chẩn đoán: khi một container báo lỗi liên quan tới tài nguyên do container khác cung cấp, đọc nhật ký của container cung cấp trước. Thông báo lỗi ở phía tiêu thụ thường chỉ mô tả hậu quả, không nêu nguyên nhân.

Để tránh hẳn loại lỗi quyền tệp này, có thể chuyển tệp khởi tạo sang định dạng `.sql`. PostgreSQL chạy tệp `.sql` bằng `psql` nên chỉ cần quyền đọc, không cần quyền thực thi. Đánh đổi là tệp `.sql` không đọc được biến môi trường, nên tên cơ sở dữ liệu và tài khoản phải viết cứng.

Không phải ảnh nào cũng nên tối ưu. Ba ảnh sau được giữ nguyên sau khi cân nhắc chi phí và rủi ro.

## 4. Ba ảnh giữ nguyên và lý do

Không phải ảnh nào cũng nên tối ưu. Ba ảnh sau được giữ nguyên sau khi cân nhắc chi phí và rủi ro.

**`bitnamilegacy/spark:3.5` — 2,30 GB, ảnh nặng nhất hệ thống.** Không có phiên bản rút gọn chính thức. Ảnh Spark buộc phải kèm theo máy ảo Java, thư viện Hadoop và Scala runtime — đây là phần chiếm gần hết dung lượng và không thể lược bỏ. Tự dựng ảnh Spark tối giản là việc khả thi nhưng tốn nhiều thời gian và dễ thiếu thư viện lúc chạy job.

**`flink:1.18-scala_2.12` — 1,40 GB.** Có biến thể `1.18-java11` nhẹ hơn khoảng vài trăm megabyte. Tuy nhiên các job Flink của dự án chưa được kiểm thử trên bản đó, và sai khác phiên bản Java có thể gây lỗi lúc chạy chứ không lộ ra lúc dựng ảnh. Với khối lượng công việc còn lại, rủi ro này không đáng đánh đổi.

**`bitnamilegacy/kafka:3.7` — 1,05 GB.** Nhà cung cấp Bitnami ngừng phát hành kho ảnh công khai từ tháng 8 năm 2025, nên dự án chuyển sang kho `bitnamilegacy`. Chuyển sang ảnh `apache/kafka` chính thức sẽ nhẹ hơn nhưng đòi hỏi viết lại toàn bộ biến môi trường cấu hình KRaft, và có nguy cơ mất dữ liệu trong các topic hiện có. Vì các topic đang chứa bằng chứng của Phase 1, thay đổi này được để lại làm việc cần làm sau.

---

## 5. Tệp `.dockerignore`

Mọi tệp trong thư mục dự án đều được gửi tới Docker daemon trước khi bắt đầu dựng, kể cả những tệp không hề xuất hiện trong lệnh `COPY`. Đây gọi là ngữ cảnh dựng.

Dự án này có thư mục `output/` chứa hơn 300 megabyte dữ liệu parquet đã sinh. Không loại trừ thì mỗi lần dựng phải chờ Docker nén và truyền toàn bộ khối dữ liệu đó, dù dung lượng ảnh cuối cùng không hề thay đổi.

Ngoài tốc độ, đây còn là vấn đề an toàn: tệp `.env` chứa mật khẩu cơ sở dữ liệu và khoá truy cập MinIO tuyệt đối không được lọt vào ngữ cảnh dựng.

Các nhóm được loại trừ:

| Nhóm | Lý do |
|---|---|
| `output/`, `*.parquet` | Dữ liệu đã sinh, hàng trăm megabyte |
| `.env`, `*.pem`, `*.key` | Thông tin nhạy cảm |
| `.venv/`, `__pycache__/` | Môi trường và tệp tạm của máy phát triển |
| `.git/` | Lịch sử phiên bản, không cần lúc chạy |
| `docs/` | Tài liệu và ảnh chụp bằng chứng |

---

## 6. Kiểm chứng ảnh chạy được

Giảm dung lượng mà ảnh không chạy được thì vô nghĩa. Script đo tự động thực hiện ba phép kiểm tra:

**Tài khoản chạy tiến trình.** Lệnh `whoami` bên trong vùng chứa phải trả về `appuser`, xác nhận không còn chạy bằng quyền quản trị.

**Thư viện nạp được.** Nhập lần lượt `pandas`, `numpy`, `pyarrow`, `yaml`, `boto3`, `sqlalchemy` và in phiên bản. Bước này bắt được lỗi phổ biến nhất của multistage build là chép thiếu thư viện chia sẻ.

**Không còn công cụ biên dịch.** Lệnh `which gcc` phải thất bại. Nếu vẫn tìm thấy `gcc` trong ảnh cuối thì việc tách tầng đã không có tác dụng.

Ngoài ra, Dockerfile khai báo một bước kiểm tra sức khoẻ chạy định kỳ:

```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from data_generator.common.config import load_config; load_config()" || exit 1
```

Bước này nạp thử tệp cấu hình. Nếu hỏng, vùng chứa bị đánh dấu không lành mạnh ngay thay vì chạy tiếp rồi lỗi giữa chừng lúc đang sinh dữ liệu.

---

## 7. Cách sử dụng

Dựng ảnh:

```bash
docker build -f docker/data-generator/Dockerfile -t data-generator:latest .
```

Chạy sinh dữ liệu thử với khối lượng nhỏ:

```bash
docker run --rm \
  --env-file .env \
  --network project_default \
  -v "$(pwd)/output:/app/output" \
  data-generator:latest --mode offline --scale 0.01
```

Bơm dữ liệu vào Kafka:

```bash
docker run --rm \
  --env-file .env \
  --network project_default \
  data-generator:latest --mode streaming --minutes 240
```

Vùng chứa nối vào mạng nội bộ của Docker Compose nên dùng được tên dịch vụ (`minio`, `postgres`, `kafka`) thay vì `localhost`. Đây là điểm khác biệt so với lúc chạy trực tiếp trên máy chủ, khi phải ghi đè bằng biến môi trường `MINIO_ENDPOINT` và `POSTGRES_HOST`.

---

## 8. Phụ lục — danh sách bằng chứng

| Tệp | Nội dung |
|---|---|
| `01_image_size.txt` | Bảng so sánh dung lượng, số lớp, và kết quả ba phép kiểm chứng |
| `02_docker_images.png` | Kết quả `docker images` hiển thị cả hai ảnh |
| `03_container_run.png` | Vùng chứa chạy thành công, sinh dữ liệu |

Toàn bộ tệp nằm trong `docs/proof/phase2/`.
