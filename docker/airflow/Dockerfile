# =============================================================================
# Dockerfile — Airflow kèm sẵn provider và thư viện của dự án
# =============================================================================
#
# Thay thế cho cách dùng biến _PIP_ADDITIONAL_REQUIREMENTS trong compose.
#
# Cách cũ có ba vấn đề nghiêm trọng:
#
#   1. Thư viện được cài lại MỖI LẦN container khởi động. Bốn container
#      Airflow (init, webserver, scheduler, worker) đều cài lại cùng một
#      danh sách, làm chậm khởi động vài phút và lặp lại công việc y hệt.
#
#   2. Phụ thuộc mạng lúc chạy. Mất kết nối tới PyPI thì container không
#      lên được, dù ảnh đã có sẵn trên máy.
#
#   3. Không tái lập được. Không ghim phiên bản chính xác thì hai lần khởi
#      động cách nhau vài tuần có thể cho ra hai môi trường khác nhau.
#
# Dựng sẵn vào ảnh giải quyết cả ba: cài đúng một lần lúc dựng, container
# khởi động là chạy ngay, và mọi lần chạy đều dùng đúng bộ thư viện đó.
#
# Cách dựng:
#   docker build -f docker/airflow/Dockerfile -t project-airflow:2.9.3 .
#
# =============================================================================

FROM apache/airflow:2.9.3

# Chuyển sang quyền quản trị để cài gói hệ thống. Ảnh gốc của Airflow mặc
# định chạy bằng tài khoản airflow, không có quyền cài đặt.
USER root

# libpq5 là thư viện chia sẻ mà psycopg2 cần khi kết nối PostgreSQL.
# Ảnh gốc đã có sẵn, nhưng khai báo tường minh để tránh phụ thuộc ngầm vào
# thành phần có thể bị loại bỏ ở phiên bản Airflow sau.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        libpq5 \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Chỉ lấy MỘT tệp thực thi `docker` (client CLI) từ gói tĩnh chính thức,
# không cài cả docker daemon. Luồng spark_offline_processing dùng CLI này
# để ra lệnh `docker exec spark-master spark-submit ...` — Airflow điều
# phối, còn Spark chạy trong container của nó.
#
# Chỉ trích một tệp `docker/docker` trong gói nên phần thêm vào image chỉ
# khoảng vài chục MB, nhẹ hơn nhiều so với cài qua kho apt (kéo theo daemon
# và phụ thuộc). Ghim phiên bản để lần dựng nào cũng ra đúng một CLI.
RUN curl -fsSL https://download.docker.com/linux/static/stable/x86_64/docker-27.3.1.tgz \
        | tar -xz -C /usr/local/bin --strip-components=1 docker/docker \
    && docker --version

# Trả lại quyền cho tài khoản airflow trước khi cài thư viện Python.
# Cài bằng quyền quản trị sẽ đặt tệp sai chủ sở hữu và Airflow không đọc được.
USER airflow

# Sao chép tệp khai báo thư viện trước, tận dụng cache của Docker: sửa DAG
# không làm mất hiệu lực lớp cài đặt này.
COPY --chown=airflow:root docker/airflow/requirements-airflow.txt /tmp/requirements-airflow.txt

# Cài các thư viện thông thường kèm tệp ràng buộc chính thức của Airflow.
# Tệp này ghim phiên bản đã được kiểm thử tương thích, tránh việc pip tự
# nâng cấp một thư viện lõi và làm hỏng Airflow.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt" \
    -r /tmp/requirements-airflow.txt

# Provider Kafka phải cài riêng, KHÔNG dùng tệp ràng buộc.
#
# Lý do: tệp ràng buộc của Airflow 2.9.3 ghim provider này ở 1.5.0, nhưng
# thực nghiệm cho thấy bản 1.5.0 yêu cầu Airflow từ 2.11 trở lên và khiến
# pip tự nâng cấp Airflow, làm hỏng container ngay lần khởi động kế tiếp.
#
# Tuỳ chọn --no-deps yêu cầu pip chỉ cài đúng gói này, không đụng tới bất kỳ
# thư viện phụ thuộc nào — nhờ vậy Airflow 2.9.3 được giữ nguyên.
RUN pip install --no-cache-dir --no-deps \
    apache-airflow-providers-apache-kafka==1.4.0
