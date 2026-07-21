# =============================================================================
# Dockerfile — Flink kèm sẵn PyFlink và trình kết nối Kafka
# =============================================================================
#
# Ảnh gốc flink:1.18-scala_2.12 (Ubuntu 22.04, Java 11) KHÔNG có Python,
# không có PyFlink, cũng không có jar kết nối Kafka. Ba thứ đó đều bắt buộc
# để chạy job DataStream viết bằng Python đọc từ Kafka.
#
# Vì sao dựng sẵn vào ảnh thay vì cài lúc chạy: cả JobManager lẫn
# TaskManager đều phải có đúng cùng một môi trường Python; cài lúc chạy sẽ
# lặp lại trên mọi container và phụ thuộc mạng mỗi lần khởi động.
#
# Cách dựng:
#   docker build -f docker/flink/Dockerfile -t project-flink:1.18 .
#
# =============================================================================

FROM flink:1.18-scala_2.12

# Cài Python 3.10 (mặc định của Ubuntu 22.04, đúng dải PyFlink 1.18 hỗ trợ).
# python-is-python3 để lệnh `python` trỏ vào python3, vì PyFlink gọi `python`.
USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        python3 \
        python3-pip \
        python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

# PyFlink phải khớp đúng phiên bản cụm (1.18.1). Đây là gói nặng (kéo theo
# bản Flink bundled cho phía client) nên tách riêng một lớp để tận dụng cache.
RUN pip3 install --no-cache-dir apache-flink==1.18.1

# Trình kết nối Kafka cho Flink 1.18. Dùng bản "sql-connector" là fat jar đã
# gói sẵn kafka-clients, dùng được cho cả DataStream API lẫn Table/SQL.
RUN wget -nv -O /opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar

# Trả lại tài khoản flink mặc định của ảnh gốc.
USER flink
