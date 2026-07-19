#!/bin/sh
# =============================================================================
# Tạo cơ sở dữ liệu thứ hai cho Airflow lúc khởi tạo PostgreSQL
# =============================================================================
#
# Ảnh PostgreSQL chỉ tạo sẵn một cơ sở dữ liệu qua biến POSTGRES_DB. Dự án
# cần hai: một cho kho dữ liệu nghiệp vụ, một cho siêu dữ liệu của Airflow.
# Mọi tệp .sh đặt trong /docker-entrypoint-initdb.d/ được chạy đúng một lần,
# lúc thư mục dữ liệu còn rỗng.
#
# Dùng #!/bin/sh thay vì #!/bin/bash để chạy được cả trên ảnh Alpine, vốn
# không cài sẵn bash. Nội dung script chỉ dùng cú pháp POSIX nên không cần
# tính năng riêng của bash.
# =============================================================================

set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE $AIRFLOW_DB;
    GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_DB TO $POSTGRES_USER;
EOSQL
