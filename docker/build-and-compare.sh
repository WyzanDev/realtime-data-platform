#!/usr/bin/env bash
# =============================================================================
# Dựng các ảnh tự build và đo dung lượng toàn hệ thống trước, sau tối ưu.
#
# Chạy từ thư mục gốc dự án:
#   bash docker/build-and-compare.sh
#
# Kết quả ghi vào docs/proof/phase2/01_image_size.txt
# =============================================================================

set -euo pipefail

PROOF_DIR="docs/proof/phase2"
mkdir -p "$PROOF_DIR"

# Đọc dung lượng ảnh theo byte. Trả về 0 nếu ảnh chưa tồn tại trên máy, để
# script không dừng giữa chừng khi một ảnh nào đó chưa được kéo về.
img_bytes() {
    docker image inspect "$1" --format='{{.Size}}' 2>/dev/null || echo 0
}

to_mb() {
    echo "scale=1; $1 / 1048576" | bc
}

echo "==========================================================="
echo "BƯỚC 1 — Kéo các ảnh phiên bản rút gọn về máy"
echo "==========================================================="
docker pull redis:7-alpine
docker pull postgres:15-alpine

echo ""
echo "==========================================================="
echo "BƯỚC 2 — Dựng ảnh bộ sinh dữ liệu, phiên bản chưa tối ưu"
echo "==========================================================="
docker build -f docker/data-generator/Dockerfile.naive -t data-generator:naive . 2>&1 | tail -5

echo ""
echo "==========================================================="
echo "BƯỚC 3 — Dựng ảnh bộ sinh dữ liệu, phiên bản đã tối ưu"
echo "==========================================================="
docker build -f docker/data-generator/Dockerfile -t data-generator:latest . 2>&1 | tail -5

echo ""
echo "==========================================================="
echo "BƯỚC 4 — Dựng ảnh Airflow riêng của dự án"
echo "==========================================================="
docker build -f docker/airflow/Dockerfile -t project-airflow:2.9.3 . 2>&1 | tail -5

echo ""
echo "==========================================================="
echo "BƯỚC 5 — Tổng hợp số liệu"
echo "==========================================================="

# --- Ảnh tự build ---
DG_NAIVE=$(img_bytes data-generator:naive)
DG_OPT=$(img_bytes data-generator:latest)
AF_BASE=$(img_bytes apache/airflow:2.9.3)
AF_OPT=$(img_bytes project-airflow:2.9.3)

# --- Ảnh thay bằng phiên bản rút gọn ---
REDIS_OLD=$(img_bytes redis:7)
REDIS_NEW=$(img_bytes redis:7-alpine)
PG_OLD=$(img_bytes postgres:15)
PG_NEW=$(img_bytes postgres:15-alpine)

# --- Ảnh giữ nguyên, chỉ liệt kê để thấy toàn cảnh ---
SPARK=$(img_bytes bitnamilegacy/spark:3.5)
FLINK=$(img_bytes flink:1.18-scala_2.12)
KAFKA=$(img_bytes bitnamilegacy/kafka:3.7)

DG_SAVED=$((DG_NAIVE - DG_OPT))
REDIS_SAVED=$((REDIS_OLD - REDIS_NEW))
PG_SAVED=$((PG_OLD - PG_NEW))
TOTAL_SAVED=$((DG_SAVED + REDIS_SAVED + PG_SAVED))

DG_PCT=$(echo "scale=1; $DG_SAVED * 100 / $DG_NAIVE" | bc)
REDIS_PCT=$(echo "scale=1; $REDIS_SAVED * 100 / $REDIS_OLD" | bc)
PG_PCT=$(echo "scale=1; $PG_SAVED * 100 / $PG_OLD" | bc)

DG_LAYERS_NAIVE=$(docker image inspect data-generator:naive --format='{{len .RootFS.Layers}}')
DG_LAYERS_OPT=$(docker image inspect data-generator:latest --format='{{len .RootFS.Layers}}')

{
    echo "==========================================================="
    echo "TỐI ƯU ẢNH DOCKER — TỔNG HỢP TOÀN HỆ THỐNG"
    echo "Thời điểm đo: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "==========================================================="
    echo ""
    echo "A. ẢNH TỰ BUILD — ÁP DỤNG MULTISTAGE"
    echo "-----------------------------------------------------------"
    printf "%-26s %12s %12s %10s\n" "Ảnh" "Trước" "Sau" "Giảm"
    printf "%-26s %11s %11s %9s%%\n" "data-generator" \
        "$(to_mb $DG_NAIVE)MB" "$(to_mb $DG_OPT)MB" "$DG_PCT"
    echo ""
    printf "  Số lớp: %s xuống %s\n" "$DG_LAYERS_NAIVE" "$DG_LAYERS_OPT"
    echo ""
    echo "B. ẢNH THAY BẰNG PHIÊN BẢN RÚT GỌN"
    echo "-----------------------------------------------------------"
    printf "%-26s %12s %12s %10s\n" "Ảnh" "Trước" "Sau" "Giảm"
    printf "%-26s %11s %11s %9s%%\n" "redis" \
        "$(to_mb $REDIS_OLD)MB" "$(to_mb $REDIS_NEW)MB" "$REDIS_PCT"
    printf "%-26s %11s %11s %9s%%\n" "postgres" \
        "$(to_mb $PG_OLD)MB" "$(to_mb $PG_NEW)MB" "$PG_PCT"
    echo ""
    echo "C. ẢNH AIRFLOW — DỰNG SẴN THƯ VIỆN THAY VÌ CÀI LÚC CHẠY"
    echo "-----------------------------------------------------------"
    printf "%-26s %11s\n" "apache/airflow:2.9.3 (gốc)" "$(to_mb $AF_BASE)MB"
    printf "%-26s %11s\n" "project-airflow:2.9.3" "$(to_mb $AF_OPT)MB"
    echo ""
    echo "  Ảnh này KHÔNG giảm dung lượng, thậm chí tăng nhẹ vì thư viện"
    echo "  được nhúng sẵn. Lợi ích nằm ở chỗ khác: bốn container Airflow"
    echo "  không còn phải cài lại thư viện mỗi lần khởi động."
    echo ""
    echo "D. ẢNH GIỮ NGUYÊN"
    echo "-----------------------------------------------------------"
    printf "%-26s %11s  %s\n" "bitnamilegacy/spark:3.5" "$(to_mb $SPARK)MB" "không có bản rút gọn chính thức"
    printf "%-26s %11s  %s\n" "flink:1.18-scala_2.12" "$(to_mb $FLINK)MB" "đổi bản java11 dễ vỡ job"
    printf "%-26s %11s  %s\n" "bitnamilegacy/kafka:3.7" "$(to_mb $KAFKA)MB" "đổi image phải cấu hình lại KRaft"
    echo ""
    echo "==========================================================="
    echo "TỔNG DUNG LƯỢNG TIẾT KIỆM: $(to_mb $TOTAL_SAVED) MB"
    echo "==========================================================="
    echo ""
    echo "E. KIỂM CHỨNG ẢNH CHẠY ĐƯỢC"
    echo "-----------------------------------------------------------"
    echo "Tài khoản chạy tiến trình trong ảnh bộ sinh dữ liệu:"
    docker run --rm --entrypoint whoami data-generator:latest
    echo ""
    echo "Thư viện nạp được:"
    docker run --rm --entrypoint python data-generator:latest -c \
        "import sys, pandas, numpy, pyarrow, yaml, boto3, sqlalchemy; \
         print(f'  Python {sys.version.split()[0]}'); \
         print(f'  pandas {pandas.__version__}'); \
         print(f'  numpy {numpy.__version__}'); \
         print(f'  pyarrow {pyarrow.__version__}')"
    echo ""
    echo "Không còn trình biên dịch trong ảnh cuối:"
    if docker run --rm --entrypoint which data-generator:latest gcc 2>/dev/null; then
        echo "  CẢNH BÁO: vẫn còn gcc"
    else
        echo "  Đã loại bỏ gcc"
    fi
    echo ""
    echo "Provider Kafka đã nằm sẵn trong ảnh Airflow:"
    docker run --rm --entrypoint python project-airflow:2.9.3 -c \
        "import airflow.providers.apache.kafka as k; print(f'  {k.__name__} sẵn sàng')"
} | tee "$PROOF_DIR/01_image_size.txt"

echo ""
echo "Đã ghi kết quả vào $PROOF_DIR/01_image_size.txt"
