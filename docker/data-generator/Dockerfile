# =============================================================================
# Dockerfile — bộ sinh dữ liệu, dựng theo kiểu nhiều tầng (multistage build)
# =============================================================================
#
# Ảnh này đóng gói bộ sinh dữ liệu để chạy được trong pipeline mà không phụ
# thuộc môi trường Python của máy chủ.
#
# Sáu kỹ thuật tối ưu được áp dụng, xem docs/DockerOptimization.md để biết
# mức giảm dung lượng đo được của từng kỹ thuật:
#
#   1. Ảnh nền rút gọn (slim) thay cho ảnh đầy đủ
#   2. Tách tầng dựng và tầng chạy, công cụ biên dịch không đi vào ảnh cuối
#   3. Cài thư viện vào một thư mục riêng rồi chép nguyên khối sang tầng chạy
#   4. Sao chép tệp phụ thuộc trước mã nguồn để tận dụng cache
#   5. Gộp lệnh và dọn cache trong cùng một lớp
#   6. Chạy bằng tài khoản thường thay vì root
#
# Cách dựng:
#   docker build -f docker/data-generator/Dockerfile -t data-generator:latest .
#
# Cách chạy:
#   docker run --rm --env-file .env --network project_default \
#     data-generator:latest --mode offline --scale 0.01
#
# =============================================================================


# -----------------------------------------------------------------------------
# TẦNG 1 — BUILDER: nơi biên dịch và cài đặt thư viện
# -----------------------------------------------------------------------------
# Tầng này được phép nặng vì nó sẽ bị loại bỏ hoàn toàn. Chỉ phần thư viện
# đã cài xong mới được chép sang tầng sau.
FROM python:3.11-slim AS builder

# Không ghi tệp .pyc và không đệm luồng xuất, giúp ảnh gọn hơn và nhật ký
# hiện ra ngay thay vì bị giữ trong bộ đệm.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Cài công cụ biên dịch. Một số gói như psycopg2 cần dịch mã nguồn C nếu
# không có sẵn bản dựng phù hợp cho nền tảng đang dùng.
#
# Ba lệnh được gộp vào một chỉ thị RUN duy nhất. Nếu tách rời, mỗi lệnh tạo
# một lớp riêng và lớp chứa cache của apt sẽ nằm lại trong ảnh dù lớp sau đã
# xoá nó — Docker chỉ đánh dấu tệp là đã xoá chứ không thu hồi dung lượng.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        build-essential \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Chỉ sao chép tệp khai báo thư viện, chưa đụng tới mã nguồn. Nhờ vậy khi
# sửa mã, Docker vẫn dùng lại lớp cài đặt đã lưu trong cache thay vì tải và
# dựng lại toàn bộ thư viện — bước tốn nhiều thời gian nhất.
COPY requirements.txt .

# Cài vào một thư mục riêng thay vì cài vào hệ thống. Cách này cho phép chép
# nguyên khối sang tầng sau bằng một lệnh COPY duy nhất, không cần biết thư
# viện nằm rải rác ở những đâu.
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt


# -----------------------------------------------------------------------------
# TẦNG 2 — RUNTIME: ảnh cuối cùng, chỉ chứa thứ cần để chạy
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# Chỉ cài thư viện dùng lúc chạy, không cài công cụ biên dịch.
# libpq5 là thư viện chia sẻ mà psycopg2 cần khi kết nối PostgreSQL; nó nhỏ
# hơn nhiều so với libpq-dev vốn kèm theo tệp tiêu đề dùng để biên dịch.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        libpq5 \
    && rm -rf /var/lib/apt/lists/*

# Chép toàn bộ thư viện đã cài từ tầng dựng. Mọi công cụ biên dịch, mã nguồn
# trung gian và cache của tầng đó đều bị bỏ lại.
COPY --from=builder /install /usr/local

# Tạo tài khoản thường. Nếu tiến trình bị chiếm quyền điều khiển, kẻ tấn công
# chỉ có quyền của tài khoản này thay vì quyền quản trị trong vùng chứa.
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Sao chép mã nguồn ở bước gần cuối. Mã nguồn là thứ thay đổi thường xuyên
# nhất, nên đặt càng muộn thì càng ít lớp phía sau bị dựng lại.
COPY --chown=appuser:appuser data_generator/ ./data_generator/

# Tạo sẵn thư mục ghi kết quả và trao quyền cho tài khoản thường. Không có
# bước này, tiến trình chạy bằng appuser sẽ không ghi được vào /app vốn
# thuộc quyền quản trị — chương trình chạy xong mọi bước tính toán rồi mới
# thất bại ở bước ghi đĩa.
RUN mkdir -p /app/output && chown appuser:appuser /app/output
VOLUME ["/app/output"]

USER appuser

# Kiểm tra sức khoẻ: xác nhận tệp cấu hình đọc được và các thư viện nạp được.
# Nếu bước này hỏng thì vùng chứa bị đánh dấu không lành mạnh ngay, thay vì
# chạy tiếp rồi hỏng giữa chừng lúc đang sinh dữ liệu.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from data_generator.common.config import load_config; load_config()" || exit 1

ENTRYPOINT ["python", "-m", "data_generator.main"]
CMD ["--mode", "offline", "--scale", "0.01"]
