"""
DP1 — Luồng nạp dữ liệu thô vào tầng Bronze.

Luồng gồm hai giai đoạn nối tiếp nhau, thể hiện bằng hai nhóm tác vụ
trên giao diện Airflow:

    ┌─────────────────────────────────────────┐
    │  INGEST STAGE                           │
    │  Nạp song song từ ba hệ thống nguồn:    │
    │    MinIO    → 4 bảng dữ liệu lớn        │
    │    Postgres → 3 bảng danh mục           │
    │    Kafka    → 1 bảng sự kiện luồng      │
    └─────────────────┬───────────────────────┘
                      ▼
    ┌─────────────────────────────────────────┐
    │  VALIDATE STAGE                         │
    │  Kiểm tra song song từng bảng:          │
    │    lược đồ, số dòng, giá trị rỗng,      │
    │    tỷ lệ trùng lặp                      │
    │  Rồi gom thành báo cáo tổng hợp         │
    └─────────────────────────────────────────┘

Ràng buộc thứ tự: toàn bộ giai đoạn nạp phải hoàn tất trước khi giai
đoạn kiểm tra bắt đầu. Lý do là phép kiểm tra trùng lặp cần nhìn toàn bộ
dữ liệu của một bảng; nếu chạy khi mới nạp được một phần, kết quả sẽ sai.

Bên trong mỗi giai đoạn, các tác vụ chạy song song vì chúng độc lập với
nhau. Tám bảng nạp cùng lúc rút ngắn đáng kể thời gian so với chạy tuần tự.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Thêm thư mục dags vào đường dẫn tìm kiếm để nhập được các module con.
# Airflow không tự thêm thư mục chứa DAG vào sys.path cho các gói con.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dp1.common import BRONZE_TABLES  # noqa: E402
from dp1.ingest import ingest_table  # noqa: E402
from dp1.validate import summarize_validation, validate_table  # noqa: E402

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    # Thử lại hai lần trước khi báo hỏng. Phần lớn lỗi ở giai đoạn này là
    # lỗi kết nối tạm thời tới MinIO hoặc Kafka, thường tự khỏi sau vài phút.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="dp1_ingest_bronze",
    description="Nạp dữ liệu thô từ MinIO, PostgreSQL và Kafka vào tầng Bronze",
    default_args=DEFAULT_ARGS,
    # Chạy hằng ngày lúc 2 giờ sáng, thời điểm tải hệ thống thấp nhất.
    schedule="0 2 * * *",
    start_date=datetime(2026, 7, 1),
    # Không chạy bù cho các ngày trong quá khứ. Dữ liệu nguồn là ảnh chụp
    # hiện tại chứ không phân chia theo ngày chạy, nên chạy bù sẽ tạo ra
    # các phân vùng trùng lặp không có ý nghĩa.
    catchup=False,
    # Chỉ cho phép một lượt chạy tại một thời điểm. Hai lượt chạy song
    # song sẽ ghi đè lên cùng phân vùng và cho kết quả không xác định.
    max_active_runs=1,
    tags=["dp1", "bronze", "ingestion"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(
        task_id="start",
        doc_md="Điểm bắt đầu, đánh dấu ranh giới của luồng trên giao diện.",
    )

    # ==================================================================
    # GIAI ĐOẠN 1 — NẠP DỮ LIỆU
    # ==================================================================
    with TaskGroup(
        group_id="ingest_stage",
        tooltip="Nạp song song từ ba hệ thống nguồn vào tầng Bronze",
    ) as ingest_stage:
        for spec in BRONZE_TABLES:
            PythonOperator(
                task_id=f"ingest_{spec.name}",
                python_callable=ingest_table,
                op_kwargs={"spec": spec},
                doc_md=(
                    f"Nạp bảng **{spec.name}** từ nguồn `{spec.source}` "
                    f"(`{spec.source_path}`) vào tầng Bronze.\n\n"
                    f"Dữ liệu được giữ nguyên như nhận được, chỉ thêm ba cột "
                    f"siêu dữ liệu ghi nhận nguồn gốc."
                ),
            )

    # ==================================================================
    # GIAI ĐOẠN 2 — KIỂM TRA CHẤT LƯỢNG
    # ==================================================================
    with TaskGroup(
        group_id="validate_stage",
        tooltip="Kiểm tra lược đồ, số dòng, giá trị rỗng và tỷ lệ trùng lặp",
    ) as validate_stage:
        checks = [
            PythonOperator(
                task_id=f"validate_{spec.name}",
                python_callable=validate_table,
                op_kwargs={"spec": spec},
                doc_md=(
                    f"Kiểm tra bảng **{spec.name}** với bốn phép kiểm tra.\n\n"
                    f"- Lược đồ: đủ {len(spec.required_columns)} cột bắt buộc\n"
                    f"- Số dòng: tối thiểu {spec.min_rows:,}\n"
                    f"- Giá trị rỗng: cột khoá `{spec.primary_key}` không được rỗng\n"
                    f"- Trùng lặp: đo tỷ lệ theo khoá `{spec.primary_key}`\n\n"
                    f"Chỉ lỗi nghiêm trọng mới làm dừng luồng. Trùng lặp và số "
                    f"dòng thấp chỉ ghi cảnh báo, vì tầng Bronze có nhiệm vụ "
                    f"giữ nguyên khiếm khuyết của nguồn."
                ),
            )
            for spec in BRONZE_TABLES
        ]

        summary = PythonOperator(
            task_id="validation_summary",
            python_callable=summarize_validation,
            doc_md=(
                "Gom kết quả kiểm tra của tám bảng thành một báo cáo duy nhất, "
                "tiện cho việc theo dõi và chụp màn hình làm bằng chứng."
            ),
        )

        checks >> summary

    end = EmptyOperator(
        task_id="end",
        doc_md="Điểm kết thúc, xác nhận toàn bộ luồng đã hoàn tất.",
    )

    # Thứ tự thực thi: nạp xong toàn bộ mới kiểm tra.
    start >> ingest_stage >> validate_stage >> end
