"""
Các phép kiểm tra chất lượng dữ liệu ở tầng Bronze.

Bốn nhóm kiểm tra được thực hiện trên mỗi bảng sau khi nạp:

  1. Lược đồ:   các cột bắt buộc có mặt đầy đủ không
  2. Số dòng:   có đạt ngưỡng tối thiểu kỳ vọng không
  3. Giá trị rỗng: cột bắt buộc có bị rỗng ở đâu không
  4. Trùng lặp: khoá nghiệp vụ lặp lại bao nhiêu

Một điểm quan trọng về cách xử lý kết quả: **không phải phép kiểm tra
nào thất bại cũng làm dừng luồng**. Chúng được chia hai mức:

  - Mức nghiêm trọng: thiếu cột bắt buộc, bảng rỗng, cột khoá bị rỗng.
    Đây là dấu hiệu dữ liệu hỏng thật, luồng phải dừng.

  - Mức cảnh báo: tỷ lệ trùng lặp cao, số dòng thấp hơn kỳ vọng.
    Đây là những khiếm khuyết đã biết trước và sẽ được xử lý ở tầng
    Silver. Dừng luồng vì chúng là sai, vì chính tầng Bronze có nhiệm
    vụ giữ nguyên khiếm khuyết của nguồn.

Phân biệt được hai mức này là điều kiện để bước kiểm tra có ích thay vì
trở thành vật cản. Một hệ thống báo động với mọi thứ cũng vô dụng ngang
một hệ thống không báo động gì.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

from dp1.common import (
    BUCKET_BRONZE,
    TableSpec,
    get_s3_client,
    list_s3_objects,
    read_parquet_from_s3,
)

log = logging.getLogger(__name__)

# Ngưỡng tỷ lệ trùng lặp gây cảnh báo, tính theo phần trăm. Bộ sinh dữ
# liệu cố ý tạo hai phần trăm bản trùng ở dữ liệu tĩnh và một phẩy năm
# phần trăm ở dữ liệu luồng, nên ngưỡng năm phần trăm đủ rộng để không
# báo động nhầm nhưng vẫn bắt được trường hợp bất thường.
DUPLICATE_WARN_THRESHOLD = 5.0


@dataclass
class CheckResult:
    """Kết quả của một phép kiểm tra.

    Attributes:
        check: tên phép kiểm tra.
        passed: đạt hay không.
        severity: mức độ, critical hoặc warning.
        detail: mô tả chi tiết để ghi vào nhật ký.
        value: giá trị đo được, dùng cho báo cáo tổng hợp.
    """

    check: str
    passed: bool
    severity: str
    detail: str
    value: float | int | str = ""


def _load_bronze_table(spec: TableSpec) -> pd.DataFrame:
    """Đọc toàn bộ phân vùng của một bảng Bronze vào bộ nhớ.

    Với bảng lớn như raw_order_items, việc này tốn bộ nhớ. Nhưng các
    phép kiểm tra về trùng lặp và giá trị rỗng cần nhìn toàn bộ dữ liệu
    mới cho kết quả đúng, nên không thể kiểm tra theo từng phân vùng
    riêng lẻ.

    Args:
        spec: mô tả bảng cần đọc.

    Returns:
        DataFrame gộp từ mọi phân vùng.

    Raises:
        ValueError: khi không tìm thấy tệp nào.
    """
    client = get_s3_client()
    keys = list_s3_objects(client, BUCKET_BRONZE, f"{spec.name}/")

    if not keys:
        raise ValueError(
            f"Bảng {spec.name} không có tệp nào ở tầng Bronze. "
            f"Bước nạp có thể đã thất bại."
        )

    frames = [read_parquet_from_s3(client, BUCKET_BRONZE, k) for k in keys]
    return pd.concat(frames, ignore_index=True)


def check_schema(df: pd.DataFrame, spec: TableSpec) -> CheckResult:
    """Kiểm tra các cột bắt buộc có mặt đầy đủ trong bảng.

    Đây là phép kiểm tra nghiêm trọng nhất vì nó bảo vệ mọi bước phía
    sau. Nếu thiếu cột mà không phát hiện ở đây, lỗi sẽ nổ ra ở tầng
    Silver hoặc Gold với thông báo khó hiểu hơn nhiều.

    Args:
        df: dữ liệu cần kiểm tra.
        spec: mô tả bảng, chứa danh sách cột bắt buộc.

    Returns:
        Kết quả kiểm tra.
    """
    missing = [c for c in spec.required_columns if c not in df.columns]

    if missing:
        return CheckResult(
            check="schema",
            passed=False,
            severity="critical",
            detail=f"Thiếu cột bắt buộc: {', '.join(missing)}",
            value=len(missing),
        )

    return CheckResult(
        check="schema",
        passed=True,
        severity="critical",
        detail=f"Đủ {len(spec.required_columns)} cột bắt buộc trên tổng {len(df.columns)} cột",
        value=len(df.columns),
    )


def check_row_count(df: pd.DataFrame, spec: TableSpec) -> CheckResult:
    """Kiểm tra số dòng có đạt ngưỡng tối thiểu kỳ vọng không.

    Bảng rỗng hoàn toàn là lỗi nghiêm trọng, thường do bước nạp thất bại
    âm thầm. Bảng có dữ liệu nhưng ít hơn kỳ vọng chỉ là cảnh báo, vì có
    thể do chạy thử với khối lượng thu nhỏ.

    Args:
        df: dữ liệu cần kiểm tra.
        spec: mô tả bảng, chứa ngưỡng tối thiểu.

    Returns:
        Kết quả kiểm tra.
    """
    n = len(df)

    if n == 0:
        return CheckResult(
            check="row_count",
            passed=False,
            severity="critical",
            detail="Bảng rỗng hoàn toàn",
            value=0,
        )

    if n < spec.min_rows:
        return CheckResult(
            check="row_count",
            passed=False,
            severity="warning",
            detail=f"Chỉ có {n:,} dòng, thấp hơn ngưỡng kỳ vọng {spec.min_rows:,}",
            value=n,
        )

    return CheckResult(
        check="row_count",
        passed=True,
        severity="critical",
        detail=f"{n:,} dòng, đạt ngưỡng tối thiểu {spec.min_rows:,}",
        value=n,
    )


def check_nulls(df: pd.DataFrame, spec: TableSpec) -> CheckResult:
    """Kiểm tra giá trị rỗng trong các cột bắt buộc.

    Cần phân biệt hai loại rỗng khác hẳn nhau về bản chất. Cột khoá
    nghiệp vụ bị rỗng là lỗi nghiêm trọng, vì không thể định danh được
    bản ghi thuộc về thực thể nào. Các cột bắt buộc khác bị rỗng thì chỉ
    là cảnh báo, vì có thể do nguồn thiếu dữ liệu chứ không phải lỗi nạp.

    Args:
        df: dữ liệu cần kiểm tra.
        spec: mô tả bảng.

    Returns:
        Kết quả kiểm tra.
    """
    present = [c for c in spec.required_columns if c in df.columns]
    null_counts = {c: int(df[c].isna().sum()) for c in present}
    has_null = {c: n for c, n in null_counts.items() if n > 0}

    # Cột khoá bị rỗng là lỗi nghiêm trọng, xét riêng.
    pk_nulls = null_counts.get(spec.primary_key, 0)
    if pk_nulls > 0:
        return CheckResult(
            check="nulls",
            passed=False,
            severity="critical",
            detail=f"Cột khoá {spec.primary_key} có {pk_nulls:,} giá trị rỗng",
            value=pk_nulls,
        )

    if has_null:
        parts = [f"{c}={n:,}" for c, n in has_null.items()]
        return CheckResult(
            check="nulls",
            passed=False,
            severity="warning",
            detail=f"Có giá trị rỗng ở cột bắt buộc: {', '.join(parts)}",
            value=sum(has_null.values()),
        )

    return CheckResult(
        check="nulls",
        passed=True,
        severity="critical",
        detail=f"Không có giá trị rỗng ở {len(present)} cột bắt buộc",
        value=0,
    )


def check_duplicates(df: pd.DataFrame, spec: TableSpec) -> CheckResult:
    """Đo tỷ lệ trùng lặp theo khoá nghiệp vụ.

    Phép kiểm tra này **luôn ở mức cảnh báo**, không bao giờ làm dừng
    luồng. Lý do nằm ở vai trò của tầng Bronze: nó phải giữ nguyên dữ
    liệu như nhận được, kể cả bản trùng. Việc khử trùng thuộc về tầng
    Silver.

    Nếu dừng luồng vì phát hiện trùng lặp, tầng Silver sẽ không bao giờ
    có dữ liệu để chứng minh cơ chế khử trùng hoạt động đúng.

    Con số đo được ở đây có giá trị đối chiếu: bộ sinh dữ liệu cố ý tạo
    hai phần trăm bản trùng, nên kết quả xấp xỉ hai phần trăm xác nhận
    đường đi của dữ liệu từ nguồn tới Bronze không làm mất mát gì.

    Args:
        df: dữ liệu cần kiểm tra.
        spec: mô tả bảng.

    Returns:
        Kết quả kiểm tra.
    """
    if spec.primary_key not in df.columns:
        return CheckResult(
            check="duplicates",
            passed=False,
            severity="critical",
            detail=f"Không tìm thấy cột khoá {spec.primary_key}",
            value="",
        )

    total = len(df)
    unique = df[spec.primary_key].nunique()
    dup_rows = total - unique
    dup_pct = dup_rows / unique * 100 if unique else 0.0

    if dup_pct > DUPLICATE_WARN_THRESHOLD:
        return CheckResult(
            check="duplicates",
            passed=False,
            severity="warning",
            detail=(
                f"Tỷ lệ trùng {dup_pct:.3f}% vượt ngưỡng cảnh báo "
                f"{DUPLICATE_WARN_THRESHOLD}%, có {dup_rows:,} bản trùng"
            ),
            value=round(dup_pct, 3),
        )

    return CheckResult(
        check="duplicates",
        passed=True,
        severity="warning",
        detail=(
            f"Tỷ lệ trùng {dup_pct:.3f}% ({dup_rows:,} bản trùng trên "
            f"{unique:,} khoá phân biệt), nằm trong ngưỡng chấp nhận"
        ),
        value=round(dup_pct, 3),
    )


def validate_table(spec: TableSpec, **context) -> dict:
    """Chạy toàn bộ phép kiểm tra trên một bảng Bronze.

    Kết quả được ghi ra nhật ký dưới dạng bảng dễ đọc, và trả về dưới
    dạng từ điển để các tác vụ sau lấy qua cơ chế trao đổi dữ liệu của
    Airflow.

    Luồng chỉ dừng khi có phép kiểm tra ở mức nghiêm trọng thất bại.
    Các cảnh báo được ghi nhận đầy đủ nhưng không chặn.

    Args:
        spec: mô tả bảng cần kiểm tra.
        context: ngữ cảnh do Airflow truyền vào.

    Returns:
        Từ điển tổng hợp kết quả.

    Raises:
        ValueError: khi có phép kiểm tra nghiêm trọng thất bại.
    """
    df = _load_bronze_table(spec)

    results = [
        check_schema(df, spec),
        check_row_count(df, spec),
        check_nulls(df, spec),
        check_duplicates(df, spec),
    ]

    # --- Ghi nhật ký dạng bảng ---
    log.info("=" * 72)
    log.info("KIỂM TRA CHẤT LƯỢNG: %s", spec.name)
    log.info("=" * 72)
    for r in results:
        status = "ĐẠT " if r.passed else ("LỖI " if r.severity == "critical" else "CẢNH BÁO")
        log.info("  [%s] %-12s %s", status, r.check, r.detail)

    critical_failures = [r for r in results if not r.passed and r.severity == "critical"]
    warnings = [r for r in results if not r.passed and r.severity == "warning"]

    summary = {
        "table": spec.name,
        "rows": len(df),
        "columns": len(df.columns),
        "checks_total": len(results),
        "checks_passed": sum(1 for r in results if r.passed),
        "warnings": len(warnings),
        "critical_failures": len(critical_failures),
        "details": {r.check: {"passed": r.passed, "value": r.value} for r in results},
    }

    if critical_failures:
        messages = "; ".join(f"{r.check}: {r.detail}" for r in critical_failures)
        raise ValueError(f"Bảng {spec.name} không đạt kiểm tra nghiêm trọng — {messages}")

    if warnings:
        log.warning(
            "Bảng %s có %d cảnh báo, sẽ được xử lý ở tầng Silver",
            spec.name,
            len(warnings),
        )

    log.info("Bảng %s: %d/%d phép kiểm tra đạt", spec.name, summary["checks_passed"], len(results))
    return summary


def summarize_validation(**context) -> dict:
    """Gom kết quả kiểm tra của mọi bảng thành một báo cáo tổng hợp.

    Tác vụ này chạy sau khi mọi bảng đã được kiểm tra xong. Nó lấy kết
    quả từ cơ chế trao đổi dữ liệu giữa các tác vụ của Airflow và in ra
    một bảng duy nhất, tiện cho việc chụp màn hình làm bằng chứng.

    Args:
        context: ngữ cảnh do Airflow truyền vào.

    Returns:
        Từ điển tổng hợp toàn bộ luồng.
    """
    from dp1.common import BRONZE_TABLES

    ti = context["ti"]
    rows = []

    for spec in BRONZE_TABLES:
        result = ti.xcom_pull(task_ids=f"validate_stage.validate_{spec.name}")
        if result:
            rows.append(result)

    total_rows = sum(r["rows"] for r in rows)
    total_warnings = sum(r["warnings"] for r in rows)

    log.info("=" * 72)
    log.info("TỔNG HỢP KIỂM TRA CHẤT LƯỢNG TẦNG BRONZE")
    log.info("=" * 72)
    log.info("%-22s %12s %8s %10s %10s", "Bảng", "Số dòng", "Số cột", "Đạt", "Cảnh báo")
    log.info("-" * 72)
    for r in rows:
        log.info(
            "%-22s %12s %8d %7d/%d %10d",
            r["table"],
            f"{r['rows']:,}",
            r["columns"],
            r["checks_passed"],
            r["checks_total"],
            r["warnings"],
        )
    log.info("-" * 72)
    log.info("Tổng cộng: %s dòng trên %d bảng, %d cảnh báo", f"{total_rows:,}", len(rows), total_warnings)

    return {
        "tables": len(rows),
        "total_rows": total_rows,
        "total_warnings": total_warnings,
    }
