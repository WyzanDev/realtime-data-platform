"""
Bộ nạp cấu hình và các tiện ích lấy mẫu theo phân phối có trọng số.

File này là lớp nền của toàn bộ generator: mọi module khác đều gọi
`load_config()` để lấy tham số, và dùng `weighted_choice()` để sinh giá
trị theo tỷ lệ đã khai báo trong YAML. Nhờ vậy không module nào cần
hardcode tỷ lệ skew hay tỷ lệ lỗi.
"""

from __future__ import annotations

import random
from datetime import date, datetime
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import yaml

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "generator.yaml"


class Config:
    """Bọc dict cấu hình thành object truy cập theo đường dẫn chuỗi.

    Ví dụ: cfg.get("skew.city") thay vì cfg["skew"]["city"].
    Mục đích là để code gọi tham số đọc rõ ràng hơn và báo lỗi sớm khi
    một khoá bị thiếu trong YAML.
    """

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = raw

    def get(self, path: str, default: Any = ...) -> Any:
        """Lấy giá trị theo đường dẫn phân cách bằng dấu chấm.

        Args:
            path: ví dụ "volume.n_customers" hoặc "skew.city".
            default: giá trị trả về nếu không tìm thấy. Nếu không truyền,
                hàm sẽ raise KeyError — cố ý để lỗi cấu hình lộ ra ngay
                thay vì âm thầm chạy sai.

        Returns:
            Giá trị tương ứng trong cấu hình.
        """
        node: Any = self.raw
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                if default is ...:
                    raise KeyError(f"Thiếu khoá cấu hình: {path}")
                return default
            node = node[part]
        return node

    @property
    def start_date(self) -> date:
        """Ngày bắt đầu của khoảng dữ liệu lịch sử."""
        return datetime.strptime(self.get("meta.start_date"), "%Y-%m-%d").date()

    @property
    def v2_start_date(self) -> date:
        """Mốc thời gian bảng menu_item chuyển sang schema phiên bản 2."""
        return datetime.strptime(
            self.get("schema_evolution.v2_start_date"), "%Y-%m-%d"
        ).date()


def load_config(path: str | Path | None = None) -> Config:
    """Đọc file YAML và cố định seed ngẫu nhiên.

    Seed được set ngay tại đây cho cả `random` lẫn `numpy` để mỗi lần
    chạy lại generator đều cho ra đúng bộ dữ liệu cũ. Điều này quan
    trọng vì các số liệu thống kê đã chụp làm bằng chứng phải tái tạo
    được.

    Args:
        path: đường dẫn file YAML. Bỏ trống thì dùng file mặc định
            trong thư mục config/.

    Returns:
        Đối tượng Config đã sẵn sàng sử dụng.
    """
    cfg_path = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(cfg_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    cfg = Config(raw)
    seed = cfg.get("meta.seed", 42)
    random.seed(seed)
    np.random.seed(seed)
    return cfg


def weighted_choice(weights: dict[Any, float], size: int = 1) -> np.ndarray:
    """Lấy mẫu theo từ điển {giá_trị: trọng_số}.

    Đây chính là cơ chế tạo skew: truyền vào dict city với "Ho Chi Minh"
    có trọng số 0.45 thì khoảng 45% mẫu sinh ra sẽ là thành phố đó.
    Trọng số được chuẩn hoá lại về tổng 1.0 nên file YAML không bắt buộc
    phải cộng tròn 100%.

    Args:
        weights: ánh xạ giá trị sang trọng số (không âm).
        size: số mẫu cần sinh.

    Returns:
        Mảng numpy chứa `size` giá trị đã lấy mẫu.
    """
    keys = list(weights.keys())
    probs = np.array([float(v) for v in weights.values()], dtype=float)
    probs = probs / probs.sum()
    return np.random.choice(keys, size=size, p=probs)


def weighted_one(weights: dict[Any, float]) -> Any:
    """Phiên bản lấy đúng một mẫu, dùng khi sinh từng dòng riêng lẻ."""
    return weighted_choice(weights, size=1)[0]


def bounded_normal(low: float, high: float, size: int, skew_right: bool = False) -> np.ndarray:
    """Sinh số trong khoảng [low, high] theo phân phối không đều.

    Dùng cho các trường như rating hay giá tiền — nơi phân phối đều
    trông rất giả. Khi `skew_right=True`, mẫu bị dồn về phía giá trị
    cao, ví dụ rating nhà hàng tập trung trong khoảng 4.0 đến 4.8.

    Args:
        low: cận dưới.
        high: cận trên.
        size: số mẫu.
        skew_right: True thì lệch về phía giá trị cao.

    Returns:
        Mảng số thực trong khoảng đã cho.
    """
    if skew_right:
        raw = np.random.beta(a=5.0, b=2.0, size=size)
    else:
        raw = np.random.beta(a=2.0, b=2.0, size=size)
    return low + raw * (high - low)


def pick_indices_by_rate(total: int, rate: float) -> np.ndarray:
    """Chọn ngẫu nhiên một tỷ lệ dòng trong tổng số `total` dòng.

    Dùng để tiêm lỗi có chủ ý: ví dụ rate=0.02 trả về chỉ số của 2% số
    dòng sẽ bị nhân bản. Tách thành hàm riêng để mọi loại lỗi đều đi
    qua cùng một cơ chế, tiện đối chiếu với cấu hình khi viết tài liệu.

    Args:
        total: tổng số dòng.
        rate: tỷ lệ cần chọn, từ 0.0 đến 1.0.

    Returns:
        Mảng chỉ số đã chọn, không lặp lại.
    """
    n_pick = int(round(total * rate))
    if n_pick <= 0:
        return np.array([], dtype=int)
    return np.random.choice(total, size=n_pick, replace=False)


def random_dates(start: date, days: int, size: int) -> np.ndarray:
    """Sinh mảng ngày ngẫu nhiên trong khoảng [start, start + days).

    Args:
        start: ngày bắt đầu.
        days: độ dài khoảng thời gian tính bằng ngày.
        size: số ngày cần sinh.

    Returns:
        Mảng numpy kiểu datetime64[D].
    """
    offsets = np.random.randint(0, days, size=size)
    base = np.datetime64(start)
    return base + offsets.astype("timedelta64[D]")


def normalized(weights: dict[Any, float]) -> dict[Any, float]:
    """Chuẩn hoá từ điển trọng số về tổng 1.0, dùng khi in báo cáo."""
    total = sum(float(v) for v in weights.values())
    return {k: float(v) / total for k, v in weights.items()}


def sample_sequence(values: Sequence[Any], size: int) -> np.ndarray:
    """Lấy mẫu đều có hoàn lại từ một dãy giá trị cho trước."""
    idx = np.random.randint(0, len(values), size=size)
    arr = np.asarray(values, dtype=object)
    return arr[idx]
