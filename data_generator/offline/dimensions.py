"""
Sinh 4 bảng dimension: customer, restaurant, driver, menu_item.

Đây là nhóm bảng "thay đổi chậm" — mô tả thực thể có thật (khách hàng,
quán ăn, tài xế, món ăn) chứ không ghi nhận sự kiện. Ba bảng đầu sẽ được
ghi xuống PostgreSQL để giả lập nguồn dữ liệu của phòng ban khác; riêng
menu_item ghi xuống MinIO vì nó là nơi diễn ra schema evolution.

Các vấn đề dữ liệu được cài vào đây:
  - Skew:              customer.city, restaurant.category
  - High cardinality:  customer_id (120k), menu_item_id (45k)
  - Schema evolution:  menu_item.spice_level (chỉ tồn tại từ v2_start_date)
  - Lỗi có chủ ý:      số điện thoại sai định dạng, email null
"""

from __future__ import annotations

import uuid

import numpy as np
import pandas as pd

from data_generator.common.config import (
    Config,
    bounded_normal,
    pick_indices_by_rate,
    random_dates,
    weighted_choice,
)

# Danh sách quận của từng thành phố. Chỉ cần đủ để tạo quan hệ phân cấp
# city sang district, phục vụ việc xem chi tiết theo từng cấp ở tầng Gold.
DISTRICTS = {
    "Ho Chi Minh": ["Quan 1", "Quan 3", "Quan 7", "Binh Thanh", "Go Vap", "Tan Binh", "Thu Duc"],
    "Ha Noi": ["Ba Dinh", "Hoan Kiem", "Cau Giay", "Dong Da", "Hai Ba Trung", "Thanh Xuan"],
    "Da Nang": ["Hai Chau", "Thanh Khe", "Son Tra", "Ngu Hanh Son"],
    "Hai Phong": ["Hong Bang", "Le Chan", "Ngo Quyen"],
    "Can Tho": ["Ninh Kieu", "Binh Thuy", "Cai Rang"],
    "Bien Hoa": ["Trang Dai", "Tan Phong", "Buu Long"],
    "Nha Trang": ["Loc Tho", "Vinh Hai", "Phuoc Long"],
    "Hue": ["Phu Hoi", "Vinh Ninh", "An Cuu"],
}

# Tên món theo từng nhóm, dùng để ghép tên menu_item cho giống thật.
DISH_NAMES = {
    "Com": ["Com tam suon", "Com ga xoi mo", "Com chien duong chau", "Com bo luc lac"],
    "Tra sua": ["Tra sua tran chau", "Tra dao cam sa", "Hong tra macchiato", "Tra vai"],
    "Bun - Pho": ["Pho bo tai", "Bun bo Hue", "Bun rieu cua", "Pho ga"],
    "Do an nhanh": ["Ga ran", "Burger bo", "Khoai tay chien", "Hot dog"],
    "Lau - Nuong": ["Lau thai hai san", "Ba chi nuong", "Lau nam chay", "Suon nuong BBQ"],
    "Banh mi": ["Banh mi thit nuong", "Banh mi cha ca", "Banh mi op la", "Banh mi xiu mai"],
    "Che - Trang mieng": ["Che khuc bach", "Che buoi", "Banh flan", "Sua chua nep cam"],
    "Do chay": ["Com chay thap cam", "Bun rieu chay", "Goi cuon chay", "Pho chay"],
}

RESTAURANT_PREFIX = ["Quan", "Nha hang", "Tiem", "Bep", "Goc"]
RESTAURANT_SUFFIX = ["Co Ba", "Chu Tu", "Ngon", "Sai Gon", "Ha Thanh", "Mien Tay", "So 1", "Xua"]

FIRST_NAMES = ["Nguyen", "Tran", "Le", "Pham", "Hoang", "Vu", "Dang", "Bui", "Do", "Ho"]
MIDDLE_NAMES = ["Van", "Thi", "Minh", "Quoc", "Ngoc", "Thanh", "Huu", "Gia"]
LAST_NAMES = ["An", "Binh", "Chi", "Dung", "Giang", "Hoa", "Khanh", "Lan", "Nam", "Phuc", "Quyen", "Tuan"]


def _random_names(size: int) -> np.ndarray:
    """Ghép họ, tên đệm và tên thành mảng tên người Việt.

    Args:
        size: số tên cần sinh.

    Returns:
        Mảng chuỗi họ tên đầy đủ.
    """
    ho = np.random.choice(FIRST_NAMES, size=size)
    dem = np.random.choice(MIDDLE_NAMES, size=size)
    ten = np.random.choice(LAST_NAMES, size=size)
    return np.char.add(np.char.add(np.char.add(ho, " "), np.char.add(dem, " ")), ten)


def _districts_for(cities: np.ndarray) -> np.ndarray:
    """Chọn quận hợp lệ tương ứng với từng thành phố.

    Không thể random độc lập vì quận phải thuộc đúng thành phố của nó.
    Nếu không, bước kiểm tra tính nhất quán ở Validate stage sẽ báo lỗi
    thật thay vì lỗi đã cố ý cài vào.

    Args:
        cities: mảng tên thành phố.

    Returns:
        Mảng tên quận, mỗi phần tử thuộc đúng thành phố cùng vị trí.
    """
    return np.array([np.random.choice(DISTRICTS[c]) for c in cities], dtype=object)


def _phone_numbers(size: int, invalid_rate: float) -> np.ndarray:
    """Sinh số điện thoại Việt Nam, có một tỷ lệ nhỏ sai định dạng.

    Số hợp lệ gồm 10 chữ số, bắt đầu bằng 03, 05, 07, 08 hoặc 09.
    Số lỗi thì thiếu chữ số hoặc chứa ký tự lạ, để Validate stage ở
    Phase 3 có dữ liệu thật mà bắt.

    Args:
        size: số lượng cần sinh.
        invalid_rate: tỷ lệ số sai định dạng.

    Returns:
        Mảng chuỗi số điện thoại.
    """
    heads = np.random.choice(["03", "05", "07", "08", "09"], size=size)
    tails = np.random.randint(10_000_000, 99_999_999, size=size).astype(str)
    phones = np.char.add(heads, tails)

    bad_idx = pick_indices_by_rate(size, invalid_rate)
    for i in bad_idx:
        phones[i] = np.random.choice(["0123", "abc-xyz", "84-0", "N/A"])
    return phones


def generate_customers(cfg: Config) -> pd.DataFrame:
    """Sinh bảng customer.

    Đây là nguồn high cardinality chính với 120 nghìn customer_id duy
    nhất. Cột `city` mang phân phối lệch nặng (thành phố lớn nhất chiếm
    45%) — chính là cột sẽ gây skew khi Spark gom nhóm hoặc join theo
    địa bàn ở Phase 4.

    Args:
        cfg: cấu hình đã nạp.

    Returns:
        DataFrame bảng customer.
    """
    n = cfg.get("volume.n_customers")

    cities = weighted_choice(cfg.get("skew.city"), size=n)
    emails = np.array([f"user{i}@example.com" for i in range(n)], dtype=object)

    # Email null ở đây là null "hợp lệ" — khách chưa cung cấp. Khác hẳn
    # null do schema evolution ở cột menu_item.spice_level.
    null_idx = pick_indices_by_rate(n, cfg.get("defects.null_email_rate"))
    emails[null_idx] = None

    df = pd.DataFrame(
        {
            "customer_id": [str(uuid.uuid4()) for _ in range(n)],
            "name": _random_names(n),
            "phone": _phone_numbers(n, cfg.get("defects.invalid_phone_rate")),
            "email": emails,
            "city": cities,
            "district": _districts_for(cities),
            "signup_date": random_dates(cfg.start_date, cfg.get("meta.days_history"), n),
            "segment": weighted_choice(cfg.get("skew.customer_segment"), size=n),
        }
    )
    df["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return df


def generate_restaurants(cfg: Config) -> pd.DataFrame:
    """Sinh bảng restaurant.

    Cột `category` là nguồn skew thứ hai, độc lập với skew theo thành
    phố — cho phép Phase 4 minh hoạ hai kiểu lệch khác nhau trên cùng
    tập dữ liệu. Cột `restaurant_id` với 8 nghìn giá trị sẽ đóng vai
    trò khoá bucketing.

    Args:
        cfg: cấu hình đã nạp.

    Returns:
        DataFrame bảng restaurant.
    """
    n = cfg.get("volume.n_restaurants")

    cities = weighted_choice(cfg.get("skew.city"), size=n)
    prefix = np.random.choice(RESTAURANT_PREFIX, size=n)
    suffix = np.random.choice(RESTAURANT_SUFFIX, size=n)

    open_hour = np.random.choice([6, 7, 8, 9, 10], size=n)
    # Giờ đóng cửa luôn sau giờ mở cửa. Ràng buộc này giữ cho đơn hàng
    # không bao giờ rơi vào lúc quán đang đóng.
    close_hour = open_hour + np.random.choice([12, 13, 14, 15], size=n)

    df = pd.DataFrame(
        {
            "restaurant_id": [f"RST{i:06d}" for i in range(n)],
            "name": np.char.add(np.char.add(prefix, " "), suffix),
            "city": cities,
            "district": _districts_for(cities),
            "category": weighted_choice(cfg.get("skew.category"), size=n),
            "rating": np.round(bounded_normal(1.0, 5.0, n, skew_right=True), 2),
            "open_hour": open_hour,
            "close_hour": np.minimum(close_hour, 23),
            "prep_time_minutes": np.random.randint(
                cfg.get("delivery.prep_time_minutes")[0],
                cfg.get("delivery.prep_time_minutes")[1] + 1,
                size=n,
            ),
        }
    )
    df["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return df


def generate_drivers(cfg: Config) -> pd.DataFrame:
    """Sinh bảng driver.

    Cột `vehicle_type` chia xe máy xăng, xe máy điện và ô tô theo tỷ lệ
    sát thực tế Việt Nam. Loại xe ảnh hưởng tốc độ giao hàng thông qua
    hệ số `speed_factor`, nên đây không phải cột trang trí mà là đầu vào
    thật cho đặc trưng dự đoán trễ đơn ở Phase 8.

    Args:
        cfg: cấu hình đã nạp.

    Returns:
        DataFrame bảng driver.
    """
    n = cfg.get("volume.n_drivers")
    cities = weighted_choice(cfg.get("skew.city"), size=n)

    df = pd.DataFrame(
        {
            "driver_id": [f"DRV{i:06d}" for i in range(n)],
            "name": _random_names(n),
            "phone": _phone_numbers(n, cfg.get("defects.invalid_phone_rate")),
            "vehicle_type": weighted_choice(cfg.get("enums.vehicle_type"), size=n),
            "city": cities,
            "active_since": random_dates(cfg.start_date, cfg.get("meta.days_history"), n),
            "rating": np.round(bounded_normal(2.5, 5.0, n, skew_right=True), 2),
        }
    )
    df["ingested_at"] = pd.Timestamp.utcnow().tz_localize(None)
    return df


def generate_menu_items(cfg: Config, restaurants: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sinh bảng menu_item và tách làm hai lô theo phiên bản schema.

    Đây là trọng tâm của yêu cầu schema evolution. Hàm trả về hai
    DataFrame RIÊNG BIỆT:

      - Lô v1: 9 cột, KHÔNG có `spice_level`
      - Lô v2: 10 cột, CÓ `spice_level`

    Hai lô này sẽ được ghi thành hai lần gọi `to_parquet` khác nhau, vào
    hai partition ngày khác nhau. Vì Parquet lưu schema ngay bên trong
    từng file, hai thư mục sẽ thật sự khác cấu trúc — đó mới là schema
    evolution đúng nghĩa.

    Nếu tạo sẵn cột rồi để null cho lô cũ thì file vẫn có đủ cột, và
    tuỳ chọn `mergeSchema` của Spark sẽ không còn gì để hợp nhất.

    Args:
        cfg: cấu hình đã nạp.
        restaurants: bảng restaurant đã sinh, dùng để gán khoá ngoại.

    Returns:
        Cặp (df_v1, df_v2) với số cột khác nhau.
    """
    n = cfg.get("volume.n_menu_items")

    # Mỗi món thuộc về một quán có thật, nhờ vậy giữ được ràng buộc khoá ngoại.
    rst_idx = np.random.randint(0, len(restaurants), size=n)
    rst_ids = restaurants["restaurant_id"].to_numpy()[rst_idx]
    rst_cats = restaurants["category"].to_numpy()[rst_idx]

    names = np.array([np.random.choice(DISH_NAMES[c]) for c in rst_cats], dtype=object)

    # Giá theo phân phối log-normal: đa số món rẻ, một số ít món rất đắt.
    # Sát thực tế hơn phân phối đều, và tạo đuôi dài cho cột subtotal.
    prices = np.random.lognormal(mean=11.0, sigma=0.55, size=n)
    prices = np.clip(prices, 15_000, 500_000)
    prices = (prices // 1000 * 1000).astype(int)

    base = pd.DataFrame(
        {
            "menu_item_id": [f"MNU{i:07d}" for i in range(n)],
            "restaurant_id": rst_ids,
            "name": names,
            "category": rst_cats,
            "price": prices,
            "is_available": np.random.random(n) > 0.05,
        }
    )

    # Chia món thành hai nhóm theo mốc thời gian v2_start_date.
    # Tỷ lệ nghiêng về v2 vì thực đơn được bổ sung liên tục theo thời gian.
    is_v2 = np.random.random(n) < 0.55
    df_v1 = base[~is_v2].copy()
    df_v2 = base[is_v2].copy()

    days = cfg.get("meta.days_history")
    v2_start = cfg.v2_start_date
    days_before = (v2_start - cfg.start_date).days

    # Lô v1 nằm trong khoảng trước mốc v2, lô v2 nằm sau mốc đó.
    df_v1["ingested_at"] = pd.to_datetime(
        random_dates(cfg.start_date, max(days_before, 1), len(df_v1))
    )
    df_v2["ingested_at"] = pd.to_datetime(
        random_dates(v2_start, max(days - days_before, 1), len(df_v2))
    )

    # Chỉ lô v2 được thêm cột mới. Độ cay chỉ có nghĩa với một số nhóm
    # món, các nhóm còn lại để null — đây là null "hợp lệ".
    spice_lo, spice_hi = cfg.get("schema_evolution.v2_value_range")
    spicy_cats = set(cfg.get("schema_evolution.applies_to_categories"))
    spice_vals = np.random.randint(spice_lo, spice_hi + 1, size=len(df_v2)).astype(object)
    not_spicy = ~df_v2["category"].isin(spicy_cats).to_numpy()
    spice_vals[not_spicy] = None
    df_v2["spice_level"] = spice_vals

    return df_v1, df_v2
