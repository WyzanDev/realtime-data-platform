"""
Sinh 3 bảng fact: order, order_item, review.

Đây là nhóm bảng ghi nhận sự kiện, tăng liên tục theo thời gian và lớn
gấp nhiều lần nhóm dimension. Toàn bộ các lỗi dữ liệu có chủ ý đều tập
trung ở đây:

  - Skew thời gian:      order.order_time dồn về giờ cao điểm
  - Trùng lặp 2%:        order_id lặp lại, phân biệt bằng ingested_at
  - Lệch số tiền:        total_amount không khớp tổng subtotal
  - Sai tham chiếu:      review.restaurant_id không khớp đơn gốc
  - Giá trị ngoại lai:   đơn văn phòng 13 đến 25 món vào giờ trưa ngày thường

Mọi lỗi đều đọc tỷ lệ từ cấu hình, không hardcode — để con số trong tài
liệu và con số trong YAML luôn khớp nhau.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from data_generator.common.config import Config, pick_indices_by_rate, weighted_choice

# Mẫu câu bình luận theo mức rating. Rating thấp cho câu phàn nàn, rating
# cao cho câu khen. Nhờ vậy cột comment có tương quan thật với điểm số,
# đủ để làm phân tích cảm xúc ở phần Novel ideas.
COMMENT_TEMPLATES = {
    "low": [
        "Do an nguoi, giao qua lau.",
        "Thieu mon, goi khong ai nghe may.",
        "Dong goi cau tha, bi do ra tui.",
        "Khong dung mon toi dat.",
    ],
    "mid": [
        "Tam on, giao hoi cham mot chut.",
        "Do an binh thuong, gia hoi cao.",
        "Duoc, nhung phan hoi it.",
    ],
    "high": [
        "Do an ngon, giao nhanh.",
        "Rat hai long, se dat lai.",
        "Tai xe than thien, mon con nong.",
        "Chat luong on dinh, dong goi ky.",
    ],
}


def _sample_order_hours(cfg: Config, size: int) -> np.ndarray:
    """Sinh giờ đặt đơn theo phân phối lệch về hai khung cao điểm.

    Khoảng 32% đơn rơi vào khung 11 đến 13 giờ và 30% rơi vào khung 18
    đến 20 giờ, phần còn lại trải đều các giờ khác. Chính sự dồn cụm này
    làm cho partition theo ngày có kích thước không đều, và là một trong
    những nguyên nhân khiến task Spark chạy lệch nhau ở baseline job.

    Hạn chế đã biết: các giờ ngoài cao điểm hiện được chia đều nhau, nên
    6 giờ sáng có lượng đơn ngang 17 giờ chiều. Thực tế không như vậy.

    Args:
        cfg: cấu hình đã nạp.
        size: số giờ cần sinh.

    Returns:
        Mảng số nguyên giờ trong ngày.
    """
    lunch = cfg.get("skew.hour_of_day.peak_lunch")
    dinner = cfg.get("skew.hour_of_day.peak_dinner")
    normal_w = cfg.get("skew.hour_of_day.normal.weight")

    bucket = np.random.choice(
        ["lunch", "dinner", "normal"],
        size=size,
        p=np.array([lunch["weight"], dinner["weight"], normal_w])
        / (lunch["weight"] + dinner["weight"] + normal_w),
    )

    other_hours = [h for h in range(6, 24) if h not in lunch["hours"] + dinner["hours"]]
    hours = np.empty(size, dtype=int)
    hours[bucket == "lunch"] = np.random.choice(lunch["hours"], size=(bucket == "lunch").sum())
    hours[bucket == "dinner"] = np.random.choice(dinner["hours"], size=(bucket == "dinner").sum())
    hours[bucket == "normal"] = np.random.choice(other_hours, size=(bucket == "normal").sum())
    return hours


def generate_orders(
    cfg: Config,
    customers: pd.DataFrame,
    restaurants: pd.DataFrame,
    drivers: pd.DataFrame,
) -> pd.DataFrame:
    """Sinh bảng order — bảng fact chính của hệ thống.

    Ba điểm đáng chú ý trong hàm này:

    1. Khách VIP được gán trọng số đặt hàng cao hơn thông qua tham số
       vip_order_multiplier, nên phân phối đơn theo customer_id cũng bị
       lệch nhẹ — giống thực tế và tạo thêm một lớp skew ngoài skew theo
       thành phố.

    2. Cột `delivery_city` được chép sẵn vào đơn thay vì phải join sang
       bảng customer. Về nghiệp vụ đây là thuộc tính thật của đơn, vì
       khách ở một thành phố vẫn có thể đặt giao sang thành phố khác. Về
       kỹ thuật, nó cho phép Phase 4 minh hoạ kỹ thuật salting mà không
       bị lẫn với chi phí shuffle của phép join.

    3. Thời gian giao dự kiến được tính có logic: thời gian chuẩn bị của
       quán cộng thời gian di chuyển theo quãng đường và loại xe của tài
       xế, cộng thêm phạt nếu rơi vào giờ cao điểm. Hiệu giữa thời gian
       thực tế và dự kiến chính là nhãn cho bài toán dự đoán trễ đơn ở
       Phase 8.

    Args:
        cfg: cấu hình đã nạp.
        customers: bảng customer đã sinh.
        restaurants: bảng restaurant đã sinh.
        drivers: bảng driver đã sinh.

    Returns:
        DataFrame bảng order, cột total_amount tạm để 0.
    """
    n = cfg.get("volume.n_orders")

    # --- Chọn khách, có trọng số nghiêng về nhóm VIP ---
    seg_weight = {"new": 1.0, "regular": 1.0, "vip": cfg.get("skew.vip_order_multiplier")}
    cust_w = np.array(customers["segment"].map(seg_weight).to_numpy(dtype=float), copy=True)
    cust_w /= cust_w.sum()
    cust_idx = np.random.choice(len(customers), size=n, p=cust_w)

    # --- Chọn quán, có trọng số theo phân phối lệch của nhóm món ---
    cat_w = cfg.get("skew.category")
    rst_w = np.array(restaurants["category"].map(cat_w).to_numpy(dtype=float), copy=True)
    rst_w /= rst_w.sum()
    rst_idx = np.random.choice(len(restaurants), size=n, p=rst_w)

    drv_idx = np.random.randint(0, len(drivers), size=n)

    # --- Thời điểm đặt đơn ---
    days = cfg.get("meta.days_history")
    day_offset = np.random.randint(0, days, size=n)
    hours = _sample_order_hours(cfg, n)
    minutes = np.random.randint(0, 60, size=n)

    order_time = (
        pd.Timestamp(cfg.start_date)
        + pd.to_timedelta(day_offset, unit="D")
        + pd.to_timedelta(hours, unit="h")
        + pd.to_timedelta(minutes, unit="m")
    )

    status = weighted_choice(cfg.get("enums.order_status"), size=n)

    # --- Tính thời gian giao dự kiến ---
    prep = restaurants["prep_time_minutes"].to_numpy()[rst_idx]
    d_lo, d_hi = cfg.get("delivery.distance_km_range")
    distance = np.round(np.random.uniform(d_lo, d_hi, size=n), 2)

    speed_map = cfg.get("enums.speed_factor")
    factor = drivers["vehicle_type"].map(speed_map).to_numpy(dtype=float)[drv_idx]

    travel = distance * cfg.get("delivery.travel_minutes_per_km") * factor
    peak_hours = set(
        cfg.get("skew.hour_of_day.peak_lunch.hours") + cfg.get("skew.hour_of_day.peak_dinner.hours")
    )
    penalty = np.where(
        np.isin(hours, list(peak_hours)), cfg.get("delivery.peak_hour_penalty_minutes"), 0
    )

    est_minutes = prep + travel + penalty
    estimated = order_time + pd.to_timedelta(np.round(est_minutes), unit="m")

    # Nhiễu lệch phải: đa số đơn trễ nhẹ, một số ít đến sớm.
    noise_lo, noise_hi = cfg.get("delivery.actual_vs_estimated_noise")
    noise = np.random.triangular(noise_lo, 2.0, noise_hi, size=n)
    actual = estimated + pd.to_timedelta(np.round(noise), unit="m")
    # Đơn huỷ hoặc thất bại thì không có thời điểm giao thực tế.
    actual = pd.Series(actual).where(status == "completed")

    # --- Lý do huỷ: chỉ đơn có trạng thái cancelled mới có ---
    reason_code = pd.Series([None] * n, dtype=object)
    cancelled_mask = status == "cancelled"
    n_cancelled = int(cancelled_mask.sum())
    if n_cancelled:
        reason_code[cancelled_mask] = weighted_choice(
            cfg.get("enums.cancelled_reason_code"), size=n_cancelled
        )

    # Chỉ mã "other" mới kèm text tự do. Ràng buộc có điều kiện này chính
    # là thứ sẽ được viết thành data contract ở Phase 9.
    reason_text = pd.Series([None] * n, dtype=object)
    other_mask = reason_code == "other"
    reason_text[other_mask] = "Khach ghi chu them ve ly do huy don."

    # Tài xế chưa được gán cho một phần đơn huỷ.
    driver_ids = pd.Series(drivers["driver_id"].to_numpy()[drv_idx])
    unassigned = cancelled_mask & (np.random.random(n) < 0.4)
    driver_ids[unassigned] = None

    df = pd.DataFrame(
        {
            "order_id": [f"ORD{i:09d}" for i in range(n)],
            "customer_id": customers["customer_id"].to_numpy()[cust_idx],
            "restaurant_id": restaurants["restaurant_id"].to_numpy()[rst_idx],
            "driver_id": driver_ids,
            "order_time": order_time,
            "status": status,
            "total_amount": 0,  # điền sau khi có bảng order_item
            "payment_method": weighted_choice(cfg.get("enums.payment_method"), size=n),
            "delivery_city": customers["city"].to_numpy()[cust_idx],
            "distance_km": distance,
            "estimated_delivery_time": estimated,
            "actual_delivery_time": actual,
            "cancelled_reason_code": reason_code,
            "cancelled_reason_text": reason_text,
        }
    )

    # 3% đơn giao tới thành phố khác với nơi khách đăng ký — lý do nghiệp
    # vụ để cột `delivery_city` tồn tại như một cột độc lập.
    cross_idx = pick_indices_by_rate(n, 0.03)
    all_cities = list(cfg.get("skew.city").keys())
    df.loc[cross_idx, "delivery_city"] = np.random.choice(all_cities, size=len(cross_idx))

    df["ingested_at"] = order_time + pd.to_timedelta(
        np.random.randint(5, 720, size=n), unit="m"
    )   
    return df


def generate_order_items(
    cfg: Config, orders: pd.DataFrame, menu_items: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Sinh bảng order_item và cập nhật cột total_amount của bảng order.

    Đây là bảng lớn nhất hệ thống, mỗi đơn có từ 2 đến 4 dòng. Phép join
    giữa bảng này và menu_item là chỗ shuffle nặng nhất — nơi baseline
    job ở Phase 4 sẽ lộ vấn đề rõ nhất trên Spark UI.

    Về số lượng món: đa số đơn chỉ 1 đến 2 món, nhưng khoảng 3% là đơn
    văn phòng đặt 13 đến 25 phần. Giá trị ngoại lai này chỉ xuất hiện
    với món cơm, bún hoặc trà sữa, vào giờ trưa ngày thường — tức là có
    logic nghiệp vụ chứ không phải nhiễu ngẫu nhiên.

    Args:
        cfg: cấu hình đã nạp.
        orders: bảng order đã sinh.
        menu_items: bảng menu_item gộp cả hai phiên bản schema.

    Returns:
        Cặp (order_items, orders_đã_cập_nhật_total_amount).
    """
    lo, hi = cfg.get("volume.items_per_order")
    n_orders = len(orders)
    items_per = np.random.randint(lo, hi + 1, size=n_orders)

    # Trải phẳng: mỗi dòng kết quả là một món trong một đơn.
    order_row_idx = np.repeat(np.arange(n_orders), items_per)
    total_items = len(order_row_idx)

    menu_idx = np.random.randint(0, len(menu_items), size=total_items)
    item_cats = menu_items["category"].to_numpy()[menu_idx]
    unit_price = menu_items["price"].to_numpy()[menu_idx]

    # --- Số lượng theo ba nhóm ---
    q_cfg = cfg.get("quantity")
    bucket = np.random.choice(
        ["normal", "medium", "bulk"],
        size=total_items,
        p=[q_cfg["normal"]["weight"], q_cfg["medium"]["weight"], q_cfg["bulk"]["weight"]],
    )

    quantity = np.ones(total_items, dtype=int)
    for name in ("normal", "medium", "bulk"):
        mask = bucket == name
        r_lo, r_hi = q_cfg[name]["range"]
        quantity[mask] = np.random.randint(r_lo, r_hi + 1, size=int(mask.sum()))

    # Lọc lại giá trị ngoại lai: chỉ giữ đơn số lượng lớn nếu thoả điều
    # kiện về nhóm món, khung giờ và ngày trong tuần. Trường hợp không
    # thoả sẽ bị hạ về mức bình thường.
    order_times = orders["order_time"].to_numpy()[order_row_idx]
    order_hours = pd.Series(order_times).dt.hour.to_numpy()
    order_dow = pd.Series(order_times).dt.dayofweek.to_numpy()

    bulk_ok = (
        np.isin(item_cats, q_cfg["bulk"]["only_categories"])
        & np.isin(order_hours, q_cfg["bulk"]["only_hours"])
        & (order_dow < 5 if q_cfg["bulk"]["weekday_only"] else True)
    )
    invalid_bulk = (bucket == "bulk") & ~bulk_ok
    quantity[invalid_bulk] = np.random.randint(1, 3, size=int(invalid_bulk.sum()))

    items = pd.DataFrame(
        {
            "order_item_id": [f"OIT{i:010d}" for i in range(total_items)],
            "order_id": orders["order_id"].to_numpy()[order_row_idx],
            "menu_item_id": menu_items["menu_item_id"].to_numpy()[menu_idx],
            "quantity": quantity,
            "unit_price": unit_price,
        }
    )
    items["subtotal"] = items["quantity"] * items["unit_price"]

    # --- Cập nhật total_amount cho bảng order ---
    totals = items.groupby("order_id", sort=False)["subtotal"].sum()
    orders = orders.copy()
    orders["total_amount"] = orders["order_id"].map(totals).fillna(0).astype(int)

    # Tiêm lỗi lệch tiền có chủ ý: total_amount không còn bằng tổng
    # subtotal. Đây là dữ liệu đầu vào cho bước kiểm tra tính nhất quán ở
    # Phase 3 — nếu không có lỗi nào thì Validate stage luôn xanh và vô
    # nghĩa.
    bad_idx = pick_indices_by_rate(len(orders), cfg.get("defects.amount_mismatch_rate"))
    drift = np.random.choice([-50_000, -20_000, 15_000, 40_000], size=len(bad_idx))
    orders.loc[bad_idx, "total_amount"] = (
        orders.loc[bad_idx, "total_amount"].to_numpy() + drift
    ).clip(0)

    items["ingested_at"] = orders["ingested_at"].to_numpy()[order_row_idx]
    return items, orders


def generate_reviews(cfg: Config, orders: pd.DataFrame) -> pd.DataFrame:
    """Sinh bảng review — bảng thưa, chỉ khoảng 30% đơn có đánh giá.

    Hai cột `customer_id` và `restaurant_id` là dư thừa về mặt chuẩn hoá
    vì đều suy được từ order_id. Chúng được giữ lại có chủ ý: một tỷ lệ
    nhỏ restaurant_id sẽ bị làm sai lệch so với đơn gốc, tạo ra lỗi tham
    chiếu chéo cho Validate stage bắt.

    Nội dung bình luận được chọn theo mức rating nên có tương quan thật
    với điểm số.

    Args:
        cfg: cấu hình đã nạp.
        orders: bảng order đã hoàn tất total_amount.

    Returns:
        DataFrame bảng review.
    """
    completed = orders[orders["status"] == "completed"]
    n_review = int(len(completed) * cfg.get("volume.review_rate"))
    picked = completed.sample(n=n_review, random_state=cfg.get("meta.seed"))

    ratings = weighted_choice(cfg.get("enums.rating_review"), size=n_review).astype(int)

    comments = np.empty(n_review, dtype=object)
    for tier, mask in (
        ("low", ratings <= 2),
        ("mid", ratings == 3),
        ("high", ratings >= 4),
    ):
        idx = np.where(mask)[0]
        comments[idx] = np.random.choice(COMMENT_TEMPLATES[tier], size=len(idx))

    # Đánh giá luôn được viết sau khi đơn đã giao xong.
    delay = pd.to_timedelta(np.random.randint(5, 2880, size=n_review), unit="m")
    created_at = picked["actual_delivery_time"].to_numpy() + delay

    df = pd.DataFrame(
        {
            "review_id": [f"REV{i:09d}" for i in range(n_review)],
            "order_id": picked["order_id"].to_numpy(),
            "customer_id": picked["customer_id"].to_numpy(),
            "restaurant_id": picked["restaurant_id"].to_numpy(),
            "rating": ratings,
            "comment": comments,
            "created_at": created_at,
        }
    )

    # Tiêm sai lệch tham chiếu: restaurant_id không còn khớp với đơn gốc.
    bad_idx = pick_indices_by_rate(len(df), cfg.get("defects.review_restaurant_mismatch"))
    df.loc[bad_idx, "restaurant_id"] = np.random.choice(
        orders["restaurant_id"].unique(), size=len(bad_idx)
    )

    df["ingested_at"] = pd.to_datetime(created_at) + pd.to_timedelta(
        np.random.randint(5, 240, size=n_review), unit="m"
    )
    return df


def inject_duplicates(cfg: Config, orders: pd.DataFrame) -> pd.DataFrame:
    """Nhân bản khoảng 2% số đơn để mô phỏng lỗi ghi trùng khi nạp dữ liệu.

    Dòng nhân bản giữ nguyên `order_id` (khoá nghiệp vụ) nhưng có
    `ingested_at` muộn hơn vài giây đến vài phút. Nhờ chênh lệch này,
    Phase 4 có thể khử trùng bằng câu lệnh:

        row_number() OVER (PARTITION BY order_id ORDER BY ingested_at DESC) = 1

    Nếu hai dòng giống hệt nhau từng cột thì không có căn cứ nào để chọn
    giữ dòng nào — đó là lý do phải có cột phá hoà.

    Args:
        cfg: cấu hình đã nạp.
        orders: bảng order gốc.

    Returns:
        DataFrame đã chèn thêm dòng trùng và xáo trộn thứ tự.
    """
    rate = cfg.get("defects.offline_duplicate_rate")
    dup_idx = pick_indices_by_rate(len(orders), rate)
    dups = orders.iloc[dup_idx].copy()

    shift = pd.to_timedelta(np.random.randint(1, 600, size=len(dups)), unit="s")
    dups["ingested_at"] = dups["ingested_at"] + shift

    out = pd.concat([orders, dups], ignore_index=True)
    return out.sample(frac=1.0, random_state=cfg.get("meta.seed")).reset_index(drop=True)
