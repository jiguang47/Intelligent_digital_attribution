"""生成与订单转化共享会话的日级用户行为"""

from __future__ import annotations

import random
from datetime import date, datetime, timedelta
from math import ceil

from ..reference import ReferenceData
from ..settings import RunContext
from ..support import (
    UNKNOWN_SK,
    TableWriter,
    date_key,
    end_of_day,
    fact_audit,
    version_at,
)
from ..timeline import BusinessState, CartPosition, ConversionIntent, SessionJourney


def generate_day(
    ctx: RunContext,
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    day: date,
    batch_id: str,
    page_view_count: int,
    search_count: int,
    order_detail_target: int,
) -> list[ConversionIntent]:
    active_profiles = refs.active_profiles(day)
    if not active_profiles:
        if page_view_count or search_count or order_detail_target:
            raise ValueError(f"{day} 没有已上架商品，无法生成行为")
        return []
    rng = random.Random(f"{ctx.gen.seed}:{day}:behavior")
    active_shop_counts: dict[int, int] = {}
    fulfillment_shop_counts: dict[tuple[int, int], int] = {}
    for active_profile in active_profiles:
        shop_id = int(active_profile.shop["shop_id"])
        active_shop_counts[shop_id] = active_shop_counts.get(shop_id, 0) + 1
        warehouse_id = int(
            refs.warehouse_for_profile(active_profile)["warehouse_id"]
        )
        fulfillment_key = (warehouse_id, shop_id)
        fulfillment_shop_counts[fulfillment_key] = (
            fulfillment_shop_counts.get(fulfillment_key, 0) + 1
        )
    eligible_profiles_by_lines = {
        line_count: [
            profile
            for profile in active_profiles
            if active_shop_counts[int(profile.shop["shop_id"])] >= line_count
        ]
        for line_count in (1, 2, 3)
    }
    eligible_profiles_by_fulfillment: dict[
        tuple[int, int],
        list,
    ] = {}
    for active_profile in active_profiles:
        warehouse_id = int(
            refs.warehouse_for_profile(active_profile)["warehouse_id"]
        )
        shop_id = int(active_profile.shop["shop_id"])
        maximum_lines = min(
            3,
            fulfillment_shop_counts[(warehouse_id, shop_id)],
        )
        for line_count in range(1, maximum_lines + 1):
            eligible_profiles_by_fulfillment.setdefault(
                (warehouse_id, line_count),
                [],
            ).append(active_profile)
    line_counts = _order_line_counts(order_detail_target, rng)
    session_count = max(
        len(line_counts),
        1 if page_view_count else 0,
        ceil(page_view_count / 3.2),
    )
    if page_view_count < session_count:
        raise ValueError(
            f"页面访问量不足以承载会话 day={day} "
            f"views={page_view_count} sessions={session_count}"
        )
    conversion_indexes = set(rng.sample(range(session_count), len(line_counts)))
    cart_indexes = _cart_session_indexes(
        session_count,
        conversion_indexes,
        rng,
    )
    search_counts = _search_counts(
        search_count,
        session_count,
        conversion_indexes,
        cart_indexes,
        rng,
    )
    minimum_views = {
        index: 5 if index in conversion_indexes else 3
        for index in cart_indexes
    }
    view_counts = _session_lengths(
        page_view_count,
        session_count,
        minimum_views,
        search_counts,
        rng,
    )
    line_count_by_session = dict(
        zip(sorted(conversion_indexes), line_counts, strict=True)
    )
    conversion_intents: list[ConversionIntent] = []
    day_active_users = refs.active_users(datetime.combine(day, datetime.min.time()))
    if not day_active_users:
        raise ValueError(f"{day} 没有已注册用户，无法生成行为")
    search_local = 1
    click_local = 1
    page_view_local = 1
    cart_local = 1
    favor_local = 1
    day_cutoff = min(end_of_day(day), ctx.data_end_time)

    for session_index in range(session_count):
        session_local = session_index + 1
        session_fact_id = date_key(day) * 1_000_000 + session_local
        session_id = f"S{session_fact_id}"
        is_conversion = session_index in line_count_by_session
        has_cart = session_index in cart_indexes
        is_guest = not has_cart and rng.random() < 0.27
        current_user = day_active_users[
            _long_tail_index(rng, len(day_active_users), 1.45)
        ]
        maximum_second = int(
            (
                day_cutoff - datetime.combine(day, datetime.min.time())
            ).total_seconds()
        )
        session_start = datetime.combine(day, datetime.min.time())
        user = (
            version_at(
                refs.user_versions,
                int(current_user["user_id"]),
                session_start,
            )
            if not is_guest
            else None
        )
        channel = _choose_channel(refs, rng, is_conversion)
        region = (
            refs.regions_by_district.get(str(user.get("district_code")))
            if user and user.get("district_code") is not None
            else None
        )
        required_lines = line_count_by_session.get(session_index, 1)
        eligible_profiles = eligible_profiles_by_lines[required_lines]
        if is_conversion:
            service_warehouse_id = int(
                refs.service_warehouse(region)["warehouse_id"]
            )
            eligible_profiles = eligible_profiles_by_fulfillment.get(
                (service_warehouse_id, required_lines),
                [],
            )
        if not eligible_profiles:
            raise ValueError(
                f"{day} 没有可承载 {required_lines} 个订单行的商品"
            )
        profile = _choose_profile(eligible_profiles, day, rng)
        pages = _page_sequence(
            refs,
            view_counts[session_index],
            is_conversion,
            has_cart,
            search_counts[session_index] > 0,
            rng,
        )
        stay_durations = [
            _stay_duration(str(page["page_id"]), str(channel["channel_code"]), rng)
            for page in pages
        ]
        planned_seconds = (
            sum(min(duration, 180) for duration in stay_durations[:-1])
            + search_counts[session_index] * 50
            + (75 if is_conversion else 35 if has_cart else 15)
        )
        latest_start_second = max(0, maximum_second - planned_seconds)
        start_second = _session_start_second(rng, latest_start_second)
        session_start = datetime.combine(day, datetime.min.time()) + timedelta(
            seconds=start_second
        )
        session_start = min(session_start, day_cutoff)
        if user is not None:
            user = version_at(
                refs.user_versions,
                int(current_user["user_id"]),
                session_start,
            )
        region = (
            refs.regions_by_district.get(str(user.get("district_code")))
            if user and user.get("district_code") is not None
            else None
        )
        if user is not None:
            user_id = int(user["user_id"])
            state.user_session_counts[user_id] = (
                state.user_session_counts.get(user_id, 0) + 1
            )
            root_category = str(profile.category["root_category_name"])
            category_counts = state.user_category_counts.setdefault(user_id, {})
            category_counts[root_category] = category_counts.get(root_category, 0) + 1
        client = _client_attributes(str(channel["channel_code"]), rng)
        device_id = (
            f"USERDEV{user['user_id']}"
            if user
            else f"GUESTDEV{date_key(day)}{session_local:06d}"
        )
        session_search_count = search_counts[session_index]
        journey = SessionJourney(session_id, session_start, session_start)
        search_ids: list[int] = []
        previous_keyword: str | None = None
        for search_index in range(session_search_count):
            search_id = date_key(day) * 1_000_000 + search_local
            search_time = min(
                session_start + timedelta(seconds=20 + search_index * 35),
                day_cutoff,
            )
            journey.observe(search_time)
            keyword, is_no_result = _search_keyword(
                profile,
                rng,
                previous_keyword,
            )
            previous_keyword = keyword
            result_count = 0 if is_no_result else 1 + int(rng.paretovariate(1.8) * 8)
            writer.add(
                "dwd_traffic_search_di",
                {
                    "search_detail_id": search_id,
                    "event_no": f"SEARCH{search_id}",
                    "event_date_key": date_key(day),
                    "user_sk": user["user_sk"] if user else UNKNOWN_SK,
                    "user_id": user["user_id"] if user else None,
                    "device_id": device_id,
                    "session_id": session_id,
                    "channel_sk": channel["channel_sk"],
                    "channel_code": channel["channel_code"],
                    "search_keyword": keyword,
                    "normalized_keyword": keyword.lower(),
                    "search_source": "站内搜索框",
                    "result_total_count": min(result_count, 500),
                    "is_no_result": int(is_no_result),
                    "is_search_success": int(not is_no_result),
                    "event_time": search_time,
                    "biz_date": day,
                }
                | fact_audit(f"search:{search_id}", batch_id),
            )
            search_ids.append(search_id)
            click_count = 0
            if not is_no_result and (is_conversion or rng.random() < 0.64):
                click_count = rng.choices([1, 2, 3], [82, 15, 3], k=1)[0]
            for click_index in range(click_count):
                click_profile = (
                    profile
                    if click_index == 0
                    else _choose_profile(active_profiles, day, rng)
                )
                click_id = date_key(day) * 1_000_000 + click_local
                click_time = min(
                    search_time + timedelta(seconds=5 + click_index * 9),
                    day_cutoff,
                )
                journey.observe(click_time)
                writer.add(
                    "dwd_traffic_search_click_di",
                    {
                        "search_click_id": click_id,
                        "search_detail_id": search_id,
                        "event_no": f"SEARCHCLICK{click_id}",
                        "event_date_key": date_key(day),
                        "user_sk": user["user_sk"] if user else UNKNOWN_SK,
                        "user_id": user["user_id"] if user else None,
                        "device_id": device_id,
                        "session_id": session_id,
                        "channel_sk": channel["channel_sk"],
                        "channel_code": channel["channel_code"],
                        "click_sku_sk": click_profile.sku["sku_sk"],
                        "click_sku_id": click_profile.sku["sku_id"],
                        "click_spu_sk": click_profile.spu["spu_sk"],
                        "click_spu_id": click_profile.spu["spu_id"],
                        "click_shop_sk": click_profile.shop["shop_sk"],
                        "click_shop_id": click_profile.shop["shop_id"],
                        "click_category_sk": click_profile.category["category_sk"],
                        "click_category_id": click_profile.category["category_id"],
                        "click_rank": min(100, 1 + int(rng.paretovariate(2.4) * 2)),
                        "event_time": click_time,
                        "biz_date": day,
                    }
                    | fact_audit(f"search-click:{click_id}", batch_id),
                )
                click_local += 1
            search_local += 1

        session_view_count = view_counts[session_index]
        last_page = None
        page_time = session_start
        for view_index, page in enumerate(pages):
            page_view_id = date_key(day) * 1_000_000 + page_view_local
            event_time = min(page_time, day_cutoff)
            journey.observe(event_time)
            product_page = page["page_id"] in {
                "PRODUCT",
                "CART",
                "ORDER",
                "CHECKOUT",
            }
            active_promotions = refs.active_promotions(event_time)
            promotion = active_promotions[0] if active_promotions else None
            writer.add(
                "dwd_traffic_page_view_di",
                {
                    "page_view_id": page_view_id,
                    "event_no": f"PV{page_view_id}",
                    "event_date_key": date_key(day),
                    "user_sk": user["user_sk"] if user else UNKNOWN_SK,
                    "user_id": user["user_id"] if user else None,
                    "device_id": device_id,
                    "session_id": session_id,
                    "page_sk": page["page_sk"],
                    "page_id": page["page_id"],
                    "last_page_sk": last_page["page_sk"]
                    if last_page
                    else UNKNOWN_SK,
                    "last_page_id": last_page["page_id"] if last_page else None,
                    "channel_sk": channel["channel_sk"],
                    "channel_code": channel["channel_code"],
                    "shop_sk": profile.shop["shop_sk"]
                    if product_page
                    else UNKNOWN_SK,
                    "shop_id": profile.shop["shop_id"] if product_page else None,
                    "sku_sk": profile.sku["sku_sk"]
                    if product_page
                    else UNKNOWN_SK,
                    "sku_id": profile.sku["sku_id"] if product_page else None,
                    "spu_sk": profile.spu["spu_sk"]
                    if product_page
                    else UNKNOWN_SK,
                    "spu_id": profile.spu["spu_id"] if product_page else None,
                    "category_sk": profile.category["category_sk"]
                    if product_page
                    else UNKNOWN_SK,
                    "category_id": profile.category["category_id"]
                    if product_page
                    else None,
                    "promotion_version_sk": promotion["promotion_version_sk"]
                    if promotion
                    else UNKNOWN_SK,
                    "promotion_id": promotion["promotion_id"]
                    if promotion
                    else None,
                    "search_detail_id": search_ids[-1] if search_ids else None,
                    "business_type": None,
                    "business_id": None,
                    "client_type": channel["platform_type"],
                    "app_version": client["app_version"],
                    "os_type": client["os_type"],
                    "region_sk": region["region_sk"] if region else UNKNOWN_SK,
                    "region_code": region["region_code"] if region else None,
                    "stay_duration_sec": stay_durations[view_index],
                    "event_time": event_time,
                    "biz_date": day,
                }
                | fact_audit(f"page-view:{page_view_id}", batch_id),
            )
            page_view_local += 1
            last_page = page
            page_time = event_time + timedelta(
                seconds=min(stay_durations[view_index], 180)
            )

        cart_quantity = 0
        if has_cart and user is not None:
            cart_time = min(
                day_cutoff,
                journey.last_event_time + timedelta(seconds=rng.randint(3, 18)),
            )
            journey.observe(cart_time)
            price_point = profile.price_on(day)
            cart_key = (int(user["user_id"]), int(profile.sku["sku_id"]))
            existing = state.cart_positions.get(cart_key)
            before_quantity = existing.quantity if existing is not None else 0
            add_quantity = rng.choices([1, 2], [86, 14], k=1)[0]
            cart_quantity = before_quantity + add_quantity
            _write_cart_event(
                writer,
                batch_id,
                date_key(day) * 1_000_000 + cart_local,
                user,
                device_id,
                session_id,
                profile,
                channel,
                "加入",
                add_quantity,
                cart_quantity,
                price_point.sale_price,
                cart_time,
            )
            state.cart_positions[cart_key] = CartPosition(cart_quantity, cart_time)
            cart_local += 1
            if not is_conversion and rng.random() < 0.18:
                mutation_time = min(
                    day_cutoff,
                    cart_time + timedelta(seconds=rng.randint(8, 90)),
                )
                journey.observe(mutation_time)
                mutation = rng.choices(
                    ["数量变更", "删除", "清空"],
                    [58, 29, 13],
                    k=1,
                )[0]
                delta = 0
                if mutation == "数量变更":
                    delta = 1 if rng.random() < 0.64 else -1
                    if cart_quantity + delta <= 0:
                        mutation = "删除"
                if mutation in {"删除", "清空"}:
                    delta = -cart_quantity
                after_quantity = cart_quantity + delta
                _write_cart_event(
                    writer,
                    batch_id,
                    date_key(day) * 1_000_000 + cart_local,
                    user,
                    device_id,
                    session_id,
                    profile,
                    channel,
                    mutation,
                    delta,
                    after_quantity,
                    price_point.sale_price,
                    mutation_time,
                )
                cart_local += 1
                cart_quantity = after_quantity
                if after_quantity:
                    state.cart_positions[cart_key] = CartPosition(
                        after_quantity,
                        mutation_time,
                    )
                else:
                    state.cart_positions.pop(cart_key, None)
        if is_conversion and user is not None:
            order_time = min(
                day_cutoff,
                journey.last_event_time + timedelta(seconds=rng.randint(12, 75)),
            )
            journey.observe(order_time)
            session_end = journey.close(day_cutoff, rng.randint(8, 45))
            conversion_intents.append(
                ConversionIntent(
                    order_time=order_time,
                    session_start_time=session_start,
                    session_end_time=session_end,
                    session_id=session_id,
                    device_id=device_id,
                    user=user,
                    channel=channel,
                    region=region,
                    primary_sku_id=int(profile.sku["sku_id"]),
                    cart_quantity=cart_quantity,
                    line_count=line_count_by_session[session_index],
                )
            )
        else:
            session_end = journey.close(day_cutoff, rng.randint(5, 55))

        if user is not None and not is_conversion and rng.random() < 0.08:
            favor_event_id = date_key(day) * 1_000_000 + favor_local
            user_id = int(user["user_id"])
            favorites = state.favorite_skus_by_user.setdefault(user_id, set())
            favorite_profile = profile
            cancel_favorite = bool(favorites) and rng.random() < 0.16
            if cancel_favorite:
                favorite_sku_id = rng.choice(tuple(favorites))
                favorite_profile = refs.profile_by_sku[favorite_sku_id]
                favorites.remove(favorite_sku_id)
            else:
                favorites.add(int(profile.sku["sku_id"]))
            favor_time = min(
                session_end,
                journey.last_event_time + timedelta(seconds=rng.randint(2, 20)),
            )
            journey.observe(favor_time)
            session_end = journey.close(day_cutoff, rng.randint(5, 55))
            writer.add(
                "dwd_interaction_favor_event_di",
                {
                    "favor_event_id": favor_event_id,
                    "event_no": f"FAVOR{favor_event_id}",
                    "event_date_key": date_key(day),
                    "user_sk": user["user_sk"],
                    "user_id": user["user_id"],
                    "device_id": device_id,
                    "session_id": session_id,
                    "shop_sk": favorite_profile.shop["shop_sk"],
                    "shop_id": favorite_profile.shop["shop_id"],
                    "sku_sk": favorite_profile.sku["sku_sk"],
                    "sku_id": favorite_profile.sku["sku_id"],
                    "spu_sk": favorite_profile.spu["spu_sk"],
                    "spu_id": favorite_profile.spu["spu_id"],
                    "channel_sk": channel["channel_sk"],
                    "channel_code": channel["channel_code"],
                    "favor_target_type": "商品",
                    "favor_event_type": "取消收藏" if cancel_favorite else "收藏",
                    "event_time": favor_time,
                    "biz_date": day,
                }
                | fact_audit(f"favor:{favor_event_id}", batch_id),
            )
            favor_local += 1

        if user is not None:
            state.record_activity(int(user["user_id"]), session_end)

        entry_page = pages[0]
        exit_page = pages[-1]
        writer.add(
            "dwd_traffic_session_di",
            {
                "session_fact_id": session_fact_id,
                "session_id": session_id,
                "session_date_key": date_key(day),
                "user_sk": user["user_sk"] if user else UNKNOWN_SK,
                "user_id": user["user_id"] if user else None,
                "device_id": device_id,
                "channel_sk": channel["channel_sk"],
                "channel_code": channel["channel_code"],
                "entry_page_sk": entry_page["page_sk"],
                "entry_page_id": entry_page["page_id"],
                "exit_page_sk": exit_page["page_sk"],
                "exit_page_id": exit_page["page_id"],
                "region_sk": region["region_sk"] if region else UNKNOWN_SK,
                "region_code": region["region_code"] if region else None,
                "client_type": channel["platform_type"],
                "app_version": client["app_version"],
                "os_type": client["os_type"],
                "ip_masked": f"10.{session_index % 255}.***.***",
                "page_view_count": session_view_count,
                "search_count": session_search_count,
                "session_duration_sec": max(
                    0,
                    int((session_end - session_start).total_seconds()),
                ),
                "is_bounce": int(session_view_count == 1),
                "session_start_time": session_start,
                "session_end_time": session_end,
                "biz_date": day,
            }
            | fact_audit(f"session:{session_fact_id}", batch_id),
        )
    return conversion_intents


def _write_cart_event(
    writer: TableWriter,
    batch_id: str,
    cart_event_id: int,
    user: dict,
    device_id: str,
    session_id: str,
    profile,
    channel: dict,
    event_type: str,
    quantity_delta: int,
    quantity_after: int,
    unit_price,
    event_time: datetime,
) -> None:
    writer.add(
        "dwd_interaction_cart_event_di",
        {
            "cart_event_id": cart_event_id,
            "event_no": f"CART{cart_event_id}",
            "event_date_key": date_key(event_time),
            "user_sk": user["user_sk"],
            "user_id": user["user_id"],
            "device_id": device_id,
            "session_id": session_id,
            "shop_sk": profile.shop["shop_sk"],
            "shop_id": profile.shop["shop_id"],
            "sku_sk": profile.sku["sku_sk"],
            "sku_id": profile.sku["sku_id"],
            "spu_sk": profile.spu["spu_sk"],
            "spu_id": profile.spu["spu_id"],
            "category_sk": profile.category["category_sk"],
            "category_id": profile.category["category_id"],
            "channel_sk": channel["channel_sk"],
            "channel_code": channel["channel_code"],
            "cart_event_type": event_type,
            "cart_source": "商品详情页" if event_type == "加入" else "购物车页",
            "sku_qty_delta": quantity_delta,
            "cart_sku_qty_after": quantity_after,
            "sku_unit_price": unit_price,
            "currency_code": "CNY",
            "event_time": event_time,
            "biz_date": event_time.date(),
        }
        | fact_audit(f"cart:{cart_event_id}", batch_id),
    )


def _order_line_counts(target: int, rng: random.Random) -> list[int]:
    counts: list[int] = []
    remaining = target
    while remaining:
        draw = rng.random()
        desired = 1 if draw < 0.58 else 2 if draw < 0.9 else 3
        count = min(desired, remaining)
        counts.append(count)
        remaining -= count
    return counts


def _session_lengths(
    total: int,
    session_count: int,
    minimum_views: dict[int, int],
    search_counts: list[int],
    rng: random.Random,
) -> list[int]:
    values = [minimum_views.get(index, 1) for index in range(session_count)]
    remaining = total - sum(values)
    if remaining < 0:
        raise ValueError("页面访问量不足以承载会话行为路径")
    candidates = list(range(session_count))
    while remaining:
        index = rng.choice(candidates)
        weight = min(0.96, 0.30 + search_counts[index] * 0.14 + values[index] * 0.08)
        if rng.random() > weight:
            continue
        values[index] += 1
        remaining -= 1
        if values[index] >= 12:
            candidates.remove(index)
            if not candidates and remaining:
                raise ValueError("会话长度上限不足以承载页面访问量")
    return values


def _cart_session_indexes(
    session_count: int,
    conversion_indexes: set[int],
    rng: random.Random,
) -> set[int]:
    target = min(
        session_count,
        max(
            len(conversion_indexes),
            round(len(conversion_indexes) * 2.35),
            round(session_count * 0.04),
        ),
    )
    result = set(conversion_indexes)
    while len(result) < target:
        result.add(rng.randrange(session_count))
    return result


def _search_counts(
    total: int,
    session_count: int,
    conversion_indexes: set[int],
    cart_indexes: set[int],
    rng: random.Random,
) -> list[int]:
    values = [0] * session_count
    assigned = 0
    while assigned < total:
        index = rng.randrange(session_count)
        if values[index] >= 5:
            continue
        acceptance = 0.28
        if index in cart_indexes:
            acceptance += 0.18
        if index in conversion_indexes:
            acceptance += 0.24
        acceptance += values[index] * 0.06
        if rng.random() > min(0.92, acceptance):
            continue
        values[index] += 1
        assigned += 1
    return values


def _long_tail_index(rng: random.Random, size: int, exponent: float) -> int:
    return min(size - 1, int((rng.random() ** exponent) * size))


def _choose_channel(
    refs: ReferenceData,
    rng: random.Random,
    is_conversion: bool,
) -> dict:
    weights = (
        {
            "APP": 52,
            "H5": 15,
            "PC": 14,
            "MINI_PROGRAM": 17,
            "SEM": 2,
            "OFFLINE": 0,
        }
        if is_conversion
        else {
            "APP": 39,
            "H5": 24,
            "PC": 14,
            "MINI_PROGRAM": 15,
            "SEM": 8,
            "OFFLINE": 0,
        }
    )
    return rng.choices(
        refs.channels,
        weights=[weights.get(str(row["channel_code"]), 1) for row in refs.channels],
        k=1,
    )[0]


def _page_sequence(
    refs: ReferenceData,
    count: int,
    is_conversion: bool,
    has_cart: bool,
    has_search: bool,
    rng: random.Random,
) -> list[dict]:
    if is_conversion:
        entry = "SEARCH" if has_search else rng.choice(["HOME", "CATEGORY", "PRODUCT"])
        middle_count = count - 5
        middle = rng.choices(
            ["CATEGORY", "SEARCH", "PRODUCT", "SHOP"],
            [16, 18 if has_search else 6, 54, 12],
            k=middle_count,
        )
        page_ids = [entry, *middle, "PRODUCT", "CART", "CHECKOUT", "ORDER"]
        return [refs.pages_by_id[page_id] for page_id in page_ids]

    if has_cart:
        entry = "SEARCH" if has_search else rng.choice(["HOME", "CATEGORY", "PRODUCT"])
        middle_count = count - 3
        middle = rng.choices(
            ["CATEGORY", "SEARCH", "PRODUCT", "SHOP"],
            [16, 20 if has_search else 7, 58, 13],
            k=middle_count,
        )
        page_ids = [entry, *middle, "PRODUCT", "CART"]
        return [refs.pages_by_id[page_id] for page_id in page_ids]

    current = (
        "SEARCH"
        if has_search and rng.random() < 0.58
        else rng.choices(
            ["HOME", "CATEGORY", "PRODUCT", "SHOP"],
            [43, 18, 32, 7],
            k=1,
        )[0]
    )
    page_ids = [current]
    transitions = {
        "HOME": (["CATEGORY", "SEARCH", "PRODUCT", "SHOP"], [28, 31, 34, 7]),
        "CATEGORY": (["CATEGORY", "SEARCH", "PRODUCT", "HOME"], [18, 16, 58, 8]),
        "SEARCH": (["SEARCH", "PRODUCT", "CATEGORY", "HOME"], [18, 62, 12, 8]),
        "PRODUCT": (["PRODUCT", "SHOP", "SEARCH", "CATEGORY", "HOME"], [48, 14, 16, 14, 8]),
        "SHOP": (["PRODUCT", "SHOP", "SEARCH", "HOME"], [57, 18, 15, 10]),
    }
    while len(page_ids) < count:
        choices, weights = transitions[current]
        current = rng.choices(choices, weights=weights, k=1)[0]
        page_ids.append(current)
    return [refs.pages_by_id[page_id] for page_id in page_ids]


def _stay_duration(
    page_id: str,
    channel_code: str,
    rng: random.Random,
) -> int:
    parameters = {
        "HOME": (3.75, 0.55),
        "CATEGORY": (4.05, 0.62),
        "SEARCH": (4.15, 0.64),
        "PRODUCT": (4.65, 0.72),
        "SHOP": (4.20, 0.65),
        "CART": (3.85, 0.60),
        "CHECKOUT": (4.10, 0.58),
        "ORDER": (3.45, 0.52),
    }
    mean, sigma = parameters.get(page_id, (4.0, 0.65))
    if channel_code == "PC":
        mean += 0.10
    elif channel_code in {"H5", "MINI_PROGRAM"}:
        mean -= 0.08
    return max(4, min(900, round(rng.lognormvariate(mean, sigma))))


def _session_start_second(rng: random.Random, maximum_second: int) -> int:
    hours = list(range(24))
    weights = [
        1,
        1,
        1,
        1,
        2,
        4,
        8,
        14,
        22,
        27,
        31,
        38,
        48,
        43,
        35,
        32,
        34,
        42,
        57,
        72,
        83,
        76,
        47,
        20,
    ]
    eligible_hours = [hour for hour in hours if hour * 3600 <= maximum_second]
    eligible_weights = weights[: len(eligible_hours)]
    hour = rng.choices(eligible_hours, weights=eligible_weights, k=1)[0]
    upper = min(3599, maximum_second - hour * 3600)
    return hour * 3600 + rng.randrange(upper + 1)


def _client_attributes(channel_code: str, rng: random.Random) -> dict[str, str | None]:
    if channel_code == "APP":
        os_type = rng.choices(["Android", "iOS", "HarmonyOS"], [63, 29, 8], k=1)[0]
        version = rng.choices(
            ["8.7.1", "8.7.0", "8.6.2", "8.5.4"],
            [52, 28, 15, 5],
            k=1,
        )[0]
        return {"os_type": os_type, "app_version": version}
    if channel_code in {"H5", "MINI_PROGRAM"}:
        os_type = rng.choices(["Android", "iOS", "HarmonyOS"], [67, 27, 6], k=1)[0]
        return {"os_type": os_type, "app_version": None}
    if channel_code == "PC":
        os_type = rng.choices(["Windows", "macOS", "Linux"], [82, 16, 2], k=1)[0]
        return {"os_type": os_type, "app_version": None}
    return {"os_type": None, "app_version": None}


def _choose_profile(
    profiles: list,
    day: date,
    rng: random.Random,
):
    candidates = [
        profiles[_long_tail_index(rng, len(profiles), 2.35)] for _ in range(6)
    ]
    seasonal_roots = {
        1: {"家用电器", "食品饮料", "服饰鞋包"},
        2: {"美妆个护", "服饰鞋包", "运动户外"},
        6: {"手机数码", "电脑办公", "家用电器"},
        9: {"电脑办公", "母婴玩具", "服饰鞋包"},
        11: {"手机数码", "家用电器", "美妆个护"},
        12: {"食品饮料", "家居家装", "服饰鞋包"},
    }
    seasonal = seasonal_roots.get(day.month, set())
    return rng.choices(
        candidates,
        weights=[
            (1.8 if str(profile.category["root_category_name"]) in seasonal else 1.0)
            * (0.62 if int(profile.shop.get("is_self_operated") or 0) else 1.0)
            for profile in candidates
        ],
        k=1,
    )[0]


def _search_keyword(
    profile,
    rng: random.Random,
    previous_keyword: str | None,
) -> tuple[str, bool]:
    brand = str(profile.brand["brand_name"]) if profile.brand else ""
    root = str(profile.category.get("root_category_name") or "")
    leaf = str(profile.category.get("category_name") or root)
    title = str(profile.spu["spu_name"])
    title_parts = [part for part in title.replace("/", " ").split() if len(part) >= 2]
    short_title = " ".join(title_parts[:2])[:32] or title[:24]
    if previous_keyword and rng.random() < 0.22:
        keyword = rng.choice([brand, leaf, short_title]) or leaf
    else:
        keyword = rng.choices(
            [brand, leaf, f"{brand} {leaf}".strip(), short_title, f"{leaf} 推荐"],
            [15, 27, 24, 28, 6],
            k=1,
        )[0]
    is_no_result = rng.random() < 0.075
    if is_no_result:
        keyword = rng.choice(
            [
                f"{keyword} 冷门型号{rng.randrange(1000, 9999)}",
                keyword[:-1] if len(keyword) > 2 else f"{keyword}x",
                f"{keyword} 已停产",
            ]
        )
    return keyword[:80], is_no_result
