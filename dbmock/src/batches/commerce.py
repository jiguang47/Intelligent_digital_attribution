"""由转化会话生成订单并推进履约和库存状态"""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

import chinese_calendar

from ..reference import ProductProfile, ReferenceData
from ..settings import RunContext
from ..support import (
    MONEY_ZERO,
    UNKNOWN_SK,
    TableWriter,
    date_key,
    end_of_day,
    fact_audit,
    money,
    price,
    start_of_day,
    version_at,
)
from ..timeline import (
    BusinessState,
    CartPosition,
    ConversionIntent,
    InventoryPosition,
    ScheduledFact,
)

WEIGHT_QUANT = Decimal("0.001")


@dataclass(frozen=True, slots=True)
class OrderLinePlan:
    profile: ProductProfile
    quantity: int


@dataclass(frozen=True, slots=True)
class OrderPlan:
    intent: ConversionIntent
    lines: tuple[OrderLinePlan, ...]


def _emit_due_facts(
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
) -> None:
    due: list[ScheduledFact] = []
    future: list[ScheduledFact] = []
    for fact in state.pending_facts:
        if fact.event_time <= cutoff:
            due.append(fact)
        else:
            future.append(fact)
    state.pending_facts = future
    for fact in sorted(
        due,
        key=lambda item: (
            item.event_time,
            item.table_name,
            item.source_record_id,
        ),
    ):
        user_id = fact.row.get("user_id")
        if user_id is not None and "user_sk" in fact.row:
            user = version_at(refs.user_versions, int(user_id), fact.event_time)
            fact = ScheduledFact(
                table_name=fact.table_name,
                source_record_id=fact.source_record_id,
                event_time=fact.event_time,
                row=fact.row | {"user_sk": user["user_sk"]},
            )
        _emit_fact(writer, batch_id, fact)


def _schedule_fact(
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    table_name: str,
    source_record_id: str,
    event_time: datetime,
    row: dict[str, Any],
) -> None:
    fact = ScheduledFact(
        table_name=table_name,
        source_record_id=source_record_id,
        event_time=event_time,
        row=row,
    )
    if event_time <= cutoff:
        _emit_fact(writer, batch_id, fact)
    else:
        state.pending_facts.append(fact)


def _emit_fact(
    writer: TableWriter,
    batch_id: str,
    fact: ScheduledFact,
) -> None:
    writer.add(
        fact.table_name,
        fact.row | fact_audit(fact.source_record_id, batch_id),
    )


def generate_day(
    ctx: RunContext,
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    day: date,
    batch_id: str,
    intents: list[ConversionIntent],
) -> None:
    rng = random.Random(f"{ctx.gen.seed}:{day}:commerce")
    cutoff = min(end_of_day(day), ctx.data_end_time)
    _emit_due_facts(refs, state, writer, cutoff, batch_id)
    plans = _build_order_plans(refs, day, intents, rng)
    inventory_events = _inventory_opening_events(refs, state, day)
    due_events, future_events = _take_due_inventory_events(
        state.pending_inventory_events,
        cutoff,
    )
    state.pending_inventory_events = future_events
    inventory_events.extend(due_events)
    inventory_events.extend(_replenishment_events(refs, state, day, plans, due_events))
    inventory_events.sort(key=_inventory_event_sort_key)
    for order_index, plan in enumerate(plans, start=1):
        due_before_order, inventory_events = _partition_inventory_events(
            inventory_events,
            plan.intent.order_time,
        )
        _apply_inventory_events(
            refs,
            state,
            writer,
            due_before_order,
            batch_id,
        )
        order_inventory_events = _write_order(
            ctx,
            refs,
            state,
            writer,
            day,
            cutoff,
            batch_id,
            order_index,
            plan,
            rng,
        )
        for event in order_inventory_events:
            if event["event_time"] <= cutoff:
                inventory_events.append(event)
            else:
                state.pending_inventory_events.append(event)
        inventory_events.sort(key=_inventory_event_sort_key)

    _apply_inventory_events(
        refs,
        state,
        writer,
        inventory_events,
        batch_id,
    )


def _build_order_plans(
    refs: ReferenceData,
    day: date,
    intents: list[ConversionIntent],
    rng: random.Random,
) -> list[OrderPlan]:
    active_by_shop: dict[tuple[int, int], list[ProductProfile]] = defaultdict(list)
    for profile in refs.active_profiles(day):
        warehouse_id = int(refs.warehouse_for_profile(profile)["warehouse_id"])
        active_by_shop[(warehouse_id, int(profile.shop["shop_id"]))].append(profile)
    plans: list[OrderPlan] = []
    for intent in intents:
        primary = refs.profile_by_sku[int(intent.primary_sku_id)]
        warehouse_id = int(refs.warehouse_for_profile(primary)["warehouse_id"])
        shop_profiles = active_by_shop[
            (warehouse_id, int(primary.shop["shop_id"]))
        ]
        if len(shop_profiles) < intent.line_count:
            eligible_shops = [
                profiles
                for profiles in active_by_shop.values()
                if len(profiles) >= intent.line_count
            ]
            if not eligible_shops:
                raise ValueError(
                    f"{day} 没有可承载 {intent.line_count} 个订单行的店铺"
                )
            shop_profiles = eligible_shops[
                int(intent.user["user_id"]) % len(eligible_shops)
            ]
            primary = shop_profiles[
                int(intent.primary_sku_id) % len(shop_profiles)
            ]
        lines = [OrderLinePlan(primary, max(1, min(3, intent.cart_quantity)))]
        used = {int(primary.sku["sku_id"])}
        while len(lines) < intent.line_count:
            candidate = shop_profiles[rng.randrange(len(shop_profiles))]
            sku_id = int(candidate.sku["sku_id"])
            if sku_id in used and len(shop_profiles) > len(used):
                continue
            used.add(sku_id)
            lines.append(OrderLinePlan(candidate, 1 + (len(lines) % 2)))
        plans.append(OrderPlan(intent, tuple(lines)))
    return sorted(plans, key=lambda plan: plan.intent.order_time)


def _inventory_opening_events(
    refs: ReferenceData,
    state: BusinessState,
    day: date,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for profile in refs.profiles_by_listing_date.get(day, []):
        sku_id = int(profile.sku["sku_id"])
        if sku_id in state.inventory:
            continue
        state.inventory[sku_id] = InventoryPosition(
            on_hand=0,
            reserved=0,
            in_transit=0,
            unit_cost=profile.price_on(day).cost_price,
        )
        events.append(
            _inventory_event(
                profile,
                "INITIAL_STOCK",
                "PURCHASE_RECEIPT",
                f"OPENING-{sku_id}",
                profile.initial_stock_qty,
                0,
                profile.price_on(day).cost_price,
                start_of_day(day),
                10,
            )
        )
    return events


def _take_due_inventory_events(
    pending: list[dict[str, Any]],
    cutoff: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    due: list[dict[str, Any]] = []
    future: list[dict[str, Any]] = []
    for event in pending:
        if event["event_time"] <= cutoff:
            due.append(event)
        else:
            future.append(event)
    return due, future


def _inventory_event_sort_key(event: dict[str, Any]) -> tuple:
    return (
        event["event_time"],
        event["priority"],
        event["sku_id"],
        str(event["biz_id"]),
    )


def _partition_inventory_events(
    events: list[dict[str, Any]],
    cutoff: datetime,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    boundary = 0
    while boundary < len(events) and events[boundary]["event_time"] <= cutoff:
        boundary += 1
    return events[:boundary], events[boundary:]


def _replenishment_events(
    refs: ReferenceData,
    state: BusinessState,
    day: date,
    plans: list[OrderPlan],
    due_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    demand: dict[int, int] = defaultdict(int)
    for plan in plans:
        for line in plan.lines:
            demand[int(line.profile.sku["sku_id"])] += line.quantity
    due_delta: dict[int, int] = defaultdict(int)
    due_transit_delta: dict[int, int] = defaultdict(int)
    for event in due_events:
        due_delta[int(event["sku_id"])] += int(event["on_hand_delta"])
        due_transit_delta[int(event["sku_id"])] += int(event["in_transit_delta"])
    events: list[dict[str, Any]] = []
    for sku_id, required in demand.items():
        profile = refs.profile_by_sku[sku_id]
        position = state.inventory[sku_id]
        projected_available = position.available + due_delta[sku_id]
        projected_supply = (
            projected_available
            + position.in_transit
            + due_transit_delta[sku_id]
        )
        target = required + profile.warning_stock_qty
        if projected_supply >= target:
            continue
        receipt = max(
            profile.initial_stock_qty,
            target * 2 - projected_available,
        )
        supply_factor = Decimal(85 + (sku_id + date_key(day)) % 36) / Decimal(100)
        receipt = max(target - projected_available, int(receipt * supply_factor))
        order_time = start_of_day(day) + timedelta(minutes=1)
        lead_days = 2 + (sku_id + date_key(day)) % 8
        receipt_time = order_time + timedelta(
            days=lead_days,
            hours=2 + sku_id % 8,
        )
        events.append(
            _inventory_event(
                profile,
                "PURCHASE_ORDER",
                "REPLENISHMENT",
                f"PO-{date_key(day)}-{sku_id}",
                0,
                0,
                profile.price_on(day).cost_price,
                order_time,
                20,
                receipt,
            )
        )
        state.pending_inventory_events.append(
            _inventory_event(
                profile,
                "PURCHASE_RECEIPT",
                "REPLENISHMENT",
                f"GR-{date_key(day)}-{sku_id}",
                receipt,
                0,
                profile.price_on(day).cost_price,
                receipt_time,
                20,
                -receipt,
            )
        )
    return events


def _write_order(
    ctx: RunContext,
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    day: date,
    cutoff: datetime,
    batch_id: str,
    order_index: int,
    plan: OrderPlan,
    rng: random.Random,
) -> list[dict[str, Any]]:
    intent = plan.intent
    order_id = date_key(day) * 1_000_000 + order_index
    user = intent.user
    primary = plan.lines[0].profile
    shop = primary.shop
    seller = refs.sellers_by_id.get(int(shop["seller_id"]))
    line_amounts = [
        money(line.profile.price_on(day).sale_price * line.quantity)
        for line in plan.lines
    ]
    sale_total = sum(line_amounts, MONEY_ZERO)
    promotion = _eligible_promotion(
        refs,
        primary,
        intent.order_time,
        sale_total,
    )
    coupon = _eligible_coupon(
        refs,
        primary,
        intent.order_time,
        sale_total,
        rng,
    )
    activity_total = _rule_discount(promotion, sale_total)
    coupon_total = min(
        _rule_discount(coupon, sale_total - activity_total),
        sale_total - activity_total,
    )
    points_balance = state.user_points_balances.get(int(user["user_id"]), 0)
    points_total = MONEY_ZERO
    if points_balance >= 100 and rng.random() < 0.18:
        points_total = money(
            min(
                Decimal(points_balance) / Decimal(100),
                (sale_total - activity_total - coupon_total) * Decimal("0.05"),
                Decimal("50"),
            )
        )
    points_to_use = int(points_total * 100)
    activity_allocations = _allocate_money(activity_total, line_amounts)
    coupon_allocations = _allocate_money(coupon_total, line_amounts)
    points_allocations = _allocate_money(points_total, line_amounts)
    freight_total = MONEY_ZERO if sale_total >= Decimal("99") else money("8")
    freight_allocations = _allocate_money(freight_total, line_amounts)
    is_first_order = int(state.user_order_counts.get(int(user["user_id"]), 0) == 0)
    details: list[dict[str, Any]] = []
    inventory_events: list[dict[str, Any]] = []
    insufficient_lines = [
        line
        for line in plan.lines
        if state.inventory[int(line.profile.sku["sku_id"])].available
        < line.quantity
    ]
    has_stock = not insufficient_lines

    for line_index, line in enumerate(plan.lines, start=1):
        profile = line.profile
        detail_id = order_id * 10 + line_index
        point = profile.price_on(day)
        list_amount = money(point.list_price * line.quantity)
        sale_amount = line_amounts[line_index - 1]
        activity_discount = activity_allocations[line_index - 1]
        coupon_discount = coupon_allocations[line_index - 1]
        points_discount = points_allocations[line_index - 1]
        freight = freight_allocations[line_index - 1]
        tax = (
            money(sale_amount * Decimal("0.05"))
            if int(shop["is_cross_border"])
            else MONEY_ZERO
        )
        receivable = money(
            sale_amount
            - activity_discount
            - coupon_discount
            - points_discount
            + freight
            + tax
        )
        row = {
            "order_detail_id": detail_id,
            "order_id": order_id,
            "parent_order_id": None,
            "trade_no": f"T{order_id}",
            "order_no": f"O{order_id}",
            "source_session_id": intent.session_id,
            "order_date_key": date_key(day),
            "user_sk": user["user_sk"],
            "user_id": user["user_id"],
            "shop_sk": shop["shop_sk"],
            "shop_id": shop["shop_id"],
            "seller_sk": seller["seller_sk"] if seller else UNKNOWN_SK,
            "seller_id": seller["seller_id"] if seller else None,
            "sku_sk": profile.sku["sku_sk"],
            "sku_id": profile.sku["sku_id"],
            "spu_sk": profile.spu["spu_sk"],
            "spu_id": profile.spu["spu_id"],
            "category_sk": profile.category["category_sk"],
            "category_id": profile.category["category_id"],
            "brand_sk": profile.brand["brand_sk"]
            if profile.brand
            else UNKNOWN_SK,
            "brand_id": profile.brand["brand_id"] if profile.brand else None,
            "receiver_region_sk": intent.region["region_sk"]
            if intent.region
            else UNKNOWN_SK,
            "receiver_region_code": intent.region["region_code"]
            if intent.region
            else None,
            "channel_sk": intent.channel["channel_sk"],
            "channel_code": intent.channel["channel_code"],
            "order_source": intent.channel["platform_type"],
            "order_scene": "普通",
            "is_first_order": is_first_order,
            "is_cross_border": shop["is_cross_border"],
            "is_presale": int(profile.spu["is_presale"] or 0),
            "is_gift": 0,
            "is_risk_order": int(rng.random() < 0.008),
            "sku_qty": line.quantity,
            "sku_list_unit_price": point.list_price,
            "sku_sale_unit_price": point.sale_price,
            "list_amount": list_amount,
            "sale_amount": sale_amount,
            "activity_discount_amount": activity_discount,
            "coupon_discount_amount": coupon_discount,
            "points_discount_amount": points_discount,
            "freight_amount": freight,
            "tax_amount": tax,
            "receivable_amount": receivable,
            "cost_amount": money(point.cost_price * line.quantity),
            "currency_code": "CNY",
            "order_create_time": intent.order_time,
            "biz_date": day,
        } | fact_audit(f"order-detail:{detail_id}", batch_id)
        writer.add("dwd_trade_order_detail_di", row)
        details.append(row | {"profile": profile, "quantity": line.quantity})
        if has_stock:
            inventory_events.append(
                _inventory_event(
                    profile,
                    "SALE_RESERVE",
                    "ORDER",
                    order_id,
                    0,
                    line.quantity,
                    point.cost_price,
                    intent.order_time,
                    40,
                )
            )
        if activity_discount and promotion is not None:
            writer.add(
                "dwd_trade_order_detail_activity_di",
                {
                    "order_detail_activity_id": detail_id,
                    "order_detail_id": detail_id,
                    "order_id": order_id,
                    "promotion_version_sk": promotion["promotion_version_sk"],
                    "promotion_id": promotion["promotion_id"],
                    "promotion_discount_amount": activity_discount,
                    "rule_snapshot_json": {
                        "promotion_type": promotion["promotion_type"],
                        "rule_version_no": promotion["rule_version_no"],
                    },
                    "currency_code": "CNY",
                    "order_create_time": intent.order_time,
                    "biz_date": day,
                }
                | fact_audit(f"order-activity:{detail_id}", batch_id),
            )
        if line_index > 1 or int(profile.sku["sku_id"]) != intent.primary_sku_id:
            cart_event_id = detail_id
            cart_time = max(
                start_of_day(day),
                intent.order_time - timedelta(seconds=20 - line_index),
            )
            writer.add(
                "dwd_interaction_cart_event_di",
                {
                    "cart_event_id": cart_event_id,
                    "event_no": f"CART{cart_event_id}",
                    "event_date_key": date_key(cart_time),
                    "user_sk": user["user_sk"],
                    "user_id": user["user_id"],
                    "device_id": intent.device_id,
                    "session_id": intent.session_id,
                    "shop_sk": profile.shop["shop_sk"],
                    "shop_id": profile.shop["shop_id"],
                    "sku_sk": profile.sku["sku_sk"],
                    "sku_id": profile.sku["sku_id"],
                    "spu_sk": profile.spu["spu_sk"],
                    "spu_id": profile.spu["spu_id"],
                    "category_sk": profile.category["category_sk"],
                    "category_id": profile.category["category_id"],
                    "channel_sk": intent.channel["channel_sk"],
                    "channel_code": intent.channel["channel_code"],
                    "cart_event_type": "加入",
                    "cart_source": "商品详情页",
                    "sku_qty_delta": line.quantity,
                    "cart_sku_qty_after": line.quantity,
                    "sku_unit_price": point.sale_price,
                    "currency_code": "CNY",
                    "event_time": cart_time,
                    "biz_date": cart_time.date(),
                }
                | fact_audit(f"cart:{cart_event_id}", batch_id),
            )
            cart_key = (int(user["user_id"]), int(profile.sku["sku_id"]))
            state.cart_positions[cart_key] = CartPosition(
                line.quantity,
                cart_time,
            )

    for line in plan.lines:
        cart_key = (int(user["user_id"]), int(line.profile.sku["sku_id"]))
        position = state.cart_positions.get(cart_key)
        if position is None or position.quantity <= line.quantity:
            state.cart_positions.pop(cart_key, None)
        else:
            position.quantity -= line.quantity
            position.updated_at = intent.order_time

    _write_order_status(
        state,
        writer,
        cutoff,
        batch_id,
        order_id,
        1,
        user,
        shop,
        None,
        "CREATED",
        "CREATE",
        intent.order_time,
        0,
    )
    if not has_stock:
        cancel_time = intent.order_time + timedelta(minutes=1)
        _write_order_status(
            state,
            writer,
            cutoff,
            batch_id,
            order_id,
            2,
            user,
            shop,
            "CREATED",
            "CANCELLED",
            "INVENTORY_SHORTAGE",
            cancel_time,
            1,
            "OUT_OF_STOCK",
            "库存分配失败，订单自动关闭",
        )
        for line in insufficient_lines:
            inventory_events.append(
                _inventory_event(
                    line.profile,
                    "ALLOCATION_FAILED",
                    "ORDER",
                    order_id,
                    0,
                    0,
                    line.profile.price_on(day).cost_price,
                    cancel_time,
                    50,
                )
            )
        if coupon is not None and coupon_total:
            _write_coupon_lifecycle(
                state,
                writer,
                cutoff,
                batch_id,
                order_id,
                user,
                coupon,
                details,
                coupon_allocations,
                intent.order_time,
                cancel_time,
                False,
            )
        return inventory_events
    pay_time = intent.order_time + timedelta(
        seconds=_bounded_lognormal(rng, 4.25, 0.85, 8, 900)
    )
    requested_amount = sum(
        (detail["receivable_amount"] for detail in details),
        MONEY_ZERO,
    )
    payment = _choose_payment(refs, intent.channel, requested_amount, user, rng)
    first_failure_reason = _payment_failure_reason(
        payment,
        requested_amount,
        rng,
    )
    attempts = [(payment, first_failure_reason)]
    if first_failure_reason is not None and rng.random() < 0.43:
        retry_payment = _choose_payment(
            refs,
            intent.channel,
            requested_amount,
            user,
            rng,
        )
        retry_failure = (
            None
            if rng.random() < 0.88
            else _payment_failure_reason(retry_payment, requested_amount, rng, True)
        )
        attempts.append((retry_payment, retry_failure))

    pay_result_time = pay_time
    successful_payment_detail_id: int | None = None
    for attempt_no, (attempt_payment, failure_reason) in enumerate(attempts, start=1):
        retry_delay = (
            _bounded_lognormal(rng, 5.15, 0.70, 45, 1_800)
            if attempt_no > 1
            else 0
        )
        attempt_time = pay_time + timedelta(seconds=retry_delay)
        pay_result_time = attempt_time + timedelta(
            seconds=_bounded_lognormal(rng, 2.35, 0.62, 2, 120)
        )
        pay_detail_id = order_id * 10 + attempt_no
        _write_payment_attempt(
            refs,
            state,
            writer,
            cutoff,
            batch_id,
            pay_detail_id,
            attempt_no,
            order_id,
            user,
            shop,
            seller,
            intent.channel,
            attempt_payment,
            details,
            requested_amount,
            attempt_time,
            pay_result_time,
            failure_reason,
        )
        if failure_reason is None:
            successful_payment_detail_id = pay_detail_id
            payment = attempt_payment
            break
    paid_success = successful_payment_detail_id is not None

    if coupon is not None and coupon_total:
        _write_coupon_lifecycle(
            state,
            writer,
            cutoff,
            batch_id,
            order_id,
            user,
            coupon,
            details,
            coupon_allocations,
            intent.order_time,
            pay_result_time,
            paid_success,
        )

    if not paid_success:
        cancel_time = pay_result_time + timedelta(minutes=1)
        _write_order_status(
            state,
            writer,
            cutoff,
            batch_id,
            order_id,
            2,
            user,
            shop,
            "CREATED",
            "CANCELLED",
            "PAYMENT_FAILED",
            cancel_time,
            1,
        )
        for detail in details:
            profile = detail["profile"]
            inventory_events.append(
                _inventory_event(
                    profile,
                    "SALE_RELEASE",
                    "ORDER_CANCEL",
                    order_id,
                    0,
                    -int(detail["quantity"]),
                    profile.price_on(day).cost_price,
                    cancel_time,
                    50,
                )
            )
        return inventory_events

    state.user_order_counts[int(user["user_id"])] = (
        state.user_order_counts.get(int(user["user_id"]), 0) + 1
    )
    state.user_points_balances[int(user["user_id"])] = max(
        0,
        state.user_points_balances.get(int(user["user_id"]), 0)
        - points_to_use
        + int(requested_amount),
    )
    state.user_spend_amounts[int(user["user_id"])] = money(
        state.user_spend_amounts.get(int(user["user_id"]), MONEY_ZERO)
        + requested_amount
    )
    _write_order_status(
        state,
        writer,
        cutoff,
        batch_id,
        order_id,
        2,
        user,
        shop,
        "CREATED",
        "PAID",
        "PAY_SUCCESS",
        pay_result_time,
        0,
    )
    delivery_create_time = pay_result_time + timedelta(
        seconds=_bounded_lognormal(rng, 8.90, 0.75, 1_800, 86_400)
    )
    delivery_create_time = _move_out_of_quiet_fulfillment_hours(
        delivery_create_time,
        rng,
    )
    ship_time = delivery_create_time + timedelta(
        seconds=_bounded_lognormal(rng, 10.35, 0.62, 7_200, 259_200)
    )
    warehouse = refs.warehouse_for_profile(primary)
    logistics = refs.logistics[order_index % len(refs.logistics)]
    signed_time = ship_time + timedelta(
        seconds=_delivery_transit_seconds(
            rng,
            warehouse,
            intent.region,
            logistics,
        )
    )
    delivery_groups = [details]
    if len(details) > 1 and rng.random() < 0.14:
        delivery_groups = [details[:-1], details[-1:]]
    detail_ship_times: dict[int, datetime] = {}
    final_ship_time = ship_time
    final_signed_time = signed_time
    for group_index, delivery_details in enumerate(delivery_groups, start=1):
        group_delay = timedelta(days=(group_index - 1) * rng.randint(1, 3))
        group_create_time = delivery_create_time + group_delay
        group_ship_time = ship_time + group_delay
        group_signed_time = signed_time + group_delay
        final_ship_time = max(final_ship_time, group_ship_time)
        final_signed_time = max(final_signed_time, group_signed_time)
        delivery_id = order_id * 10 + group_index
        _write_forward_delivery(
            refs,
            state,
            writer,
            cutoff,
            batch_id,
            delivery_id,
            order_id,
            user,
            shop,
            seller,
            warehouse,
            logistics,
            delivery_details,
            group_create_time,
            group_ship_time,
            group_signed_time,
            intent.region,
        )
        for detail in delivery_details:
            detail_ship_times[int(detail["order_detail_id"])] = group_ship_time
    _write_order_status(
        state,
        writer,
        cutoff,
        batch_id,
        order_id,
        3,
        user,
        shop,
        "PAID",
        "SHIPPED",
        "SHIP",
        final_ship_time,
        0,
    )
    _write_order_status(
        state,
        writer,
        cutoff,
        batch_id,
        order_id,
        4,
        user,
        shop,
        "SHIPPED",
        "COMPLETED",
        "SIGN",
        final_signed_time,
        1,
    )
    for detail in details:
        profile = detail["profile"]
        inventory_events.append(
            _inventory_event(
                profile,
                "SALE_SHIPMENT",
                "DELIVERY",
                order_id,
                -int(detail["quantity"]),
                -int(detail["quantity"]),
                profile.price_on(day).cost_price,
                detail_ship_times[int(detail["order_detail_id"])],
                60,
            )
        )

    should_refund = rng.random() < 0.085
    if should_refund:
        refund_detail = details[0]
        return_goods = rng.random() < 0.62
        return_event = _write_refund(
            refs,
            state,
            writer,
            cutoff,
            batch_id,
            order_id,
            user,
            shop,
            seller,
            payment,
            successful_payment_detail_id,
            intent.channel,
            warehouse,
            logistics,
            refund_detail,
            final_signed_time,
            return_goods,
            intent.region,
            rng,
        )
        if return_event is not None:
            inventory_events.append(return_event)
    elif rng.random() < 0.31:
        _write_comment(
            refs,
            state,
            writer,
            cutoff,
            batch_id,
            order_id,
            user,
            shop,
            details[0],
            final_signed_time,
            rng,
        )
    return inventory_events


def _choose_payment(
    refs: ReferenceData,
    channel: dict[str, Any],
    amount: Decimal,
    user: dict[str, Any],
    rng: random.Random,
) -> dict[str, Any]:
    channel_code = str(channel["channel_code"])
    base_weights = {
        "APP": {"ALIPAY": 38, "WECHAT": 38, "UNIONPAY": 8, "HUABEI": 15, "COD": 1},
        "MINI_PROGRAM": {"ALIPAY": 14, "WECHAT": 70, "UNIONPAY": 7, "HUABEI": 8, "COD": 1},
        "H5": {"ALIPAY": 42, "WECHAT": 38, "UNIONPAY": 9, "HUABEI": 10, "COD": 1},
        "PC": {"ALIPAY": 48, "WECHAT": 20, "UNIONPAY": 23, "HUABEI": 7, "COD": 2},
        "SEM": {"ALIPAY": 40, "WECHAT": 39, "UNIONPAY": 10, "HUABEI": 10, "COD": 1},
    }.get(channel_code, {"ALIPAY": 40, "WECHAT": 40, "UNIONPAY": 12, "HUABEI": 7, "COD": 1})
    birthday = user.get("birthday")
    age = 35
    if birthday is not None:
        age = max(18, user["register_time"].year - birthday.year)
    weights = []
    for payment in refs.payments:
        code = str(payment["payment_type_code"])
        weight = float(base_weights.get(code, 0))
        if code == "HUABEI":
            if amount >= Decimal("1000") and age <= 45:
                weight *= 1.8
            elif age >= 55:
                weight *= 0.25
        if code == "COD" and amount >= Decimal("500"):
            weight *= 0.2
        weights.append(weight)
    if not any(weights):
        raise ValueError("没有适用于当前订单的支付方式")
    return rng.choices(refs.payments, weights=weights, k=1)[0]


def _bounded_lognormal(
    rng: random.Random,
    mean: float,
    sigma: float,
    minimum: int,
    maximum: int,
) -> int:
    return max(minimum, min(maximum, round(rng.lognormvariate(mean, sigma))))


def _move_out_of_quiet_fulfillment_hours(
    moment: datetime,
    rng: random.Random,
) -> datetime:
    if moment.hour < 7:
        return moment.replace(
            hour=7,
            minute=rng.randrange(60),
            second=rng.randrange(60),
        )
    if moment.hour >= 22:
        next_day = moment + timedelta(days=1)
        return next_day.replace(
            hour=7,
            minute=rng.randrange(60),
            second=rng.randrange(60),
        )
    if not chinese_calendar.is_workday(moment.date()):
        return moment + timedelta(minutes=rng.randint(60, 360))
    return moment


def _delivery_transit_seconds(
    rng: random.Random,
    warehouse: dict[str, Any],
    region: dict[str, Any] | None,
    logistics: dict[str, Any],
) -> int:
    base = _bounded_lognormal(rng, 11.55, 0.48, 43_200, 604_800)
    warehouse_province = str(warehouse.get("province_code") or "")
    receiver_province = str(region.get("province_code") or "") if region else ""
    distance_factor = 0.72 if warehouse_province == receiver_province else 1.15
    remote_prefixes = {"15", "54", "62", "63", "64", "65"}
    if receiver_province[:2] in remote_prefixes:
        distance_factor *= 1.28
    company_id = int(logistics["logistics_company_id"])
    company_factor = 0.92 + company_id % 5 * 0.04
    return max(
        43_200,
        min(604_800, round(base * distance_factor * company_factor)),
    )


def _payment_failure_reason(
    payment: dict[str, Any],
    amount: Decimal,
    rng: random.Random,
    force_failure: bool = False,
) -> str | None:
    code = str(payment["payment_type_code"])
    failure_probability = Decimal("0.032")
    if amount >= Decimal("2000"):
        failure_probability += Decimal("0.025")
    if code in {"HUABEI", "COD"}:
        failure_probability += Decimal("0.018")
    if not force_failure and rng.random() >= float(failure_probability):
        return None
    reasons = ["USER_CANCELLED", "INSUFFICIENT_FUNDS", "CHANNEL_REJECTED", "TIMEOUT"]
    weights = [38, 29, 18, 15]
    if code == "HUABEI":
        weights = [20, 48, 20, 12]
    return rng.choices(reasons, weights=weights, k=1)[0]


def _write_payment_attempt(
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    pay_detail_id: int,
    attempt_no: int,
    order_id: int,
    user: dict[str, Any],
    shop: dict[str, Any],
    seller: dict[str, Any] | None,
    channel: dict[str, Any],
    payment: dict[str, Any],
    details: list[dict[str, Any]],
    requested_amount: Decimal,
    pay_time: datetime,
    result_time: datetime,
    failure_reason: str | None,
) -> None:
    installment_count = None
    if payment["payment_type_code"] == "HUABEI":
        installment_count = 3 if requested_amount < Decimal("3000") else 6
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_pay_detail_di",
        f"pay-detail:{pay_detail_id}",
        pay_time,
        {
            "pay_detail_id": pay_detail_id,
            "pay_order_no": f"P{pay_detail_id}",
            "pay_attempt_no": attempt_no,
            "pay_date_key": date_key(pay_time),
            "user_sk": version_at(
                refs.user_versions,
                int(user["user_id"]),
                pay_time,
            )["user_sk"],
            "user_id": user["user_id"],
            "payment_type_sk": payment["payment_type_sk"],
            "payment_type_code": payment["payment_type_code"],
            "channel_sk": channel["channel_sk"],
            "channel_code": channel["channel_code"],
            "pay_scene": "订单支付",
            "requested_pay_amount": requested_amount,
            "payment_fee_amount": MONEY_ZERO,
            "installment_count": installment_count,
            "currency_code": "CNY",
            "pay_request_time": pay_time,
            "biz_date": pay_time.date(),
        },
    )
    for line_index, detail in enumerate(details, start=1):
        detail_id = int(detail["order_detail_id"])
        allocation_id = pay_detail_id * 10 + line_index
        _schedule_fact(
            state,
            writer,
            cutoff,
            batch_id,
            "dwd_trade_pay_order_detail_di",
            f"pay-allocation:{allocation_id}",
            pay_time,
            {
                "pay_order_detail_id": allocation_id,
                "pay_detail_id": pay_detail_id,
                "order_id": order_id,
                "order_detail_id": detail_id,
                "shop_sk": shop["shop_sk"],
                "shop_id": shop["shop_id"],
                "seller_sk": seller["seller_sk"] if seller else UNKNOWN_SK,
                "seller_id": seller["seller_id"] if seller else None,
                "allocated_pay_amount": detail["receivable_amount"],
                "currency_code": "CNY",
                "pay_request_time": pay_time,
                "biz_date": pay_time.date(),
            },
        )
    _write_pay_status(
        state,
        writer,
        cutoff,
        batch_id,
        pay_detail_id,
        1,
        None,
        "REQUESTED",
        pay_time,
        None,
    )
    _write_pay_status(
        state,
        writer,
        cutoff,
        batch_id,
        pay_detail_id,
        2,
        "REQUESTED",
        "FAILED" if failure_reason else "SUCCESS",
        result_time,
        failure_reason,
    )


def _write_coupon_lifecycle(
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    order_id: int,
    user: dict[str, Any],
    coupon: dict[str, Any],
    details: list[dict[str, Any]],
    allocations: list[Decimal],
    order_time: datetime,
    resolution_time: datetime,
    paid_success: bool,
) -> None:
    receive_time = max(
        start_of_day(order_time.date()),
        order_time - timedelta(hours=2),
    )
    statuses = [
        (1, None, "RECEIVED", "领取", None, receive_time),
        (2, "RECEIVED", "LOCKED", "锁定", order_id, order_time),
        (
            3,
            "LOCKED",
            "USED" if paid_success else "RELEASED",
            "使用" if paid_success else "释放",
            order_id,
            resolution_time,
        ),
    ]
    for seq_no, before, after, event_type, related_order_id, event_time in statuses:
        event_id = order_id * 10 + seq_no
        _schedule_fact(
            state,
            writer,
            cutoff,
            batch_id,
            "dwd_marketing_user_coupon_event_di",
            f"coupon-event:{order_id}:{seq_no}",
            event_time,
            {
                "user_coupon_event_id": event_id,
                "user_coupon_id": order_id,
                "event_seq_no": seq_no,
                "event_date_key": date_key(event_time),
                "coupon_template_version_sk": coupon["coupon_template_version_sk"],
                "coupon_template_id": coupon["coupon_template_id"],
                "user_sk": user["user_sk"],
                "user_id": user["user_id"],
                "before_coupon_status": before,
                "after_coupon_status": after,
                "coupon_event_type": event_type,
                "related_order_id": related_order_id,
                "coupon_batch_no": f"CB{date_key(order_time)}",
                "event_time": event_time,
                "biz_date": event_time.date(),
            },
        )
    for detail, amount in zip(details, allocations, strict=True):
        if not amount:
            continue
        detail_id = int(detail["order_detail_id"])
        coupon_use_time = resolution_time if paid_success else order_time
        _schedule_fact(
            state,
            writer,
            cutoff,
            batch_id,
            "dwd_trade_order_detail_coupon_di",
            f"order-coupon:{detail_id}",
            coupon_use_time,
            {
                "order_detail_coupon_id": detail_id,
                "order_detail_id": detail_id,
                "order_id": order_id,
                "coupon_template_version_sk": coupon["coupon_template_version_sk"],
                "coupon_template_id": coupon["coupon_template_id"],
                "user_coupon_id": order_id,
                "user_sk": user["user_sk"],
                "user_id": user["user_id"],
                "coupon_discount_amount": amount,
                "coupon_batch_no": f"CB{date_key(order_time)}",
                "coupon_receive_time": receive_time,
                "coupon_use_time": coupon_use_time,
                "currency_code": "CNY",
                "order_create_time": order_time,
                "biz_date": coupon_use_time.date(),
            },
        )


def _write_forward_delivery(
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    delivery_id: int,
    order_id: int,
    user: dict[str, Any],
    shop: dict[str, Any],
    seller: dict[str, Any] | None,
    warehouse: dict[str, Any],
    logistics: dict[str, Any],
    details: list[dict[str, Any]],
    delivery_create_time: datetime,
    ship_time: datetime,
    signed_time: datetime,
    region: dict[str, Any] | None,
) -> None:
    weights = [_line_weight(detail) for detail in details]
    freight_total = sum(
        (detail["freight_amount"] for detail in details),
        MONEY_ZERO,
    )
    freight_allocations = _allocate_money(freight_total, weights)
    package_weight = sum(weights, Decimal("0.000"))
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_delivery_di",
        f"delivery:{delivery_id}",
        delivery_create_time,
        {
            "delivery_id": delivery_id,
            "delivery_no": f"D{delivery_id}",
            "package_no": f"PKG{delivery_id}",
            "delivery_direction": "正向",
            "delivery_date_key": date_key(delivery_create_time),
            "order_id": order_id,
            "refund_no": None,
            "user_sk": version_at(
                refs.user_versions,
                int(user["user_id"]),
                delivery_create_time,
            )["user_sk"],
            "user_id": user["user_id"],
            "shop_sk": shop["shop_sk"],
            "shop_id": shop["shop_id"],
            "seller_sk": seller["seller_sk"] if seller else UNKNOWN_SK,
            "seller_id": seller["seller_id"] if seller else None,
            "warehouse_sk": warehouse["warehouse_sk"],
            "warehouse_id": warehouse["warehouse_id"],
            "logistics_company_sk": logistics["logistics_company_sk"],
            "logistics_company_id": logistics["logistics_company_id"],
            "receiver_region_sk": region["region_sk"] if region else UNKNOWN_SK,
            "receiver_region_code": region["region_code"] if region else None,
            "tracking_no": f"TRK{delivery_id}",
            "delivery_type": "普通快递",
            "receiver_name": str(user["user_name"]),
            "receiver_phone": user.get("phone"),
            "receiver_address": "用户脱敏收货地址",
            "package_weight_kg": package_weight,
            "package_freight_amount": freight_total,
            "currency_code": "CNY",
            "delivery_create_time": delivery_create_time,
            "biz_date": delivery_create_time.date(),
        },
    )
    for index, (detail, weight, freight) in enumerate(
        zip(details, weights, freight_allocations, strict=True),
        start=1,
    ):
        delivery_item_id = delivery_id * 10 + index
        _schedule_fact(
            state,
            writer,
            cutoff,
            batch_id,
            "dwd_trade_delivery_item_di",
            f"delivery-item:{delivery_item_id}",
            delivery_create_time,
            {
                "delivery_item_id": delivery_item_id,
                "delivery_id": delivery_id,
                "order_id": order_id,
                "order_detail_id": detail["order_detail_id"],
                "refund_detail_id": None,
                "sku_sk": detail["sku_sk"],
                "sku_id": detail["sku_id"],
                "spu_sk": detail["spu_sk"],
                "spu_id": detail["spu_id"],
                "category_sk": detail["category_sk"],
                "category_id": detail["category_id"],
                "delivery_sku_qty": detail["sku_qty"],
                "allocated_weight_kg": weight,
                "allocated_freight_amount": freight,
                "currency_code": "CNY",
                "delivery_create_time": delivery_create_time,
                "biz_date": delivery_create_time.date(),
            },
        )
    statuses = (
        (1, None, "CREATED", "PACKAGE_CREATED", delivery_create_time),
        (2, "CREATED", "SHIPPED", "PACKAGE_SHIPPED", ship_time),
        (3, "SHIPPED", "SIGNED", "PACKAGE_SIGNED", signed_time),
    )
    for seq_no, before, after, code, event_time in statuses:
        _write_delivery_status(
            state,
            writer,
            cutoff,
            batch_id,
            delivery_id,
            seq_no,
            before,
            after,
            code,
            event_time,
            region,
        )


def _refund_reason(
    root_category: str,
    rng: random.Random,
) -> tuple[str, str, int]:
    common = [
        ("NOT_EXPECTED", "商品与预期不符", 0, 28),
        ("LOGISTICS_DAMAGE", "运输过程中商品受损", 0, 13),
        ("WRONG_ITEM", "收到的商品或规格不符", 0, 10),
        ("NO_LONGER_NEEDED", "购买后需求发生变化", 0, 18),
    ]
    category_specific = {
        "服饰鞋包": [("SIZE_MISMATCH", "尺码或版型不合适", 0, 34)],
        "手机数码": [("FUNCTION_ISSUE", "功能异常或无法正常使用", 1, 28)],
        "电脑办公": [("FUNCTION_ISSUE", "功能异常或无法正常使用", 1, 28)],
        "家用电器": [("QUALITY_ISSUE", "商品存在质量问题", 1, 30)],
        "食品饮料": [("PACKAGE_OR_EXPIRY", "包装或保质期不符合预期", 1, 26)],
        "美妆个护": [("ALLERGY_OR_DISCOMFORT", "使用后不适或不适合", 0, 22)],
    }.get(root_category, [("QUALITY_ISSUE", "商品存在质量问题", 1, 20)])
    candidates = common + category_specific
    selected = rng.choices(
        candidates,
        weights=[candidate[3] for candidate in candidates],
        k=1,
    )[0]
    return selected[0], selected[1], selected[2]


def _write_refund_payment_attempt(
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    refund_detail_id: int,
    attempt_no: int,
    refund_no: str,
    original_pay_detail_id: int,
    order_id: int,
    user: dict[str, Any],
    payment: dict[str, Any],
    channel: dict[str, Any],
    detail: dict[str, Any],
    refund_amount: Decimal,
    request_time: datetime,
    success: bool,
) -> None:
    refund_pay_detail_id = refund_detail_id * 10 + attempt_no
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_refund_pay_detail_di",
        f"refund-pay:{refund_pay_detail_id}",
        request_time,
        {
            "refund_pay_detail_id": refund_pay_detail_id,
            "refund_no": refund_no,
            "refund_detail_id": refund_detail_id,
            "refund_pay_attempt_no": attempt_no,
            "original_pay_detail_id": original_pay_detail_id,
            "order_id": order_id,
            "order_detail_id": detail["order_detail_id"],
            "request_date_key": date_key(request_time),
            "user_sk": version_at(
                refs.user_versions,
                int(user["user_id"]),
                request_time,
            )["user_sk"],
            "user_id": user["user_id"],
            "payment_type_sk": payment["payment_type_sk"],
            "payment_type_code": payment["payment_type_code"],
            "channel_sk": channel["channel_sk"],
            "channel_code": channel["channel_code"],
            "refund_goods_amount": refund_amount,
            "refund_freight_amount": MONEY_ZERO,
            "refund_tax_amount": MONEY_ZERO,
            "refund_amount": refund_amount,
            "refund_account_type": "原路退回",
            "currency_code": "CNY",
            "refund_pay_request_time": request_time,
            "biz_date": request_time.date(),
        },
    )
    result_time = request_time + timedelta(seconds=5 + attempt_no * 4)
    for seq_no, before, after, event_time in (
        (1, None, "REQUESTED", request_time),
        (2, "REQUESTED", "SUCCESS" if success else "FAILED", result_time),
    ):
        event_id = refund_pay_detail_id * 10 + seq_no
        _schedule_fact(
            state,
            writer,
            cutoff,
            batch_id,
            "dwd_trade_refund_pay_status_event_di",
            f"refund-pay-status:{refund_pay_detail_id}:{seq_no}",
            event_time,
            {
                "refund_pay_status_event_id": event_id,
                "refund_pay_detail_id": refund_pay_detail_id,
                "event_seq_no": seq_no,
                "event_date_key": date_key(event_time),
                "third_party_refund_no": (
                    f"TR{refund_pay_detail_id}" if after == "SUCCESS" else None
                ),
                "before_refund_pay_status": before,
                "after_refund_pay_status": after,
                "status_reason_code": (
                    "CHANNEL_TIMEOUT" if after == "FAILED" else None
                ),
                "status_reason_description": (
                    "退款渠道响应超时" if after == "FAILED" else None
                ),
                "event_time": event_time,
                "biz_date": event_time.date(),
            },
        )


def _write_refund(
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    order_id: int,
    user: dict[str, Any],
    shop: dict[str, Any],
    seller: dict[str, Any] | None,
    payment: dict[str, Any],
    original_pay_detail_id: int | None,
    channel: dict[str, Any],
    warehouse: dict[str, Any],
    logistics: dict[str, Any],
    detail: dict[str, Any],
    signed_time: datetime,
    return_goods: bool,
    region: dict[str, Any] | None,
    rng: random.Random,
) -> dict[str, Any] | None:
    if original_pay_detail_id is None:
        raise ValueError("成功订单缺少原支付尝试")
    state.user_refund_counts[int(user["user_id"])] = (
        state.user_refund_counts.get(int(user["user_id"]), 0) + 1
    )
    refund_detail_id = int(detail["order_detail_id"])
    refund_no = f"R{refund_detail_id}"
    apply_time = signed_time + timedelta(days=1 + rng.randrange(6))
    root_category = str(detail["profile"].category["root_category_name"])
    reason_code, reason_description, is_quality_issue = _refund_reason(
        root_category,
        rng,
    )
    refund_quantity = int(detail["sku_qty"])
    if refund_quantity > 1 and rng.random() < 0.36:
        refund_quantity = 1
    refund_amount = money(
        Decimal(detail["receivable_amount"])
        * Decimal(refund_quantity)
        / Decimal(detail["sku_qty"])
    )
    if not return_goods and rng.random() < 0.18:
        refund_amount = money(refund_amount * Decimal(str(rng.uniform(0.35, 0.8))))
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_refund_detail_di",
        f"refund-detail:{refund_detail_id}",
        apply_time,
        {
            "refund_detail_id": refund_detail_id,
            "refund_no": refund_no,
            "order_id": order_id,
            "order_detail_id": detail["order_detail_id"],
            "apply_date_key": date_key(apply_time),
            "user_sk": version_at(
                refs.user_versions,
                int(user["user_id"]),
                apply_time,
            )["user_sk"],
            "user_id": user["user_id"],
            "shop_sk": shop["shop_sk"],
            "shop_id": shop["shop_id"],
            "seller_sk": seller["seller_sk"] if seller else UNKNOWN_SK,
            "seller_id": seller["seller_id"] if seller else None,
            "sku_sk": detail["sku_sk"],
            "sku_id": detail["sku_id"],
            "refund_sku_qty": refund_quantity,
            "refund_type": "退货退款" if return_goods else "仅退款",
            "refund_reason_code": reason_code,
            "refund_reason_description": reason_description,
            "is_quality_issue": is_quality_issue,
            "need_return_goods": int(return_goods),
            "apply_goods_amount": refund_amount,
            "apply_freight_amount": MONEY_ZERO,
            "apply_tax_amount": MONEY_ZERO,
            "refund_apply_amount": refund_amount,
            "currency_code": "CNY",
            "apply_time": apply_time,
            "biz_date": apply_time.date(),
        },
    )
    decision_time = apply_time + timedelta(hours=rng.randint(1, 72))
    decision = rng.choices(
        ["APPROVED", "REJECTED", "CANCELLED"],
        [89, 6, 5],
        k=1,
    )[0]
    for seq_no, before, after, delta, event_time in [
        (1, None, "APPLIED", None, apply_time),
        (
            2,
            "APPLIED",
            decision,
            refund_amount if decision == "APPROVED" else None,
            decision_time,
        ),
    ]:
        event_id = refund_detail_id * 10 + seq_no
        _schedule_fact(
            state,
            writer,
            cutoff,
            batch_id,
            "dwd_trade_refund_status_event_di",
            f"refund-status:{refund_detail_id}:{seq_no}",
            event_time,
            {
                "refund_status_event_id": event_id,
                "refund_detail_id": refund_detail_id,
                "refund_no": refund_no,
                "event_seq_no": seq_no,
                "event_date_key": date_key(event_time),
                "before_refund_status": before,
                "after_refund_status": after,
                "approved_amount_delta": delta,
                "status_reason_code": (
                    "EVIDENCE_INSUFFICIENT"
                    if after == "REJECTED"
                    else "USER_WITHDREW"
                    if after == "CANCELLED"
                    else None
                ),
                "status_reason_description": (
                    "售后凭证不足"
                    if after == "REJECTED"
                    else "用户撤销售后申请"
                    if after == "CANCELLED"
                    else None
                ),
                "operator_id": "SYSTEM",
                "operator_type": "SYSTEM",
                "event_time": event_time,
                "biz_date": event_time.date(),
            },
        )
    if decision != "APPROVED":
        return None
    approved_time = decision_time
    refund_pay_time = approved_time + timedelta(minutes=rng.randint(5, 60))
    first_pay_success = rng.random() >= 0.045
    _write_refund_payment_attempt(
        refs,
        state,
        writer,
        cutoff,
        batch_id,
        refund_detail_id,
        1,
        refund_no,
        original_pay_detail_id,
        order_id,
        user,
        payment,
        channel,
        detail,
        refund_amount,
        refund_pay_time,
        first_pay_success,
    )
    if not first_pay_success and rng.random() < 0.74:
        _write_refund_payment_attempt(
            refs,
            state,
            writer,
            cutoff,
            batch_id,
            refund_detail_id,
            2,
            refund_no,
            original_pay_detail_id,
            order_id,
            user,
            payment,
            channel,
            detail,
            refund_amount,
            refund_pay_time + timedelta(hours=rng.randint(1, 12)),
            True,
        )
    if not return_goods:
        return None
    reverse_delivery_id = order_id + 500_000
    reverse_time = approved_time + timedelta(days=2)
    weight = _line_weight(detail)
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_delivery_di",
        f"delivery:{reverse_delivery_id}",
        reverse_time,
        {
            "delivery_id": reverse_delivery_id,
            "delivery_no": f"D{reverse_delivery_id}",
            "package_no": f"PKG{reverse_delivery_id}",
            "delivery_direction": "逆向",
            "delivery_date_key": date_key(reverse_time),
            "order_id": order_id,
            "refund_no": refund_no,
            "user_sk": version_at(
                refs.user_versions,
                int(user["user_id"]),
                reverse_time,
            )["user_sk"],
            "user_id": user["user_id"],
            "shop_sk": shop["shop_sk"],
            "shop_id": shop["shop_id"],
            "seller_sk": seller["seller_sk"] if seller else UNKNOWN_SK,
            "seller_id": seller["seller_id"] if seller else None,
            "warehouse_sk": warehouse["warehouse_sk"],
            "warehouse_id": warehouse["warehouse_id"],
            "logistics_company_sk": logistics["logistics_company_sk"],
            "logistics_company_id": logistics["logistics_company_id"],
            "receiver_region_sk": UNKNOWN_SK,
            "receiver_region_code": warehouse.get("district_code"),
            "tracking_no": f"TRK{reverse_delivery_id}",
            "delivery_type": "退货快递",
            "receiver_name": warehouse["warehouse_name"],
            "receiver_phone": None,
            "receiver_address": warehouse["address"],
            "package_weight_kg": weight,
            "package_freight_amount": MONEY_ZERO,
            "currency_code": "CNY",
            "delivery_create_time": reverse_time,
            "biz_date": reverse_time.date(),
        },
    )
    reverse_item_id = reverse_delivery_id * 10 + 1
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_delivery_item_di",
        f"delivery-item:{reverse_item_id}",
        reverse_time,
        {
            "delivery_item_id": reverse_item_id,
            "delivery_id": reverse_delivery_id,
            "order_id": order_id,
            "order_detail_id": detail["order_detail_id"],
            "refund_detail_id": refund_detail_id,
            "sku_sk": detail["sku_sk"],
            "sku_id": detail["sku_id"],
            "spu_sk": detail["spu_sk"],
            "spu_id": detail["spu_id"],
            "category_sk": detail["category_sk"],
            "category_id": detail["category_id"],
            "delivery_sku_qty": refund_quantity,
            "allocated_weight_kg": weight,
            "allocated_freight_amount": MONEY_ZERO,
            "currency_code": "CNY",
            "delivery_create_time": reverse_time,
            "biz_date": reverse_time.date(),
        },
    )
    _write_delivery_status(
        state,
        writer,
        cutoff,
        batch_id,
        reverse_delivery_id,
        1,
        None,
        "CREATED",
        "RETURN_CREATED",
        reverse_time,
        region,
    )
    reverse_ship_time = reverse_time + timedelta(hours=4)
    reverse_signed_time = reverse_time + timedelta(days=2)
    _write_delivery_status(
        state,
        writer,
        cutoff,
        batch_id,
        reverse_delivery_id,
        2,
        "CREATED",
        "SHIPPED",
        "RETURN_SHIPPED",
        reverse_ship_time,
        region,
    )
    _write_delivery_status(
        state,
        writer,
        cutoff,
        batch_id,
        reverse_delivery_id,
        3,
        "SHIPPED",
        "SIGNED",
        "RETURN_RECEIVED",
        reverse_signed_time,
        None,
    )
    return _inventory_event(
        detail["profile"],
        "RETURN_RECEIPT",
        "REFUND",
        refund_detail_id,
        refund_quantity,
        0,
        detail["profile"].price_on(signed_time.date()).cost_price,
        reverse_signed_time,
        70,
    )


def _write_comment(
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    order_id: int,
    user: dict[str, Any],
    shop: dict[str, Any],
    detail: dict[str, Any],
    signed_time: datetime,
    rng: random.Random,
) -> None:
    publish_time = signed_time + timedelta(hours=6 + rng.randrange(72))
    comment_id = int(detail["order_detail_id"])
    level = rng.choices([1, 2, 3, 4, 5], weights=[3, 5, 13, 31, 48], k=1)[0]
    root_category = str(detail["profile"].category["root_category_name"])
    content = _comment_content(
        root_category,
        str(detail["profile"].category["category_name"]),
        level,
        rng,
    )
    comment_detail_id = comment_id * 10 + 1
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_service_comment_detail_di",
        f"comment:{comment_detail_id}",
        publish_time,
        {
            "comment_detail_id": comment_detail_id,
            "comment_id": comment_id,
            "parent_comment_detail_id": None,
            "comment_type": "初评",
            "comment_date_key": date_key(publish_time),
            "order_id": order_id,
            "order_detail_id": detail["order_detail_id"],
            "user_sk": version_at(
                refs.user_versions,
                int(user["user_id"]),
                publish_time,
            )["user_sk"],
            "user_id": user["user_id"],
            "shop_sk": shop["shop_sk"],
            "shop_id": shop["shop_id"],
            "sku_sk": detail["sku_sk"],
            "sku_id": detail["sku_id"],
            "spu_sk": detail["spu_sk"],
            "spu_id": detail["spu_id"],
            "category_sk": detail["category_sk"],
            "category_id": detail["category_id"],
            "comment_level": level,
            "is_anonymous": int(rng.random() < 0.65),
            "image_count": rng.choices([0, 1, 2, 3], weights=[72, 18, 7, 3], k=1)[0],
            "video_count": int(rng.random() < 0.03),
            "comment_content": content,
            "service_score": level,
            "logistics_score": min(5, level + 1),
            "description_score": level,
            "sensitive_tag": None,
            "sentiment": "正向" if level >= 4 else "中性" if level == 3 else "负向",
            "comment_time": publish_time,
            "biz_date": publish_time.date(),
        },
    )
    if rng.random() >= 0.12:
        return
    follow_time = publish_time + timedelta(days=rng.randint(3, 45))
    follow_detail_id = comment_id * 10 + 2
    follow_content = rng.choice(
        [
            "使用一段时间后表现稳定，补充评价供参考",
            "后续使用体验与初次评价基本一致",
            "用了一段时间发现细节还有改进空间",
            "售后已经联系处理，补充记录处理结果",
            None,
        ]
    )
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_service_comment_detail_di",
        f"comment:{follow_detail_id}",
        follow_time,
        {
            "comment_detail_id": follow_detail_id,
            "comment_id": comment_id,
            "parent_comment_detail_id": comment_detail_id,
            "comment_type": "追评",
            "comment_date_key": date_key(follow_time),
            "order_id": order_id,
            "order_detail_id": detail["order_detail_id"],
            "user_sk": version_at(
                refs.user_versions,
                int(user["user_id"]),
                follow_time,
            )["user_sk"],
            "user_id": user["user_id"],
            "shop_sk": shop["shop_sk"],
            "shop_id": shop["shop_id"],
            "sku_sk": detail["sku_sk"],
            "sku_id": detail["sku_id"],
            "spu_sk": detail["spu_sk"],
            "spu_id": detail["spu_id"],
            "category_sk": detail["category_sk"],
            "category_id": detail["category_id"],
            "comment_level": None,
            "is_anonymous": int(rng.random() < 0.65),
            "image_count": rng.choices([0, 1, 2], weights=[82, 14, 4], k=1)[0],
            "video_count": int(rng.random() < 0.02),
            "comment_content": follow_content,
            "service_score": None,
            "logistics_score": None,
            "description_score": None,
            "sensitive_tag": None,
            "sentiment": None,
            "comment_time": follow_time,
            "biz_date": follow_time.date(),
        },
    )


def _comment_content(
    root_category: str,
    leaf_category: str,
    level: int,
    rng: random.Random,
) -> str | None:
    if rng.random() < 0.14:
        return None
    positive_attributes = {
        "手机数码": ["运行流畅", "续航符合预期", "外观质感不错"],
        "家用电器": ["操作方便", "运行声音可以接受", "功能比较实用"],
        "服饰鞋包": ["尺码合适", "面料舒适", "颜色与图片接近"],
        "食品饮料": ["日期新鲜", "包装完整", "口味符合预期"],
        "家居家装": ["做工扎实", "安装比较顺利", "尺寸合适"],
    }
    attribute = rng.choice(
        positive_attributes.get(
            root_category,
            ["包装完整", "与页面描述基本一致", "日常使用方便"],
        )
    )
    templates = {
        5: [
            f"{attribute}，配送也很及时",
            f"收到后检查没有问题，{attribute}",
            f"整体满意，{attribute}，会继续使用",
        ],
        4: [
            f"整体表现不错，{attribute}",
            f"基本符合预期，{attribute}，细节还能提升",
            "使用体验可以，性价比尚可",
        ],
        3: [
            "商品能正常使用，整体表现中规中矩",
            "与预期有一点差距，但基本功能没有问题",
            "包装和配送正常，商品体验一般",
        ],
        2: [
            "实际体验不太理想，细节和页面描述有差距",
            "包装有些破损，商品使用体验一般",
            "等待时间较长，收到后发现做工不够细致",
        ],
        1: [
            "商品存在明显问题，已经申请售后处理",
            "收到的规格不符，影响正常使用",
            "包装破损且商品有瑕疵，体验较差",
        ],
    }
    content = rng.choice(templates[level])
    if rng.random() < 0.56:
        content += rng.choice(
            [
                f"，这款{leaf_category}的细节可以参考",
                "，客服回复速度正常",
                "，外包装保护得比较到位",
                "，价格波动不大",
                "，实际感受因人而异",
                "，配送进度可以正常查询",
            ]
        )
    if rng.random() < 0.38:
        content += f"，使用{rng.randint(2, 45)}天后评价"
    return content


def _apply_inventory_events(
    refs: ReferenceData,
    state: BusinessState,
    writer: TableWriter,
    events: list[dict[str, Any]],
    batch_id: str,
) -> None:
    events.sort(key=_inventory_event_sort_key)
    for event in events:
        sku_id = int(event["sku_id"])
        profile = refs.profile_by_sku[sku_id]
        position = state.inventory[sku_id]
        before_on_hand = position.on_hand
        before_reserved = position.reserved
        before_in_transit = position.in_transit
        after_on_hand = before_on_hand + int(event["on_hand_delta"])
        after_reserved = before_reserved + int(event["reserved_delta"])
        after_in_transit = before_in_transit + int(event["in_transit_delta"])
        if (
            after_on_hand < 0
            or not 0 <= after_reserved <= after_on_hand
            or after_in_transit < 0
        ):
            raise ValueError(
                f"库存状态非法 sku_id={sku_id} event={event} "
                f"before=({before_on_hand},{before_reserved})"
            )
        position.on_hand = after_on_hand
        position.reserved = after_reserved
        position.in_transit = after_in_transit
        position.unit_cost = price(event["unit_cost"])
        event_day = event["event_time"].date()
        sequence = state.inventory_event_sequences.get(event_day, 0) + 1
        state.inventory_event_sequences[event_day] = sequence
        event_id = date_key(event_day) * 1_000_000 + sequence
        warehouse = refs.warehouse_for_profile(profile)
        row = {
            "inventory_change_id": event_id,
            "change_no": f"IC{event_id}",
            "event_date_key": date_key(event_day),
            "warehouse_sk": warehouse["warehouse_sk"],
            "warehouse_id": warehouse["warehouse_id"],
            "sku_sk": profile.sku["sku_sk"],
            "sku_id": sku_id,
            "spu_sk": profile.spu["spu_sk"],
            "spu_id": profile.spu["spu_id"],
            "shop_sk": profile.shop["shop_sk"],
            "shop_id": profile.shop["shop_id"],
            "change_type": event["change_type"],
            "biz_type": event["biz_type"],
            "biz_id": str(event["biz_id"]),
            "before_on_hand_qty": before_on_hand,
            "on_hand_qty_delta": event["on_hand_delta"],
            "after_on_hand_qty": after_on_hand,
            "before_reserved_qty": before_reserved,
            "reserved_qty_delta": event["reserved_delta"],
            "after_reserved_qty": after_reserved,
            "before_in_transit_qty": before_in_transit,
            "in_transit_qty_delta": event["in_transit_delta"],
            "after_in_transit_qty": after_in_transit,
            "unit_cost": event["unit_cost"],
            "total_cost_delta": price(
                Decimal(event["on_hand_delta"]) * event["unit_cost"]
            ),
            "currency_code": "CNY",
            "operator_id": "SYSTEM",
            "operator_type": "SYSTEM",
            "remark": event["change_type"],
            "event_time": event["event_time"],
            "biz_date": event_day,
        } | fact_audit(f"inventory-change:{event_id}", batch_id)
        writer.add("dwd_inventory_change_di", row)


def _inventory_event(
    profile: ProductProfile,
    change_type: str,
    biz_type: str,
    biz_id: int | str,
    on_hand_delta: int,
    reserved_delta: int,
    unit_cost: Decimal,
    event_time: datetime,
    priority: int,
    in_transit_delta: int = 0,
) -> dict[str, Any]:
    return {
        "sku_id": int(profile.sku["sku_id"]),
        "change_type": change_type,
        "biz_type": biz_type,
        "biz_id": str(biz_id),
        "on_hand_delta": on_hand_delta,
        "reserved_delta": reserved_delta,
        "in_transit_delta": in_transit_delta,
        "unit_cost": price(unit_cost),
        "event_time": event_time,
        "priority": priority,
    }


def _write_order_status(
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    order_id: int,
    seq_no: int,
    user: dict[str, Any],
    shop: dict[str, Any],
    before: str | None,
    after: str,
    event_type: str,
    event_time: datetime,
    terminal: int,
    reason_code: str | None = None,
    reason_description: str | None = None,
) -> None:
    event_id = order_id * 10 + seq_no
    system_cancel = after == "CANCELLED" and reason_code == "OUT_OF_STOCK"
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_order_status_event_di",
        f"order-status:{order_id}:{seq_no}",
        event_time,
        {
            "order_status_event_id": event_id,
            "order_id": order_id,
            "event_seq_no": seq_no,
            "event_date_key": date_key(event_time),
            "user_sk": user["user_sk"],
            "user_id": user["user_id"],
            "shop_sk": shop["shop_sk"],
            "shop_id": shop["shop_id"],
            "before_order_status": before,
            "after_order_status": after,
            "status_event_type": event_type,
            "status_reason_code": reason_code,
            "status_reason_description": reason_description,
            "cancel_stage": (
                "库存分配"
                if system_cancel
                else "待支付"
                if after == "CANCELLED"
                else None
            ),
            "is_terminal_status": terminal,
            "operator_id": "SYSTEM" if system_cancel else str(user["user_id"]),
            "operator_type": (
                "SYSTEM" if system_cancel or after != "CANCELLED" else "USER"
            ),
            "event_time": event_time,
            "biz_date": event_time.date(),
        },
    )


def _write_pay_status(
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    pay_detail_id: int,
    seq_no: int,
    before: str | None,
    after: str,
    event_time: datetime,
    failure_reason: str | None,
) -> None:
    event_id = pay_detail_id * 10 + seq_no
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_pay_status_event_di",
        f"pay-status:{pay_detail_id}:{seq_no}",
        event_time,
        {
            "pay_status_event_id": event_id,
            "pay_detail_id": pay_detail_id,
            "pay_order_no": f"P{pay_detail_id}",
            "event_seq_no": seq_no,
            "event_date_key": date_key(event_time),
            "third_party_pay_no": f"TP{pay_detail_id}" if after == "SUCCESS" else None,
            "before_pay_status": before,
            "after_pay_status": after,
            "status_reason_code": failure_reason if after == "FAILED" else None,
            "status_reason_description": (
                {
                    "USER_CANCELLED": "用户取消支付",
                    "INSUFFICIENT_FUNDS": "账户余额或授信不足",
                    "CHANNEL_REJECTED": "支付渠道拒绝",
                    "TIMEOUT": "支付渠道超时",
                }.get(str(failure_reason))
                if after == "FAILED"
                else None
            ),
            "event_time": event_time,
            "biz_date": event_time.date(),
        },
    )


def _write_delivery_status(
    state: BusinessState,
    writer: TableWriter,
    cutoff: datetime,
    batch_id: str,
    delivery_id: int,
    seq_no: int,
    before: str | None,
    after: str,
    code: str,
    event_time: datetime,
    region: dict[str, Any] | None,
) -> None:
    event_id = delivery_id * 10 + seq_no
    _schedule_fact(
        state,
        writer,
        cutoff,
        batch_id,
        "dwd_trade_delivery_status_event_di",
        f"delivery-status:{delivery_id}:{seq_no}",
        event_time,
        {
            "delivery_status_event_id": event_id,
            "delivery_id": delivery_id,
            "event_seq_no": seq_no,
            "event_date_key": date_key(event_time),
            "before_delivery_status": before,
            "after_delivery_status": after,
            "status_event_code": code,
            "event_region_sk": region["region_sk"] if region else UNKNOWN_SK,
            "event_region_code": region["region_code"] if region else None,
            "event_location": "区域物流节点",
            "event_remark": None,
            "event_time": event_time,
            "biz_date": event_time.date(),
        },
    )


def _eligible_promotion(
    refs: ReferenceData,
    profile: ProductProfile,
    moment: datetime,
    amount: Decimal,
) -> dict[str, Any] | None:
    return next(
        (
            row
            for row in refs.active_promotions(moment)
            if refs.promotion_applies(int(row["promotion_id"]), profile)
            and (
                row.get("threshold_amount") is None
                or amount >= row["threshold_amount"]
            )
        ),
        None,
    )


def _eligible_coupon(
    refs: ReferenceData,
    profile: ProductProfile,
    moment: datetime,
    amount: Decimal,
    rng: random.Random,
) -> dict[str, Any] | None:
    if rng.random() >= 0.46:
        return None
    return next(
        (
            row
            for row in refs.active_coupons(moment)
            if refs.coupon_applies(int(row["coupon_template_id"]), profile)
            and (
                row.get("threshold_amount") is None
                or amount >= row["threshold_amount"]
            )
        ),
        None,
    )


def _rule_discount(rule: dict[str, Any] | None, amount: Decimal) -> Decimal:
    if rule is None:
        return MONEY_ZERO
    if rule.get("discount_amount") is not None:
        return money(min(amount, rule["discount_amount"]))
    if rule.get("discount_rate") is not None:
        discount = amount * (Decimal("1") - rule["discount_rate"])
        maximum = rule.get("max_discount_amount")
        return money(min(discount, maximum) if maximum is not None else discount)
    return MONEY_ZERO


def _allocate_money(total: Decimal, weights: list[Decimal]) -> list[Decimal]:
    if not weights:
        return []
    weight_sum = sum(weights, Decimal("0"))
    if weight_sum == 0:
        result = [MONEY_ZERO] * len(weights)
        result[0] = money(total)
        return result
    allocations: list[Decimal] = []
    allocated = MONEY_ZERO
    for index, weight in enumerate(weights):
        value = (
            money(total - allocated)
            if index == len(weights) - 1
            else money(total * weight / weight_sum)
        )
        allocations.append(value)
        allocated += value
    return allocations


def _line_weight(detail: dict[str, Any]) -> Decimal:
    profile: ProductProfile = detail["profile"]
    source_weight = profile.spu.get("weight_kg")
    if source_weight is None:
        return Decimal("0.000")
    return (Decimal(str(source_weight)) * int(detail["sku_qty"])).quantize(
        WEIGHT_QUANT,
        rounding=ROUND_HALF_UP,
    )
