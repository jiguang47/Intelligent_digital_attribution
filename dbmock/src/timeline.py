"""按月编排所需的时间计划和跨月业务状态"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from .settings import GenerateConfig
from .support import iter_dates


@dataclass(frozen=True, slots=True)
class MonthPeriod:
    index: int
    key: str
    start_date: date
    end_date: date


@dataclass(frozen=True, slots=True)
class PeriodTargets:
    page_views: int
    searches: int
    order_details: int


@dataclass(frozen=True, slots=True)
class ConversionIntent:
    order_time: datetime
    session_start_time: datetime
    session_end_time: datetime
    session_id: str
    device_id: str
    user: dict[str, Any]
    channel: dict[str, Any]
    region: dict[str, Any] | None
    primary_sku_id: int
    cart_quantity: int
    line_count: int


@dataclass(slots=True)
class SessionJourney:
    session_id: str
    session_start_time: datetime
    last_event_time: datetime

    def observe(self, event_time: datetime) -> None:
        if event_time < self.session_start_time:
            raise ValueError(
                f"会话事件早于会话开始 session_id={self.session_id} "
                f"event_time={event_time}"
            )
        self.last_event_time = max(self.last_event_time, event_time)

    def close(self, cutoff: datetime, tail_seconds: int) -> datetime:
        if tail_seconds < 0:
            raise ValueError("会话尾部停留时间不能为负数")
        return min(
            cutoff,
            max(
                self.session_start_time,
                self.last_event_time + timedelta(seconds=tail_seconds),
            ),
        )


@dataclass(slots=True)
class CartPosition:
    quantity: int
    updated_at: datetime


@dataclass(slots=True)
class InventoryPosition:
    on_hand: int
    reserved: int
    in_transit: int
    unit_cost: Decimal

    @property
    def available(self) -> int:
        return self.on_hand - self.reserved


@dataclass(frozen=True, slots=True)
class ScheduledFact:
    table_name: str
    source_record_id: str
    event_time: datetime
    row: dict[str, Any]


@dataclass(slots=True)
class BusinessState:
    inventory: dict[int, InventoryPosition] = field(default_factory=dict)
    user_order_counts: dict[int, int] = field(default_factory=dict)
    user_session_counts: dict[int, int] = field(default_factory=dict)
    user_spend_amounts: dict[int, Decimal] = field(default_factory=dict)
    user_refund_counts: dict[int, int] = field(default_factory=dict)
    user_category_counts: dict[int, dict[str, int]] = field(default_factory=dict)
    user_last_active_at: dict[int, datetime] = field(default_factory=dict)
    user_activity_times: dict[int, deque[datetime]] = field(default_factory=dict)
    user_points_balances: dict[int, int] = field(default_factory=dict)
    cart_positions: dict[tuple[int, int], CartPosition] = field(default_factory=dict)
    favorite_skus_by_user: dict[int, set[int]] = field(default_factory=dict)
    inventory_event_sequences: dict[date, int] = field(default_factory=dict)
    pending_inventory_events: list[dict[str, Any]] = field(default_factory=list)
    pending_facts: list[ScheduledFact] = field(default_factory=list)

    def record_activity(self, user_id: int, event_time: datetime) -> None:
        history = self.user_activity_times.setdefault(user_id, deque())
        history.append(event_time)
        cutoff = event_time - timedelta(days=180)
        while history and history[0] < cutoff:
            history.popleft()
        self.user_last_active_at[user_id] = event_time

    def activity_count(self, user_id: int, since: datetime) -> int:
        return sum(
            event_time >= since
            for event_time in self.user_activity_times.get(user_id, ())
        )


def month_periods(start_date: date, end_date: date) -> list[MonthPeriod]:
    periods: list[MonthPeriod] = []
    current = start_date
    while current <= end_date:
        next_month = (
            current.replace(year=current.year + 1, month=1, day=1)
            if current.month == 12
            else current.replace(month=current.month + 1, day=1)
        )
        period_end = min(end_date, next_month - timedelta(days=1))
        periods.append(
            MonthPeriod(
                index=len(periods),
                key=f"{current:%Y-%m}",
                start_date=current,
                end_date=period_end,
            )
        )
        current = next_month
    return periods


def build_period_targets(
    config: GenerateConfig,
    periods: list[MonthPeriod],
    cutoff: datetime | None = None,
) -> dict[str, PeriodTargets]:
    page_weights = [
        sum(
            _day_weight(day, config.start_date, "traffic", cutoff)
            for day in iter_dates(period.start_date, period.end_date)
        )
        for period in periods
    ]
    search_weights = [
        sum(
            _day_weight(day, config.start_date, "search", cutoff)
            for day in iter_dates(period.start_date, period.end_date)
        )
        for period in periods
    ]
    order_weights = [
        sum(
            _day_weight(day, config.start_date, "order", cutoff)
            for day in iter_dates(period.start_date, period.end_date)
        )
        for period in periods
    ]
    page_views = _allocate(config.page_view_count, page_weights)
    searches = _allocate(config.search_count, search_weights)
    order_details = _allocate(config.order_detail_count, order_weights)
    return {
        period.key: PeriodTargets(
            page_views=page_views[index],
            searches=searches[index],
            order_details=order_details[index],
        )
        for index, period in enumerate(periods)
    }


def day_targets(
    total: int,
    period: MonthPeriod,
    start_date: date,
    target_kind: str,
    cutoff: datetime | None = None,
) -> dict[date, int]:
    days = list(iter_dates(period.start_date, period.end_date))
    values = _allocate(
        total,
        [_day_weight(day, start_date, target_kind, cutoff) for day in days],
    )
    return dict(zip(days, values, strict=True))


def _day_weight(
    day: date,
    start_date: date,
    target_kind: str,
    cutoff: datetime | None = None,
) -> float:
    elapsed_years = (day - start_date).days / 365.25
    growth = 1.0 + elapsed_years * 0.12
    weekend = day.weekday() >= 5
    weekday = 1.12 if weekend else 1.0
    campaign = 1.0
    if day.month == 6 and 15 <= day.day <= 20:
        campaign = 2.2
    elif day.month == 11 and 8 <= day.day <= 12:
        campaign = 3.0
    elif day.month == 12 and 10 <= day.day <= 13:
        campaign = 1.8
    elif day.month in {1, 2} and day.day <= 7:
        campaign = 1.25
    behavior_factor = 1.0
    if target_kind == "search":
        behavior_factor = (0.98 if weekend else 1.04) * campaign**0.08
    elif target_kind == "order":
        payday = 1.08 if day.day <= 5 or 15 <= day.day <= 20 else 1.0
        campaign_conversion = 1.0
        if day.month == 6 and 15 <= day.day <= 20:
            campaign_conversion = 1.32
        elif day.month == 11 and 8 <= day.day <= 12:
            campaign_conversion = 1.48
        elif day.month == 12 and 10 <= day.day <= 13:
            campaign_conversion = 1.24
        deterministic_noise = 0.92 + ((day.toordinal() * 37) % 17) / 100
        behavior_factor = (
            (1.16 if weekend else 0.98)
            * payday
            * campaign_conversion
            * deterministic_noise
        )
    elif target_kind != "traffic":
        raise ValueError(f"不支持的日目标类型: {target_kind}")
    observed_fraction = 1.0
    if cutoff is not None and day == cutoff.date():
        elapsed_seconds = (
            cutoff - datetime.combine(day, datetime.min.time())
        ).total_seconds()
        observed_fraction = max(0.02, min(1.0, elapsed_seconds / 86400))
    return growth * weekday * campaign * behavior_factor * observed_fraction


def _allocate(total: int, weights: list[float]) -> list[int]:
    if not weights:
        return []
    weight_sum = sum(weights)
    exact = [total * weight / weight_sum for weight in weights]
    allocated = [int(value) for value in exact]
    remainder = total - sum(allocated)
    order = sorted(
        range(len(weights)),
        key=lambda index: exact[index] - allocated[index],
        reverse=True,
    )
    for index in order[:remainder]:
        allocated[index] += 1
    return allocated
