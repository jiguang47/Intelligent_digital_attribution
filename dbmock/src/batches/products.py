"""真实商品维度初始化和日级商品状态生成"""

from __future__ import annotations

import logging
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from sqlalchemy import Table

from ..reference import (
    ReferenceData,
    listing_date_for_spu,
    warning_stock_qty_for_sku,
)
from ..settings import RunContext
from ..support import (
    END_OF_TIME,
    UNKNOWN_ID,
    UNKNOWN_SK,
    TableWriter,
    date_key,
    dimension_audit,
    fact_audit,
    iter_jsonl_rows,
    load_rows,
    start_of_day,
)
logger = logging.getLogger(__name__)


def _scd(
    attributes: dict[str, Any],
    ctx: RunContext,
    start_time: datetime,
) -> dict[str, Any]:
    return (
        attributes
        | {
            "effective_start_time": start_time,
            "effective_end_time": END_OF_TIME,
            "version_no": 1,
            "is_current": 1,
            "is_deleted": attributes.get("is_deleted", 0),
        }
        | dimension_audit(
            attributes,
            ctx.initial_batch_id,
        )
    )


def _spu_rows(ctx: RunContext):
    yield _scd(
        {
            "spu_sk": UNKNOWN_SK,
            "spu_id": UNKNOWN_ID,
            "spu_name": "未知SPU",
            "spu_sub_title": None,
            "category_id": UNKNOWN_ID,
            "brand_id": UNKNOWN_ID,
            "shop_id": UNKNOWN_ID,
            "is_virtual": 0,
            "is_presale": None,
            "presale_start_time": None,
            "presale_end_time": None,
            "weight_kg": None,
            "volume_m3": None,
            "on_shelf_time": None,
            "spu_status": "在售",
            "is_deleted": 0,
        },
        ctx,
        datetime(1900, 1, 1),
    )
    count = 0
    for index, source in enumerate(iter_jsonl_rows(ctx.gen.data_dir / "spus.jsonl")):
        if count >= ctx.gen.spu_count:
            break
        listing_date = listing_date_for_spu(ctx, index, int(source["spu_id"]))
        listing_time = start_of_day(listing_date)
        yield _scd(
            {
                "spu_id": int(source["spu_id"]),
                "spu_name": str(source["spu_name"]),
                "spu_sub_title": source.get("spu_sub_title"),
                "category_id": int(source["category_id"]),
                "brand_id": int(source["brand_id"]),
                "shop_id": int(source["shop_id"]),
                "is_virtual": int(source.get("is_virtual", 0)),
                "is_presale": source.get("is_presale"),
                "presale_start_time": None,
                "presale_end_time": None,
                "weight_kg": Decimal(str(source["weight_kg"]))
                if source.get("weight_kg") is not None
                else None,
                "volume_m3": Decimal(str(source["volume_m3"]))
                if source.get("volume_m3") is not None
                else None,
                "on_shelf_time": listing_time,
                "spu_status": str(source.get("spu_status", "在售")),
                "is_deleted": 0,
            },
            ctx,
            listing_time,
        )
        count += 1
    if count != ctx.gen.spu_count:
        raise ValueError(
            f"真实目录 SPU 不足 requested={ctx.gen.spu_count} available={count}"
        )


def _sku_rows(ctx: RunContext, spus: list[dict[str, Any]]):
    yield _scd(
        {
            "sku_sk": UNKNOWN_SK,
            "sku_id": UNKNOWN_ID,
            "sku_name": "未知SKU",
            "spu_id": UNKNOWN_ID,
            "shop_id": UNKNOWN_ID,
            "category_id": UNKNOWN_ID,
            "brand_id": UNKNOWN_ID,
            "bar_code": None,
            "sku_specs_json": {"未知": "未知"},
            "unit": None,
            "warning_stock_qty": 0,
            "sku_status": "在售",
            "is_deleted": 0,
        },
        ctx,
        datetime(1900, 1, 1),
    )
    spu_by_id = {int(row["spu_id"]): row for row in spus}
    counts: Counter[int] = Counter()
    for source in iter_jsonl_rows(ctx.gen.data_dir / "skus.jsonl"):
        spu_id = int(source["spu_id"])
        if spu_id not in spu_by_id:
            continue
        listing_time = spu_by_id[spu_id]["on_shelf_time"]
        yield _scd(
            {
                "sku_id": int(source["sku_id"]),
                "sku_name": str(source["sku_name"]),
                "spu_id": spu_id,
                "shop_id": int(source["shop_id"]),
                "category_id": int(source["category_id"]),
                "brand_id": int(source["brand_id"]),
                "bar_code": source.get("bar_code"),
                "sku_specs_json": source.get("sku_specs_json"),
                "unit": source.get("unit"),
                "warning_stock_qty": warning_stock_qty_for_sku(
                    int(source["sku_id"])
                ),
                "sku_status": str(source.get("sku_status", "在售")),
                "is_deleted": 0,
            },
            ctx,
            listing_time,
        )
        counts[spu_id] += 1
    invalid = [spu_id for spu_id in spu_by_id if counts[spu_id] == 0]
    if invalid:
        raise ValueError(f"存在没有真实 SKU 的 SPU: {invalid[:10]}")


def run_dimensions(ctx: RunContext, tables: dict[str, Table]) -> None:
    with ctx.engine.connect() as conn:
        writer = TableWriter(
            ctx.loader,
            ctx.gen.batch_size,
            ctx.gen.stream_load_workers,
            ctx.gen.start_date,
            ctx.as_of_time,
        )
        writer.add_many("dim_spu_info_zip", _spu_rows(ctx))
        writer.flush_all()
        spus = load_rows(
            conn,
            tables["dim_spu_info_zip"],
            where=(tables["dim_spu_info_zip"].c.is_current == 1)
            & (tables["dim_spu_info_zip"].c.spu_id != UNKNOWN_ID),
        )
        writer.add_many("dim_sku_info_zip", _sku_rows(ctx, spus))
        counts = writer.flush_all()
    logger.info("商品维度初始化完成 %s", counts)


def generate_price_events(
    ctx: RunContext,
    refs: ReferenceData,
    writer: TableWriter,
    day: date,
    batch_id: str,
) -> None:
    for local_index, (profile, point) in enumerate(
        refs.price_points_by_date.get(day, []),
        start=1,
    ):
        point_index = profile.price_points.index(point)
        previous = profile.price_points[point_index - 1] if point_index else None
        effective_time = start_of_day(day) + timedelta(minutes=1)
        writer.add(
            "dwd_product_sku_price_change_di",
            {
                "price_change_id": date_key(day) * 1_000_000 + local_index,
                "event_date_key": date_key(day),
                "sku_sk": profile.sku["sku_sk"],
                "sku_id": profile.sku["sku_id"],
                "spu_sk": profile.spu["spu_sk"],
                "spu_id": profile.spu["spu_id"],
                "shop_sk": profile.shop["shop_sk"],
                "shop_id": profile.shop["shop_id"],
                "category_sk": profile.category["category_sk"],
                "category_id": profile.category["category_id"],
                "brand_sk": profile.brand["brand_sk"]
                if profile.brand
                else UNKNOWN_SK,
                "brand_id": profile.brand["brand_id"] if profile.brand else None,
                "previous_list_price": previous.list_price if previous else None,
                "previous_sale_price": previous.sale_price if previous else None,
                "previous_cost_price": previous.cost_price if previous else None,
                "new_list_price": point.list_price,
                "new_sale_price": point.sale_price,
                "new_cost_price": point.cost_price,
                "change_reason_code": point.reason_code,
                "change_reason_description": point.reason_description,
                "currency_code": "CNY",
                "price_effective_time": effective_time,
                "change_time": effective_time - timedelta(minutes=5),
                "biz_date": day,
            }
            | fact_audit(
                f"sku-price:{profile.sku['sku_id']}:{point_index + 1}",
                batch_id,
            ),
        )
