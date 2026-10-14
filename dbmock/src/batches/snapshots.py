"""由库存变更事件批量生成月内库存日快照"""

from __future__ import annotations

import logging

from sqlalchemy import text

from ..settings import RunContext
from ..timeline import MonthPeriod

logger = logging.getLogger(__name__)


def materialize_period(
    ctx: RunContext,
    period: MonthPeriod,
    batch_id: str,
) -> int:
    """在 Doris 内按月集合计算库存日快照"""
    parameters = {
        "period_start": period.start_date,
        "period_end": period.end_date,
        "as_of_time": ctx.as_of_time,
        "batch_id": batch_id,
    }
    with ctx.engine.connect() as conn:
        conn.execute(text(_INSERT_SQL), parameters)
        count = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM dwd_inventory_daily_snapshot_df
                    WHERE biz_date BETWEEN :period_start AND :period_end
                    """
                ),
                parameters,
            ).scalar_one()
        )
    logger.info("库存日快照批量生成完成 period=%s rows=%s", period.key, count)
    return count


_INSERT_SQL = """
INSERT INTO dwd_inventory_daily_snapshot_df (
    snapshot_date_key,
    warehouse_sk,
    sku_sk,
    warehouse_id,
    sku_id,
    spu_sk,
    spu_id,
    shop_sk,
    shop_id,
    on_hand_qty,
    reserved_qty,
    available_qty,
    in_transit_qty,
    unit_cost,
    inventory_cost_amount,
    currency_code,
    snapshot_time,
    biz_date,
    source_record_id,
    load_batch_id,
    dw_load_time
)
WITH
sku_base AS (
    SELECT warehouse_sk,
           warehouse_id,
           sku_sk,
           sku_id,
           spu_sk,
           spu_id,
           shop_sk,
           shop_id,
           MIN(biz_date) AS listing_date
    FROM dwd_inventory_change_di
    WHERE change_type = 'INITIAL_STOCK'
      AND biz_date <= :period_end
    GROUP BY warehouse_sk,
             warehouse_id,
             sku_sk,
             sku_id,
             spu_sk,
             spu_id,
             shop_sk,
             shop_id
),
daily_ending AS (
    SELECT warehouse_id,
           sku_id,
           biz_date,
           MAX_BY(after_on_hand_qty, inventory_change_id) AS on_hand_qty,
           MAX_BY(after_reserved_qty, inventory_change_id) AS reserved_qty,
           MAX_BY(after_in_transit_qty, inventory_change_id) AS in_transit_qty,
           MAX_BY(unit_cost, inventory_change_id) AS unit_cost
    FROM dwd_inventory_change_di
    WHERE biz_date BETWEEN :period_start AND :period_end
    GROUP BY warehouse_id, sku_id, biz_date
),
opening AS (
    SELECT warehouse_id,
           sku_id,
           MAX_BY(after_on_hand_qty, inventory_change_id) AS on_hand_qty,
           MAX_BY(after_reserved_qty, inventory_change_id) AS reserved_qty,
           MAX_BY(after_in_transit_qty, inventory_change_id) AS in_transit_qty,
           MAX_BY(unit_cost, inventory_change_id) AS unit_cost
    FROM dwd_inventory_change_di
    WHERE biz_date < :period_start
    GROUP BY warehouse_id, sku_id
),
calendar AS (
    SELECT b.*,
           d.full_date,
           d.date_key,
           GREATEST(b.listing_date, CAST(:period_start AS DATE)) AS first_date
    FROM sku_base b
    JOIN dim_date d
      ON d.full_date BETWEEN GREATEST(
             b.listing_date,
             CAST(:period_start AS DATE)
         ) AND CAST(:period_end AS DATE)
),
seeded AS (
    SELECT c.*,
           CASE
               WHEN e.biz_date IS NOT NULL THEN e.on_hand_qty
               WHEN c.full_date = c.first_date THEN o.on_hand_qty
           END AS on_hand_seed,
           CASE
               WHEN e.biz_date IS NOT NULL THEN e.reserved_qty
               WHEN c.full_date = c.first_date THEN o.reserved_qty
           END AS reserved_seed,
           CASE
               WHEN e.biz_date IS NOT NULL THEN e.unit_cost
               WHEN c.full_date = c.first_date THEN o.unit_cost
           END AS unit_cost_seed,
           CASE
               WHEN e.biz_date IS NOT NULL THEN e.in_transit_qty
               WHEN c.full_date = c.first_date THEN o.in_transit_qty
           END AS in_transit_seed
    FROM calendar c
    LEFT JOIN daily_ending e
      ON e.warehouse_id = c.warehouse_id
     AND e.sku_id = c.sku_id
     AND e.biz_date = c.full_date
    LEFT JOIN opening o
      ON o.warehouse_id = c.warehouse_id
     AND o.sku_id = c.sku_id
),
computed AS (
    SELECT *,
           LAST_VALUE(on_hand_seed, TRUE) OVER (
               PARTITION BY warehouse_id, sku_id
               ORDER BY full_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS on_hand_qty,
           LAST_VALUE(reserved_seed, TRUE) OVER (
               PARTITION BY warehouse_id, sku_id
               ORDER BY full_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS reserved_qty,
           LAST_VALUE(unit_cost_seed, TRUE) OVER (
               PARTITION BY warehouse_id, sku_id
               ORDER BY full_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS unit_cost,
           LAST_VALUE(in_transit_seed, TRUE) OVER (
               PARTITION BY warehouse_id, sku_id
               ORDER BY full_date
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS in_transit_qty
    FROM seeded
),
snapshot_rows AS (
    SELECT date_key AS snapshot_date_key,
           warehouse_sk,
           sku_sk,
           warehouse_id,
           sku_id,
           spu_sk,
           spu_id,
           shop_sk,
           shop_id,
           on_hand_qty,
           reserved_qty,
           on_hand_qty - reserved_qty AS available_qty,
           in_transit_qty,
           unit_cost,
           ROUND(unit_cost * on_hand_qty, 4) AS inventory_cost_amount,
           'CNY' AS currency_code,
           LEAST(
               CAST(
                   CONCAT(CAST(full_date AS STRING), ' 23:59:59.999999')
                   AS DATETIME(6)
               ),
               CAST(:as_of_time AS DATETIME(6))
           ) AS snapshot_time,
           full_date AS biz_date,
           CONCAT(
               'inventory-snapshot:',
               CAST(warehouse_id AS STRING),
               ':',
               CAST(sku_id AS STRING),
               ':',
               CAST(date_key AS STRING)
           ) AS source_record_id,
           :batch_id AS load_batch_id
    FROM computed
)
SELECT snapshot_date_key,
       warehouse_sk,
       sku_sk,
       warehouse_id,
       sku_id,
       spu_sk,
       spu_id,
       shop_sk,
       shop_id,
       on_hand_qty,
       reserved_qty,
       available_qty,
       in_transit_qty,
       unit_cost,
       inventory_cost_amount,
       currency_code,
       snapshot_time,
       biz_date,
       source_record_id,
       load_batch_id,
       LEAST(
           DATE_ADD(
               snapshot_time,
               INTERVAL (60 + MOD(sku_id, 240)) MINUTE
           ),
           CAST(:as_of_time AS DATETIME(6))
       ) AS dw_load_time
FROM snapshot_rows
"""
