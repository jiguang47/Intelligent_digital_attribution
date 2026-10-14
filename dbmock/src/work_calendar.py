"""中国法定节假日和调休工作日"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import chinese_calendar


@dataclass(frozen=True, slots=True)
class DayArrangement:
    is_holiday: int
    is_workday: int
    holiday_name: str | None


HOLIDAY_NAMES = {
    "New Year's Day": "元旦",
    "Spring Festival": "春节",
    "Tomb-sweeping Day": "清明节",
    "Labour Day": "劳动节",
    "Dragon Boat Festival": "端午节",
    "Mid-autumn Festival": "中秋节",
    "National Day": "国庆节",
    "Anti-Fascist 70th Day": "中国人民抗日战争暨世界反法西斯战争胜利70周年纪念日",
}


def build_work_calendar(
    start_date: date,
    end_date: date,
) -> dict[date, DayArrangement]:
    calendar: dict[date, DayArrangement] = {}
    current = start_date
    while current <= end_date:
        try:
            is_day_off, holiday_name = chinese_calendar.get_holiday_detail(current)
            is_workday = chinese_calendar.is_workday(current)
        except NotImplementedError as error:
            raise ValueError(f"缺少中国节假日安排: {current.year}") from error

        is_named_holiday = is_day_off and holiday_name is not None
        calendar[current] = DayArrangement(
            is_holiday=int(is_named_holiday),
            is_workday=int(is_workday),
            holiday_name=(
                _holiday_name_cn(holiday_name)
                if is_named_holiday and holiday_name is not None
                else None
            ),
        )
        current += timedelta(days=1)
    return calendar


def _holiday_name_cn(name: str) -> str:
    try:
        return HOLIDAY_NAMES[name]
    except KeyError as error:
        raise ValueError(f"未配置节假日中文名称: {name}") from error
