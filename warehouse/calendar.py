"""时区感知的日历映射:UTC 时刻 → 本地日 → ISO 周 / 月。

所有层级共用同一套映射,保证跨月边界(跨月的 ISO 周)与时区一致性:
周、月桶完全由本地日推导,任何层级不会独立解释原始时间戳。
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo


def parse_occurred_at(value) -> int:
    """ISO 8601 字符串或 epoch 秒 → epoch 秒(UTC)。朴素时间按 UTC 解释。"""
    if isinstance(value, (int, float)):
        return int(value)
    s = str(value).strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def epoch_to_day(epoch: int, tz: str) -> str:
    """UTC epoch 秒 → 配置时区下的本地日 YYYY-MM-DD。"""
    return datetime.fromtimestamp(epoch, ZoneInfo(tz)).date().isoformat()


def day_to_week(day: str) -> tuple[int, int, str, str]:
    """本地日 → (ISO 年, ISO 周, 周一日期, 周日日期)。周一起始,可跨月。"""
    d = date.fromisoformat(day)
    iso = d.isocalendar()
    monday = d - timedelta(days=d.isoweekday() - 1)
    sunday = monday + timedelta(days=6)
    return iso[0], iso[1], monday.isoformat(), sunday.isoformat()


def week_key(iso_year: int, iso_week: int) -> str:
    return f"{iso_year}-W{iso_week:02d}"


def week_bounds(iso_year: int, iso_week: int) -> tuple[str, str]:
    """ISO 周 → (周一日期, 周日日期)。"""
    monday = date.fromisocalendar(iso_year, iso_week, 1)
    return monday.isoformat(), (monday + timedelta(days=6)).isoformat()


def day_to_month(day: str) -> str:
    return day[:7]
