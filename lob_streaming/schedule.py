"""Exchange-closed-hours handling and trading-day boundaries.

The Trade API's streaming protocol has no "market closed" signal of any
kind -- checked directly against the proto (see `orderbook.py`'s module
docstring): `StreamOrderBook` pushes carry no session/lifecycle field at
all. This module encodes a fixed, hand-set assumption instead: MOEX/FORTS
is closed 01:00-06:00 Moscow time, every day of the week. That same window
is where this project anchors the "trading day" boundary for file rotation
and naming (`trading_date`), and the only time it's considered safe to
upload a file that's no longer the one being actively appended to
(`upload.py`).
"""
from __future__ import annotations

import datetime as dt
from zoneinfo import ZoneInfo

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

# Assumed exchange-closed window, Moscow time, every day of the week -- not
# derived from AssetsService.Schedule, just a fixed, conservative assumption.
CLOSE_START = dt.time(1, 0)
CLOSE_END = dt.time(6, 0)


def is_exchange_closed(moment: dt.datetime | None = None) -> bool:
    """True if `moment` (default: now) falls in the assumed closed window,
    Moscow time."""
    moment = (moment or dt.datetime.now(dt.timezone.utc)).astimezone(MOSCOW_TZ)
    return CLOSE_START <= moment.time() < CLOSE_END


def trading_date(moment: dt.datetime | None = None) -> dt.date:
    """The trading-day label for `moment` (default: now). A new trading day
    starts at `CLOSE_START` (01:00) Moscow time, not at local midnight -- so
    00:30 MSK still belongs to the *previous* calendar day's file, and the
    boundary itself always lands inside the closed window, when nothing
    should be happening anyway."""
    moment = (moment or dt.datetime.now(dt.timezone.utc)).astimezone(MOSCOW_TZ)
    shifted = moment - dt.timedelta(hours=CLOSE_START.hour, minutes=CLOSE_START.minute)
    return shifted.date()
