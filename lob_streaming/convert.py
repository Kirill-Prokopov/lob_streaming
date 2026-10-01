"""Helpers to turn Trade API protobuf messages into plain Python values.

The API represents prices as `google.type.Decimal` (a decimal-formatted
string, not a binary float) and timestamps as `google.protobuf.Timestamp`.
Everything downstream in this project works with plain floats and
timezone-aware datetimes instead, converted here in one place.
"""
from __future__ import annotations

import datetime as dt
from typing import Any


def dec(value: Any) -> float:
    """`google.type.Decimal` -> float. `value` may also be a plain number/None."""
    if value is None:
        return float("nan")
    if hasattr(value, "value"):  # google.type.Decimal
        value = value.value
    return float(value)


def ts(timestamp: Any) -> dt.datetime:
    """`google.protobuf.Timestamp` -> timezone-aware UTC datetime."""
    return timestamp.ToDatetime(tzinfo=dt.timezone.utc)


_ACTION_NAMES = {0: "UNSPECIFIED", 1: "REMOVE", 2: "ADD", 3: "UPDATE"}


def orderbook_row_to_dict(symbol: str, row: Any, is_snapshot: bool, receipt_time: dt.datetime | None = None) -> dict:
    side = row.WhichOneof("side")  # "sell_size" or "buy_size"
    return {
        "symbol": symbol,
        "receipt_time": receipt_time or dt.datetime.now(dt.timezone.utc),
        "exchange_time": ts(row.timestamp),
        "is_data_snapshot": is_snapshot,
        "action": _ACTION_NAMES.get(row.action, row.action),
        "side": "sell" if side == "sell_size" else "buy",
        "price": dec(row.price),
        "size": dec(getattr(row, side)),
        "mpid": row.mpid,
    }
