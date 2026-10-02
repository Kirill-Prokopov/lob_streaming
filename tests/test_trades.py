"""Offline tests for `lob_streaming.trades` -- hand-built protobuf messages,
no network, no credentials. Run with: python -m pytest tests"""
import datetime as dt
import json

from finam_trade_api.market_data import SubscribeLatestTradesResponse, Trade
from google.protobuf.timestamp_pb2 import Timestamp
from google.type.decimal_pb2 import Decimal

from lob_streaming.orderbook import _RotatingWriter
from lob_streaming.trades import _push_to_dict, trade_to_dict

RECEIPT = dt.datetime(2026, 10, 2, 12, 0, 0, tzinfo=dt.timezone.utc)


def _trade(trade_id="42", side=1, price="2650.5", size="3", open_interest=None, snapshot=False) -> Trade:
    ts = Timestamp()
    ts.FromDatetime(dt.datetime(2026, 10, 2, 11, 59, 59, 500000))
    t = Trade(
        trade_id=trade_id,
        mpid="MPID",
        timestamp=ts,
        price=Decimal(value=price),
        size=Decimal(value=size),
        side=side,
        is_data_snapshot=snapshot,
    )
    if open_interest is not None:
        t.open_interest.CopyFrom(Decimal(value=open_interest))
    return t


def test_trade_to_dict_fields():
    d = trade_to_dict("MXZ6@RTSX", _trade(open_interest="1500"), RECEIPT)
    assert d == {
        "symbol": "MXZ6@RTSX",
        "receipt_time": RECEIPT,
        "exchange_time": dt.datetime(2026, 10, 2, 11, 59, 59, 500000, tzinfo=dt.timezone.utc),
        "trade_id": "42",
        "is_data_snapshot": False,
        "side": "buy",
        "price": 2650.5,
        "size": 3.0,
        "open_interest": 1500.0,
        "mpid": "MPID",
    }


def test_unset_open_interest_is_null_not_a_crash():
    assert trade_to_dict("X@RTSX", _trade(), RECEIPT)["open_interest"] is None


def test_side_names():
    assert trade_to_dict("X@RTSX", _trade(side=1), RECEIPT)["side"] == "buy"
    assert trade_to_dict("X@RTSX", _trade(side=2), RECEIPT)["side"] == "sell"
    assert trade_to_dict("X@RTSX", _trade(side=0), RECEIPT)["side"] == "UNSPECIFIED"


def test_push_keeps_trades_grouped_and_snapshot_flag_per_trade():
    resp = SubscribeLatestTradesResponse(
        symbol="MXZ6@RTSX",
        trades=[_trade("1", snapshot=True), _trade("2", side=2, snapshot=False)],
    )
    push = _push_to_dict(resp, RECEIPT)
    assert set(push) == {"symbol", "receipt_time", "trades"}
    assert push["symbol"] == "MXZ6@RTSX"
    assert [t["trade_id"] for t in push["trades"]] == ["1", "2"]
    assert [t["is_data_snapshot"] for t in push["trades"]] == [True, False]
    assert all(t["symbol"] == "MXZ6@RTSX" and t["receipt_time"] == RECEIPT for t in push["trades"])


def test_written_line_is_valid_json_in_the_orderbook_layout(tmp_path):
    resp = SubscribeLatestTradesResponse(symbol="MXZ6@RTSX", trades=[_trade("7")])
    writer = _RotatingWriter(tmp_path)
    writer.write("MXZ6@RTSX", _push_to_dict(resp, RECEIPT))
    writer.close()

    (path,) = list((tmp_path / "MXZ6_RTSX").glob("*.jsonl"))  # <out>/<TICKER_MIC>/<date>.jsonl
    (line,) = path.read_text().splitlines()
    record = json.loads(line)
    assert record["symbol"] == "MXZ6@RTSX"
    assert record["receipt_time"] == "2026-10-02T12:00:00+00:00"
    assert record["trades"][0]["trade_id"] == "7"
    assert record["trades"][0]["open_interest"] is None
