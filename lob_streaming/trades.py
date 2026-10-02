"""Trade (time & sales) streaming and recording.

Counterpart of `orderbook.py`: `MarketDataService.SubscribeLatestTrades` is a
server-streamed feed of executed trades going forward from the moment you
subscribe, and there is no way to ask for history on it -- so, like the order
book, historic trades can only be built by recording the live stream
continuously. That's what `record_trades` does.

Each push is stored as one JSON line `{symbol, receipt_time, trades: [...]}`
(same "keep a push's items grouped" layout as the order-book recorder), with
the same per-symbol, trading-day-rotated file naming, so the two recordings
line up file-for-file. Every trade carries its own `is_data_snapshot` flag,
and the recording is deliberately raw -- downstream code must clean it up:

* The server re-sends the latest trade flagged `is_data_snapshot=True`: as
  the very first push after (re)subscribing (its live copy then arrives a few
  pushes later) and also periodically mid-stream, a second or two after the
  trade was delivered live. So de-duplicate on `(symbol, trade_id)`; never
  assume each trade appears exactly once.
* Because of that first snapshot, a file is not strictly ordered by
  `exchange_time` right after a (re)subscribe. `trade_id` increases
  monotonically, so sort on it to restore the true order.

Reconnect policy is the same as `orderbook.arecord_orderbook`: only on an
actual stream error, never on a timer.

File writing is reused from `orderbook.py` (`_RotatingWriter`) rather than
moved to a shared module, so this recorder can be deployed without touching
the modules the live order-book recorder runs from.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import grpc

from finam_trade_api import AsyncFinamClient, FinamClient
from finam_trade_api.market_data import SubscribeLatestTradesRequest

from .auth import DEFAULT_SECRET_VAR_NAME, load_secret
from .convert import dec, ts
from .orderbook import _RotatingWriter

_SIDE_NAMES = {0: "UNSPECIFIED", 1: "buy", 2: "sell"}


def trade_to_dict(symbol: str, trade, receipt_time: dt.datetime | None = None) -> dict:
    # `open_interest` is optional on the wire: when unset it reads back as an
    # empty Decimal, which `dec()` can't parse -- record it as null instead.
    open_interest = dec(trade.open_interest) if trade.open_interest.value else None
    return {
        "symbol": symbol,
        "receipt_time": receipt_time or dt.datetime.now(dt.timezone.utc),
        "exchange_time": ts(trade.timestamp),
        "trade_id": trade.trade_id,
        "is_data_snapshot": trade.is_data_snapshot,
        "side": _SIDE_NAMES.get(trade.side, trade.side),
        "price": dec(trade.price),
        "size": dec(trade.size),
        "open_interest": open_interest,
        "mpid": trade.mpid,
    }


def _push_to_dict(resp, receipt_time: dt.datetime) -> dict:
    return {
        "symbol": resp.symbol,
        "receipt_time": receipt_time,
        "trades": [trade_to_dict(resp.symbol, t, receipt_time) for t in resp.trades],
    }


def iter_trades(client: FinamClient, symbol: str) -> Iterator[dict]:
    """Sync generator, one dict per trades push: `{symbol, receipt_time,
    trades: [...]}`. Good for a quick look at one symbol; `record_trades`
    handles many symbols concurrently via the asyncio client."""
    stream = client.market_data.SubscribeLatestTrades(SubscribeLatestTradesRequest(symbol=symbol))
    for resp in stream:
        yield _push_to_dict(resp, dt.datetime.now(dt.timezone.utc))


async def aiter_trades(client: AsyncFinamClient, symbol: str) -> AsyncIterator[dict]:
    """Async counterpart of `iter_trades`."""
    async for resp in client.market_data.SubscribeLatestTrades(SubscribeLatestTradesRequest(symbol=symbol)):
        yield _push_to_dict(resp, dt.datetime.now(dt.timezone.utc))


async def _record_one_symbol(client: AsyncFinamClient, symbol: str, writer: _RotatingWriter, counters: dict) -> None:
    async for record in aiter_trades(client, symbol):
        writer.write(symbol, record)
        counters[symbol] = counters.get(symbol, 0) + 1
        total = sum(counters.values())
        if total % 1000 == 0:
            print(f"  ... {total} messages recorded this connection {dict(counters)}", flush=True)


async def arecord_trades(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
    max_reconnects: int | None = None,
) -> None:
    """Async version of `record_trades` -- use this one directly (with
    `await`) from a notebook or any other code that already has an event loop
    running, since `asyncio.run()` cannot be nested inside one.

    Reconnects only on an actual stream error. To bound how long a one-off run
    lasts (e.g. a smoke test), wrap the call itself in
    `asyncio.wait_for(arecord_trades(...), timeout=...)` at the call site.
    """
    secret = load_secret(secrets_file, secret_var)
    attempts = 0
    while max_reconnects is None or attempts < max_reconnects:
        attempts += 1
        writer = _RotatingWriter(out_dir)
        counters: dict[str, int] = {}
        try:
            async with AsyncFinamClient(secret=secret) as client:
                tasks = [asyncio.create_task(_record_one_symbol(client, s, writer, counters)) for s in symbols]
                await asyncio.gather(*tasks)
        except (grpc.RpcError, grpc.aio.AioRpcError, OSError) as e:
            print(f"stream error: {e!r}", flush=True)
        finally:
            writer.close()
        print("reconnecting...", flush=True)
        await asyncio.sleep(2)


def record_trades(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
    max_reconnects: int | None = None,
) -> None:
    """Continuously record every `symbol`'s trade stream to
    `<out_dir>/<TICKER_MIC>/<trading-date>.jsonl` (one JSON object per push,
    from `aiter_trades`), rotating the file at each trading-day boundary
    (01:00 Moscow time -- `schedule.trading_date`) and reconnecting only on
    an actual stream error. Runs until interrupted (Ctrl-C) or
    `max_reconnects` connection attempts are used up.

    Every symbol is streamed concurrently on one asyncio client, one task per
    symbol, so a quiet symbol never blocks a busy one.
    """
    asyncio.run(arecord_trades(symbols, out_dir, secrets_file, secret_var, max_reconnects))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("symbols", nargs="+", help="e.g. MXZ6@RTSX MMZ6@RTSX")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--secrets-file", required=True)
    parser.add_argument("--secret-var", default=DEFAULT_SECRET_VAR_NAME)
    args = parser.parse_args()
    t0 = time.time()
    print(f"recording trades {args.symbols} -> {args.out_dir} (Ctrl-C to stop)", flush=True)
    try:
        record_trades(args.symbols, out_dir=args.out_dir, secrets_file=args.secrets_file, secret_var=args.secret_var)
    except KeyboardInterrupt:
        print(f"stopped after {time.time() - t0:.0f}s", flush=True)
