"""L2 order-book streaming and recording.

The Trade API has no historical order-book endpoint: `MarketDataService.OrderBook`
is a snapshot of *now*, and `SubscribeOrderBook` is a server-streamed feed of
updates going forward from the moment you subscribe. So "historic" order-book
data can only be built by recording the live stream yourself, continuously --
that's what `record_orderbook` does.

Granularity: `SubscribeOrderBook` gives rows tagged ACTION_ADD/REMOVE/UPDATE
at whatever depth your token's `MDPermission.quote_level` grants for that
instrument's `mic` -- from best-bid-offer up to full order-by-order
depth-of-book. This module always subscribes at that ceiling; there is no
coarser/finer knob to request on this RPC itself. Check `TokenDetails`
before assuming you're getting raw order-by-order data.

Reconnect policy: `arecord_orderbook` reconnects only on an actual stream
error (`grpc.RpcError`/`OSError`) -- there is no proactive reconnect on a
timer. An earlier version force-reconnected every 10 minutes as a hedge
against the ~15-minute JWT possibly invalidating an already-open stream
(never confirmed to actually happen), but empirically that cost ~2s of
missed data per cycle for a risk that's never been observed -- and it did
nothing to catch the one real multi-hour gap this recorder has hit in
production, which was a stream that went silent without ever raising an
error (see the gRPC keepalive discussion: `channel_options` is the correct
fix for a genuinely dead connection, not a blind timer).
"""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import grpc

from finam_trade_api import AsyncFinamClient, FinamClient
from finam_trade_api.market_data import SubscribeOrderBookRequest

from .auth import DEFAULT_SECRET_VAR_NAME, load_secret
from .convert import orderbook_row_to_dict
from .schedule import trading_date


def iter_orderbook(client: FinamClient, symbol: str) -> Iterator[dict]:
    """Sync generator, one dict per order-book push: `{symbol, receipt_time,
    is_data_snapshot, rows: [...]}`, `rows` from `convert.orderbook_row_to_dict`
    (kept nested under the push, so simultaneous row updates stay grouped).
    Good for a quick look at one symbol; `record_orderbook` handles many
    symbols concurrently via the asyncio client."""
    stream = client.market_data.SubscribeOrderBook(SubscribeOrderBookRequest(symbol=symbol))
    for resp in stream:
        receipt_time = dt.datetime.now(dt.timezone.utc)
        for ob in resp.order_book:
            yield _push_to_dict(ob, receipt_time)


def _push_to_dict(ob, receipt_time: dt.datetime) -> dict:
    return {
        "symbol": ob.symbol,
        "receipt_time": receipt_time,
        "is_data_snapshot": ob.is_data_snapshot,
        "rows": [orderbook_row_to_dict(ob.symbol, row, ob.is_data_snapshot, receipt_time) for row in ob.rows],
    }


async def aiter_orderbook(client: AsyncFinamClient, symbol: str) -> AsyncIterator[dict]:
    """Async counterpart of `iter_orderbook`."""
    async for resp in client.market_data.SubscribeOrderBook(SubscribeOrderBookRequest(symbol=symbol)):
        receipt_time = dt.datetime.now(dt.timezone.utc)
        for ob in resp.order_book:
            yield _push_to_dict(ob, receipt_time)


def _json_default(o):
    if isinstance(o, dt.datetime):
        return o.isoformat()
    raise TypeError(type(o))


def _out_path(out_dir: str | Path, symbol: str) -> Path:
    date = trading_date().isoformat()
    safe_symbol = symbol.replace("@", "_")
    path = Path(out_dir) / safe_symbol / f"{date}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


class _RotatingWriter:
    """One append-only JSONL file per symbol, rotating at each trading-day
    boundary (01:00 Moscow time -- see `schedule.trading_date`), not at UTC
    midnight."""

    def __init__(self, out_dir: str | Path):
        self.out_dir = out_dir
        self._open: dict[str, tuple[Path, object]] = {}

    def write(self, symbol: str, record: dict) -> None:
        path = _out_path(self.out_dir, symbol)
        if symbol not in self._open or self._open[symbol][0] != path:
            if symbol in self._open:
                self._open[symbol][1].close()
            self._open[symbol] = (path, open(path, "a"))
        f = self._open[symbol][1]
        f.write(json.dumps(record, default=_json_default) + "\n")
        f.flush()

    def close(self) -> None:
        for _, f in self._open.values():
            f.close()
        self._open.clear()


async def _record_one_symbol(client: AsyncFinamClient, symbol: str, writer: _RotatingWriter, counters: dict) -> None:
    async for record in aiter_orderbook(client, symbol):
        writer.write(symbol, record)
        counters[symbol] = counters.get(symbol, 0) + 1
        total = sum(counters.values())
        if total % 1000 == 0:
            print(f"  ... {total} messages recorded this connection {dict(counters)}", flush=True)


async def arecord_orderbook(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
    max_reconnects: int | None = None,
) -> None:
    """Async version of `record_orderbook` -- use this one directly (with
    `await`) from a notebook or any other code that already has an event loop
    running, since `asyncio.run()` cannot be nested inside one.

    Reconnects only on an actual stream error -- see the module docstring
    for why there's deliberately no proactive/timer-based reconnect. To
    bound how long a one-off run lasts (e.g. a demo), wrap the call itself
    in `asyncio.wait_for(arecord_orderbook(...), timeout=...)` at the call
    site rather than passing a timeout into this function.
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


def record_orderbook(
    symbols: list[str],
    out_dir: str | Path,
    secrets_file: str | Path,
    secret_var: str = DEFAULT_SECRET_VAR_NAME,
    max_reconnects: int | None = None,
) -> None:
    """Continuously record every `symbol`'s order-book stream to
    `<out_dir>/<TICKER_MIC>/<trading-date>.jsonl` (one JSON object per line,
    from `aiter_orderbook`), rotating the file at each trading-day boundary
    (01:00 Moscow time -- `schedule.trading_date`) and reconnecting only on
    an actual stream error. Runs until interrupted (Ctrl-C) or
    `max_reconnects` connection attempts are used up.

    Every symbol is streamed concurrently on one asyncio client (one task per
    symbol, all multiplexed over the same gRPC/HTTP2 channel), so a quiet
    symbol never blocks a busy one.

    From a notebook or any other code with an event loop already running,
    `await arecord_orderbook(...)` directly instead -- `asyncio.run()` (used
    here) cannot be nested inside one.
    """
    asyncio.run(arecord_orderbook(symbols, out_dir, secrets_file, secret_var, max_reconnects))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("symbols", nargs="+", help="e.g. MXZ6@RTSX MMZ6@RTSX")
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--secrets-file", required=True)
    parser.add_argument("--secret-var", default=DEFAULT_SECRET_VAR_NAME)
    args = parser.parse_args()
    t0 = time.time()
    print(f"recording {args.symbols} -> {args.out_dir} (Ctrl-C to stop)", flush=True)
    try:
        record_orderbook(args.symbols, out_dir=args.out_dir, secrets_file=args.secrets_file, secret_var=args.secret_var)
    except KeyboardInterrupt:
        print(f"stopped after {time.time() - t0:.0f}s", flush=True)
