"""Upload closed (no-longer-actively-written) order-book recordings to
Yandex Disk.

Only allowed during the assumed exchange-closed window (`schedule.py`) --
uploading outside it risks grabbing a mid-write snapshot of a file that
isn't actually done growing for the day yet. Needs the sibling `y_disk`
package installed separately (not a formal dependency of this package,
since it's an unpublished local project, not something on PyPI).
"""
from __future__ import annotations

from pathlib import Path

from .schedule import CLOSE_END, CLOSE_START, is_exchange_closed, trading_date


def upload_closed_files(
    out_dir: str | Path,
    secrets_path: str | Path,
    remote_root: str = "app:/orderbook",
    token_name: str = "Y_DISK_OBS_KEY",
    overwrite: bool = True,
    force: bool = False,
) -> list[str]:
    """Upload every recorded file whose trading day has already closed --
    i.e. every `*.jsonl` file except today's (`schedule.trading_date()`),
    which may still be open for writing. Refuses to run outside the
    exchange-closed window unless `force=True`. `overwrite=True` (default)
    replaces any previous upload of the same file in place, rather than
    piling up numbered duplicates on repeat runs. `secrets_path` is the
    file `token_name` is read from -- passed through to `y_disk.load_token`
    explicitly rather than relying on its hardcoded default
    (`~/Code/.secrets`), since that's rarely where this project's own
    secrets actually live. Returns the list of remote paths uploaded to.
    """
    if not force and not is_exchange_closed():
        raise RuntimeError(
            f"refusing to upload outside the assumed exchange-closed window "
            f"({CLOSE_START}-{CLOSE_END} Moscow time); pass force=True to override"
        )

    from y_disk import YandexDiskClient, load_token  # optional dependency, imported lazily

    client = YandexDiskClient(load_token(token_name, secrets_path))
    today = trading_date().isoformat()

    uploaded = []
    for symbol_dir in sorted(p for p in Path(out_dir).iterdir() if p.is_dir()):
        for f in sorted(symbol_dir.glob("*.jsonl")):
            if f.stem == today:
                continue  # still open for writing today -- skip
            remote_path = f"{remote_root}/{symbol_dir.name}/{f.name}"
            uploaded.append(client.upload_file(str(f), remote_path, overwrite=overwrite))
    return uploaded


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--secrets-path", required=True)
    args = parser.parse_args()
    for path in upload_closed_files(args.out_dir, args.secrets_path):
        print(f"uploaded: {path}", flush=True)
