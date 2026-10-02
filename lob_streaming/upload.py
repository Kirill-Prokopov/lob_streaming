"""Compress, upload and clean up closed (no-longer-actively-written) recordings.

For every `<out_dir>/<symbol>/<date>.jsonl` whose trading day has already
closed, one file at a time:

  1. compress it to `<date>.jsonl.zst` -- zstd level 9 with a content checksum
     (~27x smaller on this data, ~95 MB/s on the 1-vCPU server). zstd rather
     than zip/gzip/bzip2 because Yandex Disk answers an upload of those only
     after processing the archive, ~0.27 s per MB of *uncompressed* content
     (measured: 280 MB -> 76 s, a day's files -> hours), while a zstd upload is
     answered in about a second. Higher levels cost far more CPU for little
     gain (level 15: 28x at 16 MB/s; level 19: 33x at 1 MB/s). Read the files
     with the `zstd` CLI, Python 3.14's `compression.zstd`, or the `zstandard`
     package;
  2. check the archive round-trips to byte-identical content (sha256);
  3. upload the archive -- only the compressed file ever goes to Yandex Disk;
  4. read the file's metadata back from Yandex Disk and require its size and
     checksums to match the local archive;
  5. only then delete the local raw file (and the archive).

Anything that fails leaves the local raw file untouched, to be retried on the
next run, and makes the run exit non-zero. Only today's file is never touched:
it is still open for writing.

Only allowed during the assumed exchange-closed window (`schedule.py`) --
a file from a closed day can't still be growing, but that window is also when
the CPU time for compressing is free. Needs Python 3.14+ (stdlib
`compression.zstd`) and the sibling `y_disk` package installed separately (not
a formal dependency of this package, since it's an unpublished local project,
not something on PyPI).
"""
from __future__ import annotations

import hashlib
import shutil
import time
from compression import zstd
from compression.zstd import CompressionParameter
from dataclasses import dataclass
from pathlib import Path

from .schedule import CLOSE_END, CLOSE_START, is_exchange_closed, trading_date

ZSTD_OPTIONS = {CompressionParameter.compression_level: 9, CompressionParameter.checksum_flag: 1}
CHUNK_SIZE = 1 << 20
MIN_AGE_S = 600  # skip a file modified in the last 10 minutes -- it may not be done growing
UPLOAD_ATTEMPTS = 3
VERIFY_POLLS = 12  # Yandex Disk can take a moment to publish a fresh file's checksums
VERIFY_POLL_S = 5.0


class UploadFailed(RuntimeError):
    """One or more files could not be uploaded and verified (they were kept locally)."""


@dataclass(frozen=True)
class Digests:
    size: int
    md5: str
    sha256: str


def file_digests(path: str | Path) -> Digests:
    md5 = hashlib.md5(usedforsecurity=False)
    sha256 = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            md5.update(chunk)
            sha256.update(chunk)
            size += len(chunk)
    return Digests(size, md5.hexdigest(), sha256.hexdigest())


def compress_to_zst(src: Path) -> Path:
    """`<date>.jsonl` -> `<date>.jsonl.zst` next to it, written under a
    `.part` name and renamed into place so a crash never leaves a
    truncated-but-complete-looking archive. Streams -- constant memory."""
    zst_path = src.with_name(src.name + ".zst")
    part = zst_path.with_name(zst_path.name + ".part")
    part.unlink(missing_ok=True)
    try:
        with open(src, "rb") as fin, zstd.open(part, "wb", options=ZSTD_OPTIONS) as fout:
            shutil.copyfileobj(fin, fout, CHUNK_SIZE)
        part.replace(zst_path)
    except BaseException:
        part.unlink(missing_ok=True)
        raise
    return zst_path


def zst_matches_source(zst_path: Path, expected_sha256: str) -> bool:
    """True if `zst_path` decompresses cleanly -- its frame checksum is verified
    on the way -- to bytes hashing to `expected_sha256`. A truncated or damaged
    archive counts as a mismatch."""
    sha256 = hashlib.sha256()
    try:
        with zstd.open(zst_path, "rb") as f:
            while chunk := f.read(CHUNK_SIZE):
                sha256.update(chunk)
    except (zstd.ZstdError, EOFError, OSError):
        return False
    return sha256.hexdigest() == expected_sha256


def upload_verified(
    client,
    archive_path: Path,
    remote_path: str,
    digests: Digests,
    attempts: int = UPLOAD_ATTEMPTS,
    polls: int = VERIFY_POLLS,
    poll_s: float = VERIFY_POLL_S,
) -> bool:
    """Upload `archive_path` (replacing any earlier upload at `remote_path`) and
    return True only once Yandex Disk reports the same size, md5 and -- when it
    reports one -- sha256. A missing md5 is never accepted as a match."""
    from y_disk import ResourceNotFoundError  # optional dependency, imported lazily

    for attempt in range(1, attempts + 1):
        client.upload_file(str(archive_path), remote_path, overwrite=True)
        for poll in range(polls):
            try:
                meta = client.get_meta(remote_path)
            except ResourceNotFoundError:
                meta = None
            if meta is not None and meta.md5 is not None:  # checksums published -- decide now
                if meta.size == digests.size and meta.md5 == digests.md5 and meta.sha256 in (None, digests.sha256):
                    return True
                print(
                    f"  remote mismatch (attempt {attempt}/{attempts}): "
                    f"size {meta.size} vs {digests.size}, md5 {meta.md5} vs {digests.md5}",
                    flush=True,
                )
                break
            if poll < polls - 1:
                time.sleep(poll_s)
        else:
            print(f"  no checksums from Yandex Disk after {polls} polls (attempt {attempt}/{attempts})", flush=True)
    return False


def _process_file(client, src: Path, remote_path: str, upload_kwargs: dict) -> bool:
    """Compress, verify, upload, verify remotely, then delete `src`. Returns
    True only if `src` was deleted -- i.e. a verified copy is safely remote."""
    stat_before = src.stat()
    source = file_digests(src)
    zst_path = compress_to_zst(src)
    try:
        if not zst_matches_source(zst_path, source.sha256):
            print(f"  FAILED {src}: archive does not round-trip to the original -- keeping it", flush=True)
            return False
        archive = file_digests(zst_path)
        if not upload_verified(client, zst_path, remote_path, archive, **upload_kwargs):
            print(f"  FAILED {src}: remote copy could not be verified -- keeping it", flush=True)
            return False
        stat_after = src.stat()
        if (stat_after.st_size, stat_after.st_mtime_ns) != (stat_before.st_size, stat_before.st_mtime_ns):
            print(f"  FAILED {src}: changed while being processed -- keeping it", flush=True)
            return False
        src.unlink()
        print(
            f"{remote_path}: {source.size / 1e6:.1f} MB -> {archive.size / 1e6:.1f} MB "
            f"({source.size / max(archive.size, 1):.1f}x), verified, local deleted",
            flush=True,
        )
        return True
    finally:
        zst_path.unlink(missing_ok=True)  # derived; regenerated on the next run if needed


def upload_closed_files(
    out_dir: str | Path,
    secrets_path: str | Path,
    remote_root: str = "app:/orderbook",
    token_name: str = "Y_DISK_OBS_KEY",
    force: bool = False,
    dry_run: bool = False,
    min_age_s: float = MIN_AGE_S,
    client=None,
    upload_attempts: int = UPLOAD_ATTEMPTS,
    verify_polls: int = VERIFY_POLLS,
    verify_poll_s: float = VERIFY_POLL_S,
) -> list[str]:
    """Process every recorded `*.jsonl` file whose trading day has already
    closed -- i.e. every one except today's (`schedule.trading_date()`) and any
    modified within `min_age_s` -- as described in the module docstring, each
    uploaded to `<remote_root>/<symbol>/<date>.jsonl.zst` (replacing any earlier
    upload of the same day in place, so reruns never pile up duplicates).
    Refuses to run outside the exchange-closed window unless `force=True`.
    `dry_run=True` only lists what would be done. `secrets_path` is the file
    `token_name` is read from -- passed through to `y_disk.load_token`
    explicitly rather than relying on its hardcoded default
    (`~/Code/.secrets`), since that's rarely where this project's own
    secrets actually live. Returns the remote paths uploaded and verified;
    raises `UploadFailed` at the end if any file could not be (the others are
    still processed).
    """
    if not force and not is_exchange_closed():
        raise RuntimeError(
            f"refusing to run outside the assumed exchange-closed window "
            f"({CLOSE_START}-{CLOSE_END} Moscow time); pass force=True to override"
        )

    if client is None and not dry_run:
        from y_disk import YandexDiskClient, load_token  # optional dependency, imported lazily

        client = YandexDiskClient(load_token(token_name, secrets_path))
    today = trading_date().isoformat()
    upload_kwargs = dict(attempts=upload_attempts, polls=verify_polls, poll_s=verify_poll_s)

    symbol_dirs = sorted(p for p in Path(out_dir).iterdir() if p.is_dir())
    if not dry_run:
        for symbol_dir in symbol_dirs:
            for stale in symbol_dir.glob("*.jsonl.zst.part"):  # a previous run died mid-compress
                stale.unlink()

    uploaded, failed = [], []
    for symbol_dir in symbol_dirs:
        for f in sorted(symbol_dir.glob("*.jsonl")):
            if f.stem >= today:
                continue  # still open for writing today -- skip
            if time.time() - f.stat().st_mtime < min_age_s:
                print(f"skipping {f}: modified in the last {min_age_s:.0f}s", flush=True)
                continue
            remote_path = f"{remote_root}/{symbol_dir.name}/{f.name}.zst"
            if dry_run:
                print(f"[dry run] would compress, upload and delete {f} ({f.stat().st_size / 1e6:.1f} MB) -> {remote_path}", flush=True)
                uploaded.append(remote_path)
                continue
            try:
                ok = _process_file(client, f, remote_path, upload_kwargs)
            except Exception as e:  # one bad file must not block the rest
                print(f"  FAILED {f}: {e!r}", flush=True)
                ok = False
            (uploaded if ok else failed).append(remote_path)

    if failed:
        raise UploadFailed(f"{len(failed)} file(s) not uploaded/verified, kept locally: {failed}")
    return uploaded


if __name__ == "__main__":
    import argparse
    import sys

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--secrets-path", required=True)
    parser.add_argument("--remote-root", default="app:/orderbook")
    parser.add_argument("--force", action="store_true", help="run outside the assumed exchange-closed window")
    parser.add_argument("--dry-run", action="store_true", help="only list what would be compressed/uploaded/deleted")
    args = parser.parse_args()
    try:
        paths = upload_closed_files(
            args.out_dir, args.secrets_path, remote_root=args.remote_root, force=args.force, dry_run=args.dry_run
        )
    except UploadFailed as e:
        print(f"FAILED: {e}", flush=True)
        sys.exit(1)
    print(f"done: {len(paths)} file(s)", flush=True)
