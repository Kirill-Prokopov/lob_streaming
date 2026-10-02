"""Offline tests for `lob_streaming.upload` with a fake Yandex Disk client --
no network, no credentials. The point of most of them: a local raw file may only
ever be deleted after a verified remote copy exists. Run with: python -m pytest tests"""
import hashlib
import io
import os
import zipfile
from pathlib import Path

import pytest
from y_disk import DiskItem, ResourceNotFoundError

from lob_streaming import upload
from lob_streaming.schedule import trading_date
from lob_streaming.upload import UploadFailed, upload_closed_files

OLD_DAY = "2020-01-02"
PAYLOAD = b'{"symbol": "MXZ6@RTSX", "rows": []}\n' * 5000


class FakeClient:
    """Stands in for `YandexDiskClient`: keeps uploads in memory and answers
    `get_meta` the way Yandex Disk does, with knobs for the ways it can go wrong."""

    def __init__(self, truncate_paths=(), checksum_lag_polls=0, missing_polls=0, on_upload=None):
        self.files: dict[str, bytes] = {}
        self.upload_calls: list[str] = []
        self.truncate_paths = set(truncate_paths)  # uploads here arrive damaged
        self.checksum_lag_polls = checksum_lag_polls  # get_meta answers without md5 this many times
        self.missing_polls = missing_polls  # get_meta answers "not found" this many times
        self.on_upload = on_upload

    def upload_file(self, local_path, remote_path, make_unique=True, overwrite=False):
        assert overwrite, "uploads must replace in place, not pile up duplicates"
        self.upload_calls.append(remote_path)
        data = Path(local_path).read_bytes()
        if remote_path in self.truncate_paths:
            data = data[: len(data) // 2]
        self.files[remote_path] = data
        if self.on_upload:
            self.on_upload()
        return remote_path

    def get_meta(self, path):
        if self.missing_polls > 0:
            self.missing_polls -= 1
            raise ResourceNotFoundError(404, "not found", {})
        data = self.files[path]
        lagging = self.checksum_lag_polls > 0
        if lagging:
            self.checksum_lag_polls -= 1
        return DiskItem(
            name=Path(path).name,
            path=path,
            type="file",
            size=len(data),
            md5=None if lagging else hashlib.md5(data).hexdigest(),
            sha256=None if lagging else hashlib.sha256(data).hexdigest(),
        )


def _make(out_dir: Path, symbol: str, day: str, payload: bytes = PAYLOAD, age_s: float = 3600) -> Path:
    path = out_dir / symbol / f"{day}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    old = path.stat().st_mtime - age_s
    os.utime(path, (old, old))
    return path


def _run(out_dir, client, **kw):
    kw.setdefault("force", True)
    kw.setdefault("verify_poll_s", 0)
    return upload_closed_files(out_dir, "unused-secrets", remote_root="app:/root", client=client, **kw)


def test_happy_path_uploads_a_valid_zip_and_deletes_local_files(tmp_path):
    src = _make(tmp_path, "MXZ6_RTSX", OLD_DAY)
    client = FakeClient()

    assert _run(tmp_path, client) == [f"app:/root/MXZ6_RTSX/{OLD_DAY}.jsonl.zip"]

    remote = client.files[f"app:/root/MXZ6_RTSX/{OLD_DAY}.jsonl.zip"]
    with zipfile.ZipFile(io.BytesIO(remote)) as zf:  # a plain, standard ZIP: any OS can open it
        assert zf.namelist() == [f"{OLD_DAY}.jsonl"]
        assert zf.getinfo(f"{OLD_DAY}.jsonl").compress_type == zipfile.ZIP_DEFLATED
        assert zf.read(f"{OLD_DAY}.jsonl") == PAYLOAD
    assert len(remote) < len(PAYLOAD) / 10  # actually compressed
    assert not src.exists()
    assert list(tmp_path.rglob("*.zip*")) == []  # no archive or .part left behind


def test_only_compressed_data_is_ever_uploaded(tmp_path):
    _make(tmp_path, "A_RTSX", OLD_DAY)
    client = FakeClient()
    _run(tmp_path, client)
    assert all(p.endswith(".jsonl.zip") for p in client.files)


def test_todays_file_is_never_touched(tmp_path):
    today = _make(tmp_path, "A_RTSX", trading_date().isoformat())
    client = FakeClient()
    assert _run(tmp_path, client) == []
    assert today.read_bytes() == PAYLOAD and client.upload_calls == []


def test_recently_modified_file_is_skipped(tmp_path):
    fresh = _make(tmp_path, "A_RTSX", OLD_DAY, age_s=0)
    client = FakeClient()
    assert _run(tmp_path, client, min_age_s=600) == []
    assert fresh.exists() and client.upload_calls == []


def test_damaged_upload_keeps_the_local_file_and_fails_the_run(tmp_path):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)
    remote = f"app:/root/A_RTSX/{OLD_DAY}.jsonl.zip"
    client = FakeClient(truncate_paths=[remote])

    with pytest.raises(UploadFailed):
        _run(tmp_path, client)

    assert src.read_bytes() == PAYLOAD
    assert len(client.upload_calls) == upload.UPLOAD_ATTEMPTS  # retried before giving up
    assert list(tmp_path.rglob("*.zip*")) == []


def test_archive_that_does_not_round_trip_is_never_uploaded(tmp_path, monkeypatch):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)
    real_compress = upload.compress_to_zip

    def damaged_compress(path):  # e.g. the disk filled up mid-write
        zip_path = real_compress(path)
        zip_path.write_bytes(zip_path.read_bytes()[:-50])
        return zip_path

    monkeypatch.setattr(upload, "compress_to_zip", damaged_compress)
    client = FakeClient()

    with pytest.raises(UploadFailed):
        _run(tmp_path, client)

    assert src.read_bytes() == PAYLOAD
    assert client.upload_calls == []  # never even sent
    assert list(tmp_path.rglob("*.zip*")) == []


def test_one_bad_file_does_not_block_the_others(tmp_path):
    bad = _make(tmp_path, "BAD_RTSX", OLD_DAY)
    good = _make(tmp_path, "GOOD_RTSX", OLD_DAY)
    client = FakeClient(truncate_paths=[f"app:/root/BAD_RTSX/{OLD_DAY}.jsonl.zip"])

    with pytest.raises(UploadFailed, match="BAD_RTSX"):
        _run(tmp_path, client)

    assert bad.exists() and not good.exists()


def test_waits_for_checksums_that_arrive_late(tmp_path):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)
    client = FakeClient(checksum_lag_polls=3, missing_polls=2)
    assert len(_run(tmp_path, client)) == 1
    assert not src.exists()


def test_never_accepts_a_remote_copy_that_has_no_checksums(tmp_path):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)
    client = FakeClient(checksum_lag_polls=10_000)
    with pytest.raises(UploadFailed):
        _run(tmp_path, client, verify_polls=3)
    assert src.exists()


def test_file_that_grows_during_processing_is_kept(tmp_path):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)

    def append_late():
        with open(src, "ab") as f:
            f.write(b'{"late": true}\n')

    with pytest.raises(UploadFailed):
        _run(tmp_path, FakeClient(on_upload=append_late))
    assert src.read_bytes().endswith(b'{"late": true}\n')


def test_dry_run_changes_nothing(tmp_path):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)
    assert _run(tmp_path, None, dry_run=True) == [f"app:/root/A_RTSX/{OLD_DAY}.jsonl.zip"]
    assert src.read_bytes() == PAYLOAD and list(tmp_path.rglob("*.zip*")) == []


def test_stale_part_file_from_a_crashed_run_is_removed(tmp_path):
    _make(tmp_path, "A_RTSX", OLD_DAY)
    stale = tmp_path / "A_RTSX" / f"{OLD_DAY}.jsonl.zip.part"
    stale.write_bytes(b"half a zip")
    _run(tmp_path, FakeClient())
    assert not stale.exists()


def test_refuses_to_run_outside_the_closed_window_unless_forced(tmp_path, monkeypatch):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)
    monkeypatch.setattr(upload, "is_exchange_closed", lambda: False)
    with pytest.raises(RuntimeError, match="exchange-closed window"):
        _run(tmp_path, FakeClient(), force=False)
    assert src.exists()
    assert len(_run(tmp_path, FakeClient(), force=True)) == 1


def test_client_is_built_with_a_timeout_long_enough_for_yandex_to_process_big_archives(tmp_path, monkeypatch):
    seen = {}

    class SpyClient:
        def __init__(self, token, timeout=None):
            seen["timeout"] = timeout

    monkeypatch.setattr("y_disk.YandexDiskClient", SpyClient)
    monkeypatch.setattr("y_disk.load_token", lambda name, path: "token")
    (tmp_path / "A_RTSX").mkdir()

    upload_closed_files(tmp_path, "unused-secrets", force=True)  # builds the real-path client; nothing to upload

    assert seen["timeout"] == upload.UPLOAD_TIMEOUT_S >= 600  # the library default (60 s) failed on 280+ MB days


def test_zip_matches_source_rejects_wrong_content_and_damage(tmp_path):
    src = _make(tmp_path, "A_RTSX", OLD_DAY)
    zip_path = upload.compress_to_zip(src)
    good = hashlib.sha256(PAYLOAD).hexdigest()
    assert upload.zip_matches_source(zip_path, src.name, good)
    assert not upload.zip_matches_source(zip_path, src.name, hashlib.sha256(b"other").hexdigest())
    zip_path.write_bytes(zip_path.read_bytes()[:-50])  # truncated archive
    assert not upload.zip_matches_source(zip_path, src.name, good)
