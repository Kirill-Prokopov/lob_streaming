"""Secret loading and client construction for the Finam Trade API.

The Trade API issues short-lived JWTs in exchange for a long-lived API
secret. `finam-sdk`'s FinamClient/AsyncFinamClient do the exchange and
background renewal for us -- this module just wires the project's `.secrets`
file into that client.
"""
from __future__ import annotations

from pathlib import Path

from finam_trade_api import AsyncFinamClient, FinamClient

DEFAULT_SECRET_VAR_NAME = "FINAM_OBS_KEY"

def load_secret(
    secrets_file: str | Path,
    var_name: str = DEFAULT_SECRET_VAR_NAME
) -> str:
    """Read `VAR=value` pairs from a single-line/dotenv-style secrets file."""
    text = Path(secrets_file).read_text()
    secrets = dict(
        line.strip().split("=", 1)
        for line in text.splitlines()
        if line.strip() and not line.strip().startswith("#")
    )
    if var_name not in secrets:
        raise KeyError(f"{var_name!r} not found in {secrets_file}")
    return secrets[var_name]


def get_client(
    secrets_file: str | Path,
    var_name: str = DEFAULT_SECRET_VAR_NAME
) -> FinamClient:
    """Sync client"""
    return FinamClient(secret=load_secret(secrets_file, var_name))


def get_async_client(
        secrets_file: str | Path,
        var_name: str = DEFAULT_SECRET_VAR_NAME
) -> AsyncFinamClient:
    """Async client"""
    return AsyncFinamClient(secret=load_secret(secrets_file, var_name))
