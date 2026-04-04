"""Large relay file example for brokersystem.

This example keeps the artifact on the agent host and lets the broker relay it
to a client without permanently storing the bytes on the broker.

Run:
  BROKER_URL=https://... AGENT_AUTH='<agent_id>:<agent_secret>' \
    python examples/relay_large_file.py agent [--file /path/to/archive.zip]

  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/relay_large_file.py client --agent-id <agent_id> [--dest out.bin]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
import time
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin

import requests

from brokersystem import Agent, Broker, Number, RelayFile


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def require_agent_auth() -> str:
    agent_auth = os.environ.get("AGENT_AUTH")
    if agent_auth:
        return agent_auth
    agent_id = require_env("AGENT_ID")
    agent_secret = require_env("AGENT_SECRET")
    return f"{agent_id}:{agent_secret}"


def create_demo_large_file(path: Path, size_mib: int) -> Path:
    """Create a deterministic demo payload for relay testing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pattern = bytes(range(256)) * 4096
    remaining = size_mib * 1024 * 1024
    with path.open("wb") as handle:
        while remaining > 0:
            chunk = pattern[: min(len(pattern), remaining)]
            handle.write(chunk)
            remaining -= len(chunk)
    return path


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(64 * 1024)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def require_result_dict(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"Expected result field {field!r} to be an object.")
    return cast(dict[str, Any], value)


def require_result_int(value: object, *, field: str) -> int:
    if not isinstance(value, int):
        raise RuntimeError(f"Expected result field {field!r} to be an integer.")
    return value


def require_result_str(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"Expected result field {field!r} to be a string.")
    return value


def build_agent(file_path: Path) -> Agent:
    broker_url = require_env("BROKER_URL")
    agent_auth = require_agent_auth()

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "relay-large-file-example"
        agent.agent_auth = agent_auth
        agent.description = "Streams a large local file through the broker relay path."
        agent.charge = 1
        agent.output.archive = RelayFile(
            name=file_path.name,
            content_type="application/octet-stream",
            help="Large file kept on the agent host and relayed on demand.",
        )
        agent.output.size_bytes = Number(unit="B")

    @agent.job_func
    def job(_job) -> dict[str, object]:
        return {"archive": file_path, "size_bytes": file_path.stat().st_size}

    return agent


def download_relay_file(
    relay_uri: str, destination: Path, token: str
) -> tuple[float, str]:
    broker_url = require_env("BROKER_URL")
    destination.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    response = requests.get(
        urljoin(broker_url, relay_uri),
        headers={"Authorization": f"Token {token}"},
        stream=True,
        timeout=(10, 300),
    )
    response.raise_for_status()
    with destination.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if chunk:
                handle.write(chunk)
    elapsed = time.perf_counter() - started
    return elapsed, sha256_file(destination)


def run_agent(file_arg: str | None, size_mib: int) -> None:
    if file_arg:
        file_path = Path(file_arg).expanduser().resolve()
    else:
        file_path = create_demo_large_file(
            Path(tempfile.gettempdir())
            / "brokersystem_examples"
            / "relay_large_demo.bin",
            size_mib,
        )
    print("Serving relay file:", file_path)
    print("Bytes:", file_path.stat().st_size)
    build_agent(file_path).run()


def run_client(agent_id: str, destination: str | None) -> None:
    broker = Broker(
        broker_url=require_env("BROKER_URL"), auth=require_env("BROKER_TOKEN")
    )
    result_payload = broker.ask(agent_id, {})["result"]
    archive = require_result_dict(result_payload["archive"], field="archive")
    relay_uri = require_result_str(archive.get("uri"), field="archive.uri")
    archive_name = require_result_str(archive.get("name"), field="archive.name")
    size_bytes = require_result_int(result_payload["size_bytes"], field="size_bytes")
    destination_path = (
        Path(destination).expanduser().resolve()
        if destination
        else Path.cwd() / archive_name
    )
    elapsed, digest = download_relay_file(
        relay_uri,
        destination_path,
        require_env("BROKER_TOKEN"),
    )
    mib_s = (size_bytes / (1024 * 1024)) / elapsed if elapsed > 0 else float("inf")
    print("Downloaded to:", destination_path)
    print("SHA256:", digest)
    print("Elapsed seconds:", round(elapsed, 3))
    print("Throughput MiB/s:", round(mib_s, 3))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    agent_parser = subparsers.add_parser("agent")
    agent_parser.add_argument("--file", help="Existing local file to relay.")
    agent_parser.add_argument(
        "--size-mib",
        type=int,
        default=16,
        help="Size of the generated demo file when --file is omitted.",
    )

    client_parser = subparsers.add_parser("client")
    client_parser.add_argument("--agent-id", required=True)
    client_parser.add_argument(
        "--dest", help="Destination path for the downloaded file."
    )

    args = parser.parse_args()
    if args.mode == "agent":
        run_agent(args.file, args.size_mib)
    else:
        run_client(args.agent_id, args.dest)


if __name__ == "__main__":
    main()
