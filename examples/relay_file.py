"""Large relay file example for brokersystem.

This example keeps the artifact on the agent host and lets the broker relay it
to a client without permanently storing the bytes on the broker.

Run:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    python examples/relay_file.py agent [--file /path/to/archive.zip]

  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/relay_file.py client --agent-id <agent_id> [--dest out.bin]
"""

from __future__ import annotations

import argparse
import hashlib
import os
import tempfile
import time
from pathlib import Path
from typing import Mapping

from brokersystem import Agent, Broker, Job, Number, RelayFile, RelayFileHandle


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


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


def read_archive_result(
    broker: Broker, result_payload: Mapping[str, object]
) -> RelayFileHandle:
    match result_payload:
        case {"archive": dict() as archive_obj}:
            return broker.parse_relay_file(archive_obj)
        case _:
            raise RuntimeError("Unexpected broker result payload.")


def build_agent(file_path: Path) -> Agent:
    broker_url = require_env("BROKER_URL")
    agent_auth = require_env("AGENT_AUTH")

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "relay-file-example-sdk"
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
    def job(_job: Job) -> dict[str, object]:
        return {"archive": file_path, "size_bytes": file_path.stat().st_size}

    return agent


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
    relay_file = read_archive_result(broker, result_payload)
    destination_path = (
        Path(destination).expanduser().resolve()
        if destination
        else Path.cwd() / relay_file.name
    )
    started = time.perf_counter()
    broker.download_file(relay_file, destination_path)
    elapsed = time.perf_counter() - started
    digest = sha256_file(destination_path)
    mib_s = (
        (relay_file.size_bytes / (1024 * 1024)) / elapsed
        if elapsed > 0
        else float("inf")
    )
    print("Downloaded to:", destination_path)
    print("Relay URI:", relay_file.uri)
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
