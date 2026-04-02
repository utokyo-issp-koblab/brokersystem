"""Segmented video relay example for brokersystem.

This demonstrates a realistic broker-friendly pattern for video delivery today:
the agent keeps short local segments and the client pulls one segment at a time.
It is not WebRTC or true low-latency media streaming, but it maps well to the
current broker relay features.

Run:
  BROKER_URL=https://... AGENT_ID=... AGENT_SECRET=... \
    python examples/relay_segmented_video.py agent

  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/relay_segmented_video.py client --agent-id <agent_id>
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin

import requests

from brokersystem import Agent, Broker, Choice, Number, RelayFile, String


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def create_demo_segments(
    directory: Path, *, segment_count: int, segment_size_kib: int
) -> list[Path]:
    """Create deterministic transport-stream-like demo segments."""
    directory.mkdir(parents=True, exist_ok=True)
    pattern = bytes(range(256)) * 1024
    segments: list[Path] = []
    for index in range(segment_count):
        path = directory / f"segment_{index:04d}.ts"
        remaining = segment_size_kib * 1024
        with path.open("wb") as handle:
            while remaining > 0:
                chunk = pattern[: min(len(pattern), remaining)]
                handle.write(chunk)
                remaining -= len(chunk)
        segments.append(path)
    return segments


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


def require_result_float(value: object, *, field: str) -> float:
    if not isinstance(value, (int, float)):
        raise RuntimeError(f"Expected result field {field!r} to be numeric.")
    return float(value)


def require_result_str(value: object, *, field: str) -> str:
    if not isinstance(value, str):
        raise RuntimeError(f"Expected result field {field!r} to be a string.")
    return value


def build_segment_agent(segments: list[Path], segment_duration_s: float) -> Agent:
    broker_url = require_env("BROKER_URL")
    agent_id = require_env("AGENT_ID")
    agent_secret = require_env("AGENT_SECRET")

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "relay-segmented-video-example"
        agent.secret_token = f"{agent_id}:{agent_secret}"
        agent.description = "Serves short local video segments through broker relay one request at a time."
        agent.charge = 1
        agent.input.request_kind = Choice(
            ["manifest", "segment"],
            help="First request manifest, then request individual segments.",
        )
        agent.input.segment_index = Number(
            0,
            min=0,
            max=max(len(segments) - 1, 0),
            help="Segment index for request_kind=segment.",
        )
        agent.output.segment_count = Number(unit="segments")
        agent.output.segment_duration_s = Number(unit="s")
        agent.output.segment_prefix = String(
            "segment_",
            help="Client uses this naming convention for sequential segment pulls.",
        )
        agent.output.segment_name = String(help="Name of the returned segment.")
        agent.output.segment = RelayFile(
            content_type="video/mp2t",
            help="Single video segment relayed through the broker.",
        )

    @agent.job_func
    def job(job) -> dict[str, object]:
        request_kind = job["request_kind"]
        if request_kind == "manifest":
            return {
                "segment_count": len(segments),
                "segment_duration_s": segment_duration_s,
                "segment_prefix": "segment_",
            }

        index = int(job["segment_index"])
        segment_path = segments[index]
        return {
            "segment_count": len(segments),
            "segment_duration_s": segment_duration_s,
            "segment_prefix": "segment_",
            "segment_name": segment_path.name,
            "segment": segment_path,
        }

    return agent


def download_segment(
    relay_uri: str, destination: Path, token: str
) -> tuple[float, str]:
    broker_url = require_env("BROKER_URL")
    response = requests.get(
        urljoin(broker_url, relay_uri),
        headers={"Authorization": f"Token {token}"},
        stream=True,
        timeout=(10, 300),
    )
    response.raise_for_status()
    started = time.perf_counter()
    with destination.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if chunk:
                handle.write(chunk)
    return time.perf_counter() - started, sha256_file(destination)


def run_agent(
    segment_dir: str | None, segment_count: int, segment_size_kib: int
) -> None:
    if segment_dir:
        directory = Path(segment_dir).expanduser().resolve()
        segments = sorted(directory.glob("*.ts"))
        if not segments:
            raise RuntimeError(f"No .ts segments found in {directory}")
    else:
        directory = (
            Path(tempfile.gettempdir()) / "brokersystem_examples" / "relay_segments"
        )
        segments = create_demo_segments(
            directory,
            segment_count=segment_count,
            segment_size_kib=segment_size_kib,
        )

    print("Serving segments from:", directory)
    print("Segment count:", len(segments))
    build_segment_agent(segments, segment_duration_s=1.0).run()


def run_client(
    agent_id: str, destination_dir: str | None, segment_limit: int | None
) -> None:
    broker = Broker(
        broker_url=require_env("BROKER_URL"), auth=require_env("BROKER_TOKEN")
    )
    destination = (
        Path(destination_dir).expanduser().resolve()
        if destination_dir
        else Path.cwd() / "downloaded_segments"
    )
    destination.mkdir(parents=True, exist_ok=True)

    manifest = broker.ask(
        agent_id,
        {"request_kind": "manifest", "segment_index": 0},
    )["result"]
    segment_count = require_result_int(manifest["segment_count"], field="segment_count")
    segment_duration_s = require_result_float(
        manifest["segment_duration_s"], field="segment_duration_s"
    )
    segment_prefix = require_result_str(
        manifest["segment_prefix"], field="segment_prefix"
    )
    fetch_count = min(segment_count, segment_limit) if segment_limit else segment_count

    print("Manifest:", json.dumps(manifest, indent=2, sort_keys=True))
    started = time.perf_counter()
    downloaded: list[dict[str, object]] = []
    for index in range(fetch_count):
        result = broker.ask(
            agent_id,
            {"request_kind": "segment", "segment_index": index},
        )["result"]
        segment = require_result_dict(result["segment"], field="segment")
        segment_name = require_result_str(result["segment_name"], field="segment_name")
        elapsed, digest = download_segment(
            require_result_str(segment.get("uri"), field="segment.uri"),
            destination / segment_name,
            require_env("BROKER_TOKEN"),
        )
        downloaded.append(
            {
                "index": index,
                "name": segment_name,
                "sha256": digest,
                "elapsed_s": round(elapsed, 3),
            }
        )
        print(
            f"Fetched {segment_prefix}{index:04d} in {elapsed:.3f}s "
            f"(segment duration target {segment_duration_s:.2f}s)"
        )

    total_elapsed = time.perf_counter() - started
    print("Downloaded segments:", json.dumps(downloaded, indent=2, sort_keys=True))
    print("Total elapsed seconds:", round(total_elapsed, 3))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    agent_parser = subparsers.add_parser("agent")
    agent_parser.add_argument(
        "--segment-dir", help="Existing directory containing .ts segments."
    )
    agent_parser.add_argument("--segment-count", type=int, default=5)
    agent_parser.add_argument("--segment-size-kib", type=int, default=256)

    client_parser = subparsers.add_parser("client")
    client_parser.add_argument("--agent-id", required=True)
    client_parser.add_argument(
        "--dest-dir", help="Directory to store downloaded segments."
    )
    client_parser.add_argument(
        "--segment-limit",
        type=int,
        help="Fetch only the first N segments for a quick smoke run.",
    )

    args = parser.parse_args()
    if args.mode == "agent":
        run_agent(args.segment_dir, args.segment_count, args.segment_size_kib)
    else:
        run_client(args.agent_id, args.dest_dir, args.segment_limit)


if __name__ == "__main__":
    main()
