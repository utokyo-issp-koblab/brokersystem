"""Relay media video example for brokersystem.

This example returns a playable broker-relayed video handle from a single
job. The client receives a `playback_uri` and `download_uri`, so both UI and
SDK callers can use the result directly.

Run:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    python examples/relay_video_stored.py agent [--file /path/to/video.webm]

  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/relay_video_stored.py client --agent-id <agent_id>
"""

from __future__ import annotations

import argparse
import hashlib
import mimetypes
import os
import tempfile
import time
from pathlib import Path
from typing import Mapping

from brokersystem import (
    Agent,
    Broker,
    Job,
    Number,
    RelayMedia,
    RelayMediaHandle,
    String,
)


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def ffmpeg_auth_header_arg(token: str) -> str:
    return f"$'authorization: Token {token}\\r\\n'"


def create_demo_video(path: Path) -> Path:
    """Copy the bundled demo video for relay-media examples."""
    path.parent.mkdir(parents=True, exist_ok=True)
    demo_asset = Path(__file__).with_name("assets") / "relay_media_demo.webm"
    path.write_bytes(demo_asset.read_bytes())
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


def read_media_result(
    broker: Broker, result_payload: Mapping[str, object]
) -> RelayMediaHandle:
    match result_payload:
        case {"preview": dict() as preview_obj}:
            return broker.parse_relay_media(preview_obj)
        case _:
            raise RuntimeError("Unexpected broker result payload.")


def guess_content_type(path: Path) -> str:
    guessed, _encoding = mimetypes.guess_type(path.name)
    return guessed or "video/webm"


def build_agent(file_path: Path) -> Agent:
    broker_url = require_env("BROKER_URL")
    agent_auth = require_env("AGENT_AUTH")
    content_type = guess_content_type(file_path)

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "relay-video-stored-example-sdk"
        agent.agent_auth = agent_auth
        agent.description = (
            "Returns a playable video handle that the broker can relay without"
            " storing the media bytes permanently."
        )
        agent.interactive = True
        agent.charge = 60
        agent.output.preview = RelayMedia(
            name=file_path.name,
            content_type=content_type,
            help="Playable video kept on the agent host and relayed on demand.",
        )
        agent.output.size_bytes = Number(unit="B")
        agent.output.content_type = String(help="Media content type.")

    @agent.job_func
    def job(_job: Job) -> dict[str, object]:
        return {
            "preview": file_path,
            "size_bytes": file_path.stat().st_size,
            "content_type": content_type,
        }

    return agent


def run_agent(file_arg: str | None) -> None:
    if file_arg:
        file_path = Path(file_arg).expanduser().resolve()
    else:
        file_path = create_demo_video(
            Path(tempfile.gettempdir())
            / "brokersystem_examples"
            / "relay_preview_demo.webm"
        )
    print("Serving relay media:", file_path)
    print("Bytes:", file_path.stat().st_size)
    print("Content-Type:", guess_content_type(file_path))
    build_agent(file_path).run()


def run_client(agent_id: str, destination: str | None) -> None:
    broker = Broker(
        broker_url=require_env("BROKER_URL"), auth=require_env("BROKER_TOKEN")
    )
    result_payload = broker.ask(agent_id, {}, max_duration_minutes=10)["result"]
    relay_media = read_media_result(broker, result_payload)
    destination_path = (
        Path(destination).expanduser().resolve()
        if destination
        else Path.cwd() / relay_media.name
    )
    started = time.perf_counter()
    broker.download_media(relay_media, destination_path)
    elapsed = time.perf_counter() - started
    digest = sha256_file(destination_path)
    mib_s = (
        (relay_media.size_bytes / (1024 * 1024)) / elapsed
        if elapsed > 0
        else float("inf")
    )

    print("Playback URI:", relay_media.playback_uri)
    print("Download URI:", relay_media.download_uri)
    print("Downloaded to:", destination_path)
    print("SHA256:", digest)
    print("Elapsed seconds:", round(elapsed, 3))
    print("Throughput MiB/s:", round(mib_s, 3))
    print("Example ffmpeg usage:")
    print(
        "  "
        f'-headers {ffmpeg_auth_header_arg(require_env("BROKER_TOKEN"))} '
        f'-i "{broker.broker_url}{relay_media.playback_uri}" -c copy relay_preview_copy.webm'
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    agent_parser = subparsers.add_parser("agent")
    agent_parser.add_argument("--file", help="Existing local video file to relay.")

    client_parser = subparsers.add_parser("client")
    client_parser.add_argument("--agent-id", required=True)
    client_parser.add_argument(
        "--dest", help="Destination path for the downloaded media file."
    )

    args = parser.parse_args()
    if args.mode == "agent":
        run_agent(args.file)
    else:
        run_client(args.agent_id, args.dest)


if __name__ == "__main__":
    main()
