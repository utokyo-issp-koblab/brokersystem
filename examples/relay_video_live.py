"""Live relay video example for brokersystem.

This example returns a live `RelayMedia` handle backed by an HLS playlist plus
segments that stay on the agent host. The broker relays playlist and segment
requests on demand, so the same handle works in the UI and from the SDK.

Run:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    python examples/relay_video_live.py agent

  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/relay_video_live.py client --agent-id <agent_id>
"""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from brokersystem import (
    Agent,
    Broker,
    Job,
    RelayAssetSource,
    RelayMedia,
    RelayMediaHandle,
    String,
)

HLS_CONTENT_TYPE = "application/vnd.apple.mpegurl"
HLS_ENTRY_PATH = "index.m3u8"
HLS_WINDOW_SIZE = 3


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def parse_media_result(
    broker: Broker, result_payload: Mapping[str, object]
) -> RelayMediaHandle:
    match result_payload:
        case {"preview": dict() as preview_obj}:
            return broker.parse_relay_media(preview_obj)
        case _:
            raise RuntimeError("Unexpected broker result payload.")


@dataclass(frozen=True)
class HlsSeedSegment:
    filename: str
    duration_seconds: float
    path: Path


def load_hls_seed(seed_dir: Path) -> list[HlsSeedSegment]:
    playlist_path = seed_dir / HLS_ENTRY_PATH
    lines = playlist_path.read_text(encoding="utf-8").splitlines()
    segments: list[HlsSeedSegment] = []
    pending_duration: float | None = None

    for line in lines:
        if line.startswith("#EXTINF:"):
            pending_duration = float(line.removeprefix("#EXTINF:").rstrip(","))
            continue
        if line.startswith("#") or line.strip() == "":
            continue
        if pending_duration is None:
            raise RuntimeError(
                "HLS seed playlist is missing EXTINF before a segment line."
            )
        segment_path = seed_dir / line
        if not segment_path.is_file():
            raise RuntimeError(f"HLS seed segment does not exist: {segment_path}")
        segments.append(
            HlsSeedSegment(
                filename=line,
                duration_seconds=pending_duration,
                path=segment_path,
            )
        )
        pending_duration = None

    if not segments:
        raise RuntimeError("HLS seed playlist did not contain any segments.")
    return segments


class LiveHlsPublisher:
    def __init__(
        self,
        seed_dir: Path,
        output_dir: Path,
        *,
        window_size: int = HLS_WINDOW_SIZE,
    ) -> None:
        self.seed_segments = load_hls_seed(seed_dir)
        self.output_dir = output_dir
        self.window_size = window_size
        self._published: list[tuple[int, HlsSeedSegment, str]] = []
        self._next_sequence = 0
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def ensure_running(self) -> RelayAssetSource:
        with self._lock:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            if not self._published:
                for _ in range(min(self.window_size, len(self.seed_segments))):
                    self._publish_next_segment_locked()
            if self._thread is None or not self._thread.is_alive():
                self._stop_event.clear()
                self._thread = threading.Thread(
                    target=self._run,
                    daemon=True,
                    name="relay-video-live-publisher",
                )
                self._thread.start()
        return RelayAssetSource(root_dir=self.output_dir, entry_path=HLS_ENTRY_PATH)

    def close(self) -> None:
        self._stop_event.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop_event.wait(self._next_publish_delay()):
            with self._lock:
                self._publish_next_segment_locked()

    def _next_publish_delay(self) -> float:
        if not self._published:
            return 1.0
        return self._published[-1][1].duration_seconds

    def _publish_next_segment_locked(self) -> None:
        seed = self.seed_segments[self._next_sequence % len(self.seed_segments)]
        sequence = self._next_sequence
        output_name = f"segment{sequence:06d}.ts"
        output_path = self.output_dir / output_name
        shutil.copyfile(seed.path, output_path)
        self._published.append((sequence, seed, output_name))
        self._next_sequence += 1

        while len(self._published) > self.window_size:
            old_sequence, _old_seed, old_name = self._published.pop(0)
            _ = old_sequence
            old_path = self.output_dir / old_name
            if old_path.exists():
                old_path.unlink()

        self._write_playlist_locked()

    def _write_playlist_locked(self) -> None:
        target_duration = max(
            1,
            int(max(seed.duration_seconds for _, seed, _ in self._published)),
        )
        media_sequence = self._published[0][0]
        lines = [
            "#EXTM3U",
            "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{target_duration}",
            f"#EXT-X-MEDIA-SEQUENCE:{media_sequence}",
        ]
        for _sequence, seed, output_name in self._published:
            lines.append(f"#EXTINF:{seed.duration_seconds:.6f},")
            lines.append(output_name)

        playlist_path = self.output_dir / HLS_ENTRY_PATH
        temp_path = playlist_path.with_suffix(".tmp")
        temp_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        temp_path.replace(playlist_path)


def build_live_publisher() -> LiveHlsPublisher:
    seed_dir = Path(__file__).with_name("assets") / "live_hls_seed"
    output_dir = (
        Path(tempfile.gettempdir()) / "brokersystem_examples" / "relay_live_hls"
    )
    return LiveHlsPublisher(seed_dir=seed_dir, output_dir=output_dir)


def read_playlist_snapshot(broker: Broker, relay_media: RelayMediaHandle) -> str:
    response = broker.open_media(relay_media, purpose="playback", stream=False)
    try:
        playlist_text = response.text
    finally:
        response.close()
    return playlist_text


def build_agent() -> Agent:
    broker_url = require_env("BROKER_URL")
    agent_auth = require_env("AGENT_AUTH")
    publisher = build_live_publisher()

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "relay-video-live-example-sdk"
        agent.agent_auth = agent_auth
        agent.description = (
            "Returns a live HLS preview that the broker relays without storing"
            " the media bytes permanently."
        )
        agent.charge = 1
        agent.output.preview = RelayMedia(
            name="live-preview.m3u8",
            content_type=HLS_CONTENT_TYPE,
            live=True,
            help="Live HLS preview relayed from the agent host.",
        )
        agent.output.content_type = String(help="Media content type.")

    @agent.job_func
    def job(_job: Job) -> dict[str, object]:
        return {
            "preview": publisher.ensure_running(),
            "content_type": HLS_CONTENT_TYPE,
        }

    return agent


def run_agent() -> None:
    print("Serving live relay media.")
    print("Content-Type:", HLS_CONTENT_TYPE)
    build_agent().run()


def run_client(agent_id: str) -> None:
    broker = Broker(
        broker_url=require_env("BROKER_URL"), auth=require_env("BROKER_TOKEN")
    )
    result_payload = broker.ask(agent_id, {})["result"]
    relay_media = parse_media_result(broker, result_payload)
    playlist_text = read_playlist_snapshot(broker, relay_media)

    print("Playback URI:", relay_media.playback_uri)
    print("Name:", relay_media.name)
    print("Live:", relay_media.live)
    print("Playlist preview:")
    print(playlist_text.strip())
    print("Example ffmpeg usage:")
    print(
        "  "
        f'ffmpeg -headers "authorization: Basic {require_env("BROKER_TOKEN")}\\r\\n" '
        f'-i "{broker.broker_url}{relay_media.playback_uri}" -t 5 -c copy relay_live_capture.ts'
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    subparsers.add_parser("agent")

    client_parser = subparsers.add_parser("client")
    client_parser.add_argument("--agent-id", required=True)

    args = parser.parse_args()
    if args.mode == "agent":
        run_agent()
    else:
        run_client(args.agent_id)


if __name__ == "__main__":
    main()
