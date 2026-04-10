"""Webcam live relay video example for brokersystem.

This example captures a live video source, packages it as HLS, and returns a
live `RelayMedia` handle that works in both the UI and SDK clients.

Run on a Windows host with OBS Virtual Camera:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    AGENT_NAME='relay-video-webcam-desktop-a' \
    VIDEO_SOURCE_KIND=dshow VIDEO_DEVICE='OBS Virtual Camera' \
    python examples/relay_video_webcam.py agent

Run on a Windows host with OBS Virtual Camera using NVIDIA NVENC:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    AGENT_NAME='relay-video-webcam-desktop-a' \
    VIDEO_SOURCE_KIND=dshow VIDEO_DEVICE='OBS Virtual Camera' \
    VIDEO_ENCODER=h264_nvenc VIDEO_NVENC_PRESET=p5 VIDEO_NVENC_CQ=28 \
    python examples/relay_video_webcam.py agent

Run on a Windows host with a physical webcam and its microphone:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    AGENT_NAME='relay-video-webcam-desktop-a' \
    VIDEO_SOURCE_KIND=dshow VIDEO_DEVICE='ELECOM 2MP Webcam' \
    AUDIO_DEVICE='マイク (2- Webcam internal mic)' \
    python examples/relay_video_webcam.py agent

Run on a Linux host with a V4L2 device:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    AGENT_NAME='relay-video-webcam-linux-a' \
    VIDEO_SOURCE_KIND=v4l2 VIDEO_DEVICE=/dev/video2 \
    python examples/relay_video_webcam.py agent

Run with a generated test pattern when no camera is available:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    AGENT_NAME='relay-video-webcam-testsrc' \
    VIDEO_SOURCE_KIND=testsrc python examples/relay_video_webcam.py agent

Inspect from the SDK client:
  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/relay_video_webcam.py client --agent-id <agent_id>
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Literal, Mapping

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
DEFAULT_SEGMENT_SECONDS = 0.5
DEFAULT_WIDTH = 1280
DEFAULT_HEIGHT = 720
DEFAULT_FPS = 5
DEFAULT_VIDEO_ENCODER = "libx264"
DEFAULT_VIDEO_NVENC_PRESET = "p5"
DEFAULT_VIDEO_NVENC_CQ = "28"


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


def resolve_ffmpeg_bin() -> str:
    ffmpeg_bin = os.environ.get("FFMPEG_BIN")
    if ffmpeg_bin:
        return ffmpeg_bin

    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg:
        return system_ffmpeg

    try:
        import imageio_ffmpeg  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "ffmpeg is required. Install ffmpeg on the host, or install "
            "`imageio-ffmpeg` in the Python environment."
        ) from exc

    return imageio_ffmpeg.get_ffmpeg_exe()


VideoSourceKind = Literal["dshow", "v4l2", "testsrc"]
VideoEncoder = Literal["libx264", "h264_nvenc"]


def resolve_video_encoder() -> VideoEncoder:
    configured = os.environ.get("VIDEO_ENCODER", DEFAULT_VIDEO_ENCODER)
    if configured == "libx264":
        return "libx264"
    if configured == "h264_nvenc":
        return "h264_nvenc"
    raise RuntimeError("VIDEO_ENCODER must be either 'libx264' or 'h264_nvenc'.")


def resolve_nvenc_preset() -> str:
    return os.environ.get("VIDEO_NVENC_PRESET", DEFAULT_VIDEO_NVENC_PRESET)


def resolve_nvenc_cq() -> str:
    return os.environ.get("VIDEO_NVENC_CQ", DEFAULT_VIDEO_NVENC_CQ)


def build_video_codec_args(*, encoder: VideoEncoder, fps: int) -> list[str]:
    gop = str(max(fps, 1))
    if encoder == "libx264":
        return [
            "-c:v",
            "libx264",
            "-preset",
            "ultrafast",
            "-tune",
            "zerolatency",
            "-pix_fmt",
            "yuv420p",
            "-g",
            gop,
            "-keyint_min",
            gop,
            "-sc_threshold",
            "0",
        ]

    return [
        "-c:v",
        "h264_nvenc",
        "-preset",
        resolve_nvenc_preset(),
        "-tune",
        "ll",
        "-rc:v",
        "vbr",
        "-cq:v",
        resolve_nvenc_cq(),
        "-b:v",
        "0",
        "-bf:v",
        "0",
        "-pix_fmt",
        "yuv420p",
        "-g",
        gop,
        "-keyint_min",
        gop,
        "-sc_threshold",
        "0",
    ]


def build_ffmpeg_hls_command(
    *,
    ffmpeg_bin: str,
    source_kind: VideoSourceKind,
    source: str,
    audio_source: str | None,
    video_encoder: VideoEncoder,
    output_dir: Path,
    width: int,
    height: int,
    fps: int,
    list_size: int = HLS_WINDOW_SIZE,
    segment_seconds: float = DEFAULT_SEGMENT_SECONDS,
) -> list[str]:
    input_args: list[str]
    output_filter_args: list[str] = []
    audio_args: list[str]
    if source_kind == "dshow":
        dshow_input = f"video={source}"
        if audio_source:
            dshow_input += f":audio={audio_source}"
        input_args = [
            "-rtbufsize",
            "256M",
            "-f",
            "dshow",
            "-i",
            dshow_input,
        ]
        # Many DirectShow virtual cameras expose only a fixed native mode.
        # Capture that native stream first, then scale/throttle the encoded HLS output.
        output_filter_args = [
            "-vf",
            f"fps={max(fps, 1)},scale={width}:{height}",
        ]
    elif source_kind == "v4l2":
        input_args = [
            "-f",
            "v4l2",
            "-framerate",
            str(fps),
            "-video_size",
            f"{width}x{height}",
            "-i",
            source,
        ]
    else:
        input_args = [
            "-f",
            "lavfi",
            "-i",
            f"{source}=size={width}x{height}:rate={fps}",
        ]

    if audio_source is None:
        audio_args = ["-an"]
    else:
        audio_args = [
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-ar",
            "48000",
            "-ac",
            "2",
        ]

    return [
        ffmpeg_bin,
        "-hide_banner",
        "-loglevel",
        "warning",
        "-nostdin",
        *input_args,
        *output_filter_args,
        *audio_args,
        *build_video_codec_args(encoder=video_encoder, fps=fps),
        "-f",
        "hls",
        "-hls_time",
        str(segment_seconds),
        "-hls_list_size",
        str(list_size),
        "-hls_flags",
        "delete_segments+append_list+omit_endlist+independent_segments",
        "-hls_segment_filename",
        str(output_dir / "segment%06d.ts"),
        str(output_dir / HLS_ENTRY_PATH),
    ]


class WebcamHlsCapture:
    def __init__(
        self,
        *,
        source_kind: VideoSourceKind,
        source: str,
        audio_source: str | None,
        video_encoder: VideoEncoder,
        width: int,
        height: int,
        fps: int,
        output_dir: Path,
    ) -> None:
        self.source_kind: VideoSourceKind = source_kind
        self.source = source
        self.audio_source = audio_source
        self.video_encoder: VideoEncoder = video_encoder
        self.width = width
        self.height = height
        self.fps = fps
        self.window_size = HLS_WINDOW_SIZE
        self.segment_seconds = DEFAULT_SEGMENT_SECONDS
        self.output_dir = output_dir
        self.ffmpeg_bin = resolve_ffmpeg_bin()
        self._lock = threading.Lock()
        self._process: subprocess.Popen[bytes] | None = None
        self._log_path = output_dir / "ffmpeg.log"

    def ensure_running(self) -> RelayAssetSource:
        with self._lock:
            if self._process is not None and self._process.poll() is None:
                return RelayAssetSource(
                    root_dir=self.output_dir, entry_path=HLS_ENTRY_PATH
                )

            self._start_locked()
            return RelayAssetSource(root_dir=self.output_dir, entry_path=HLS_ENTRY_PATH)

    def close(self) -> None:
        with self._lock:
            self._stop_locked()

    def _start_locked(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for path in self.output_dir.glob("*"):
            if path.is_file():
                path.unlink()

        command = build_ffmpeg_hls_command(
            ffmpeg_bin=self.ffmpeg_bin,
            source_kind=self.source_kind,
            source=self.source,
            audio_source=self.audio_source,
            video_encoder=self.video_encoder,
            output_dir=self.output_dir,
            width=self.width,
            height=self.height,
            fps=self.fps,
            list_size=self.window_size,
            segment_seconds=self.segment_seconds,
        )

        log_handle = self._log_path.open("wb")
        try:
            self._process = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=log_handle,
            )
        finally:
            log_handle.close()

        self._wait_until_ready_locked()

    def _wait_until_ready_locked(self) -> None:
        playlist_path = self.output_dir / HLS_ENTRY_PATH
        deadline = time.monotonic() + 15.0

        while time.monotonic() < deadline:
            if self._process is None:
                break
            exit_code = self._process.poll()
            if exit_code is not None:
                raise RuntimeError(self._startup_error_message(exit_code))

            if playlist_path.exists():
                playlist_text = playlist_path.read_text(
                    encoding="utf-8", errors="ignore"
                )
                if "#EXTM3U" in playlist_text and "segment" in playlist_text:
                    return

            time.sleep(0.2)

        raise RuntimeError("Timed out waiting for ffmpeg to produce an HLS playlist.")

    def _startup_error_message(self, exit_code: int) -> str:
        log_tail = ""
        if self._log_path.exists():
            log_tail = self._log_path.read_text(encoding="utf-8", errors="ignore")[
                -1000:
            ]

        source_hint = (
            f"source_kind={self.source_kind} source={self.source} "
            f"audio_source={self.audio_source} video_encoder={self.video_encoder} "
            f"width={self.width} height={self.height} fps={self.fps}"
        )
        return (
            f"ffmpeg exited early with code {exit_code}. {source_hint}\n"
            f"{log_tail}".rstrip()
        )

    def _stop_locked(self) -> None:
        if self._process is None:
            return
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=5)
        self._process = None


def detect_source_kind() -> VideoSourceKind:
    configured = os.environ.get("VIDEO_SOURCE_KIND")
    if configured == "dshow":
        return "dshow"
    if configured == "v4l2":
        return "v4l2"
    if configured == "testsrc":
        return "testsrc"
    if configured is not None:
        raise RuntimeError(
            "VIDEO_SOURCE_KIND must be one of 'dshow', 'v4l2', or 'testsrc'."
        )

    if os.environ.get("VIDEO_SOURCE") == "testsrc":
        return "testsrc"

    if platform.system() == "Windows":
        return "dshow"

    return "v4l2"


def default_source_for(kind: VideoSourceKind) -> str:
    if kind == "dshow":
        return os.environ.get("VIDEO_DEVICE", "OBS Virtual Camera")
    if kind == "v4l2":
        return os.environ.get("VIDEO_DEVICE", "/dev/video0")
    return os.environ.get("VIDEO_TESTSRC_NAME", "testsrc2")


def default_audio_source_for(kind: VideoSourceKind) -> str | None:
    audio_source = os.environ.get("AUDIO_DEVICE")
    if not audio_source:
        return None
    if kind != "dshow":
        raise RuntimeError(
            "AUDIO_DEVICE is currently supported only with VIDEO_SOURCE_KIND=dshow."
        )
    return audio_source


def build_capture() -> WebcamHlsCapture:
    source_kind = detect_source_kind()
    source = default_source_for(source_kind)
    audio_source = default_audio_source_for(source_kind)
    video_encoder = resolve_video_encoder()

    width = int(os.environ.get("VIDEO_WIDTH", str(DEFAULT_WIDTH)))
    height = int(os.environ.get("VIDEO_HEIGHT", str(DEFAULT_HEIGHT)))
    fps = int(os.environ.get("VIDEO_FPS", str(DEFAULT_FPS)))
    segment_seconds = float(
        os.environ.get("VIDEO_SEGMENT_SECONDS", str(DEFAULT_SEGMENT_SECONDS))
    )
    list_size = int(os.environ.get("VIDEO_HLS_WINDOW_SIZE", str(HLS_WINDOW_SIZE)))
    temp_root = Path(tempfile.gettempdir()) / "brokersystem_examples"
    temp_root.mkdir(parents=True, exist_ok=True)
    output_dir = Path(tempfile.mkdtemp(prefix="relay_webcam_hls_", dir=temp_root))

    capture = WebcamHlsCapture(
        source_kind=source_kind,
        source=source,
        audio_source=audio_source,
        video_encoder=video_encoder,
        width=width,
        height=height,
        fps=fps,
        output_dir=output_dir,
    )
    capture.segment_seconds = segment_seconds
    capture.window_size = list_size
    return capture


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
    agent_name = os.environ.get("AGENT_NAME", "relay-video-webcam-example-sdk")
    capture = build_capture()

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = agent_name
        agent.agent_auth = agent_auth
        agent.description = (
            "Captures a live camera feed, packages it as HLS, and relays it "
            "through the broker without permanently storing the media bytes."
        )
        agent.charge = 1
        agent.output.preview = RelayMedia(
            name="webcam-live.m3u8",
            content_type=HLS_CONTENT_TYPE,
            live=True,
            help="Live HLS camera preview relayed from the agent host.",
        )
        agent.output.source = String(help="Capture source descriptor.")
        agent.output.content_type = String(help="Media content type.")

    @agent.job_func
    def job(_job: Job) -> dict[str, object]:
        source = capture.ensure_running()
        source_description = (
            capture.source
            if capture.source_kind in {"dshow", "v4l2"}
            else f"generated:{capture.source}:{capture.width}x{capture.height}@{capture.fps}"
        )
        if capture.audio_source:
            source_description = f"{source_description} + audio:{capture.audio_source}"
        return {
            "preview": source,
            "source": source_description,
            "content_type": HLS_CONTENT_TYPE,
        }

    return agent


def run_agent() -> None:
    capture = build_capture()
    source_label = (
        capture.source
        if capture.source_kind in {"dshow", "v4l2"}
        else f"generated {capture.source}"
    )
    print("Serving webcam relay media.")
    print("Source kind:", capture.source_kind)
    print("Source:", source_label)
    if capture.audio_source:
        print("Audio source:", capture.audio_source)
    print("Video encoder:", capture.video_encoder)
    if capture.video_encoder == "h264_nvenc":
        print("NVENC preset:", resolve_nvenc_preset())
        print("NVENC CQ:", resolve_nvenc_cq())
    print("Resolution:", f"{capture.width}x{capture.height}")
    print("FPS:", capture.fps)
    build_agent().run()


def run_client(agent_id: str) -> None:
    broker = Broker(
        broker_url=require_env("BROKER_URL"), auth=require_env("BROKER_TOKEN")
    )
    result_payload = broker.ask(agent_id, {})["result"]
    relay_media = parse_media_result(broker, result_payload)
    playlist_text = read_playlist_snapshot(broker, relay_media)

    print("Playback URI:", relay_media.playback_uri)
    print("Download URI:", relay_media.download_uri)
    print("Name:", relay_media.name)
    print("Live:", relay_media.live)
    print("Playlist preview:")
    print(playlist_text.strip())
    print("Example ffmpeg usage:")
    print(
        "  "
        f'ffmpeg -headers "authorization: Basic {require_env("BROKER_TOKEN")}\\r\\n" '
        f'-i "{broker.broker_url}{relay_media.playback_uri}" -t 5 -c copy relay_webcam_capture.ts'
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
