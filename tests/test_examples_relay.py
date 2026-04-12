from __future__ import annotations

import importlib.util
import sys
import threading
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLES_DIR = REPO_ROOT / "examples"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_relay_file_demo_generator(tmp_path: Path) -> None:
    module = _load_module(
        "relay_file_example",
        EXAMPLES_DIR / "relay_file.py",
    )
    output = module.create_demo_large_file(tmp_path / "demo.bin", 1)
    assert output.exists()
    assert output.stat().st_size == 1024 * 1024
    assert len(module.sha256_file(output)) == 64


def test_relay_video_stored_demo_generator(tmp_path: Path) -> None:
    module = _load_module(
        "relay_video_stored_example",
        EXAMPLES_DIR / "relay_video_stored.py",
    )
    output = module.create_demo_video(tmp_path / "preview.webm")
    assert output.exists()
    assert output.suffix == ".webm"
    assert output.stat().st_size > 1024
    assert len(module.sha256_file(output)) == 64


def test_relay_video_live_demo_generator(tmp_path: Path) -> None:
    module = _load_module(
        "relay_video_live_example",
        EXAMPLES_DIR / "relay_video_live.py",
    )
    publisher = module.LiveHlsPublisher(
        seed_dir=EXAMPLES_DIR / "assets" / "live_hls_seed",
        output_dir=tmp_path / "relay-live-test",
        window_size=3,
    )
    source = publisher.ensure_running()
    playlist_path = Path(source.root_dir) / source.entry_path
    assert playlist_path.exists()
    playlist_text = playlist_path.read_text(encoding="utf-8")
    assert "#EXTM3U" in playlist_text
    assert "segment" in playlist_text
    publisher.close()


def test_relay_repl_demo_session() -> None:
    module = _load_module(
        "relay_repl_example",
        EXAMPLES_DIR / "relay_repl.py",
    )
    emitted: list[str] = []
    closed = {"value": False}
    source = module.build_demo_session()
    handler = source.open_session(
        threading.Event(),
        emitted.append,
        lambda: closed.__setitem__("value", True),
    )
    assert handler is not None
    assert "relay repl demo" in emitted[0]
    handler("echo hello")
    assert emitted[-1] == "hello\nrepl> "
    handler("exit")
    assert closed["value"] is True


def test_relay_video_webcam_builds_dshow_command() -> None:
    module = _load_module(
        "relay_video_webcam_example",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    command = module.build_ffmpeg_hls_command(
        ffmpeg_bin="ffmpeg",
        source_kind="dshow",
        source="OBS Virtual Camera",
        audio_source=None,
        video_encoder="libx264",
        output_dir=Path("/tmp/webcam-hls"),
        width=1280,
        height=720,
        fps=5,
    )
    assert command[:3] == ["ffmpeg", "-hide_banner", "-loglevel"]
    assert "-f" in command
    assert "dshow" in command
    assert "video=OBS Virtual Camera" in command
    assert "-framerate" not in command
    assert "-video_size" not in command
    assert "-an" in command
    assert "-vf" in command
    assert "libx264" in command
    assert "fps=5,scale=1280:720" in command
    assert "-hls_segment_type" in command
    assert "fmp4" in command
    assert "-hls_fmp4_init_filename" in command
    assert module.HLS_INIT_FILENAME in command
    assert "temp_file+delete_segments+omit_endlist+independent_segments" in command
    assert str(Path("/tmp/webcam-hls") / "segment%06d.m4s") in command
    assert str(Path("/tmp/webcam-hls") / "index.m3u8") in command


def test_relay_video_webcam_builds_adaptive_command() -> None:
    module = _load_module(
        "relay_video_webcam_example_adaptive",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    command = module.build_ffmpeg_adaptive_command(
        ffmpeg_bin="ffmpeg",
        source_kind="dshow",
        source="OBS Virtual Camera",
        audio_source=None,
        video_encoder="libx264",
        output_dir=Path("/tmp/webcam-adaptive"),
        width=1280,
        height=720,
        fps=5,
    )
    assert command[:3] == ["ffmpeg", "-hide_banner", "-loglevel"]
    assert "-f" in command
    assert "dash" in command
    assert "-ldash" in command
    assert "-streaming" in command
    assert "-dash_segment_type" in command
    assert "mp4" in command
    assert "-hls_playlist" in command
    assert "-hls_master_name" in command
    assert module.ADAPTIVE_HLS_MASTER_PLAYLIST_PATH in command
    assert "-format_options" in command
    assert "movflags=cmaf" in command
    assert "-media_seg_name" in command
    assert "chunk-$RepresentationID$-$Number%05d$.$ext$" in command
    assert str(Path("/tmp/webcam-adaptive") / module.DASH_ENTRY_PATH) in command


def test_relay_video_webcam_adaptive_entry_path_points_to_media_playlist() -> None:
    module = _load_module(
        "relay_video_webcam_example_entry_path",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    capture = module.WebcamRelayCapture(
        source_kind="testsrc",
        source="testsrc2",
        audio_source=None,
        video_encoder="libx264",
        output_format="adaptive",
        width=320,
        height=180,
        fps=5,
        output_dir=Path("/tmp/webcam-adaptive-entry-path"),
    )
    assert capture.entry_path == module.ADAPTIVE_HLS_MEDIA_PLAYLIST_PATH
    template = capture.preview_template()
    assert template.profiles["hls"].entry_path == module.ADAPTIVE_HLS_MEDIA_PLAYLIST_PATH


def test_relay_video_examples_print_ffmpeg_header_with_real_crlf_shell_syntax() -> None:
    webcam = _load_module(
        "relay_video_webcam_example_ffmpeg_header",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    live = _load_module(
        "relay_video_live_example_ffmpeg_header",
        EXAMPLES_DIR / "relay_video_live.py",
    )
    stored = _load_module(
        "relay_video_stored_example_ffmpeg_header",
        EXAMPLES_DIR / "relay_video_stored.py",
    )

    expected = "$'authorization: Token demo-token\\r\\n'"
    assert webcam.ffmpeg_auth_header_arg("demo-token") == expected
    assert live.ffmpeg_auth_header_arg("demo-token") == expected
    assert stored.ffmpeg_auth_header_arg("demo-token") == expected


def test_relay_video_webcam_builds_dshow_command_with_audio() -> None:
    module = _load_module(
        "relay_video_webcam_example_audio",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    command = module.build_ffmpeg_hls_command(
        ffmpeg_bin="ffmpeg",
        source_kind="dshow",
        source="ELECOM 2MP Webcam",
        audio_source="マイク (2- Webcam internal mic)",
        video_encoder="libx264",
        output_dir=Path("/tmp/webcam-hls"),
        width=1280,
        height=720,
        fps=5,
    )
    assert "video=ELECOM 2MP Webcam:audio=マイク (2- Webcam internal mic)" in command
    assert "-an" not in command
    assert "-c:a" in command
    assert "aac" in command


def test_relay_video_webcam_builds_nvenc_command() -> None:
    module = _load_module(
        "relay_video_webcam_example_nvenc",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    command = module.build_ffmpeg_hls_command(
        ffmpeg_bin="ffmpeg",
        source_kind="dshow",
        source="OBS Virtual Camera",
        audio_source=None,
        video_encoder="h264_nvenc",
        output_dir=Path("/tmp/webcam-hls"),
        width=1280,
        height=720,
        fps=5,
    )
    assert "-c:v" in command
    assert "h264_nvenc" in command
    assert "-preset" in command
    assert module.resolve_nvenc_preset() in command
    assert module.resolve_nvenc_cq() in command
    assert "ll" in command
    assert "fps=5,scale=1280:720" in command


def test_relay_video_webcam_builds_testsrc_realtime_command() -> None:
    module = _load_module(
        "relay_video_webcam_example_testsrc",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    command = module.build_ffmpeg_hls_command(
        ffmpeg_bin="ffmpeg",
        source_kind="testsrc",
        source="testsrc2",
        audio_source=None,
        video_encoder="libx264",
        output_dir=Path("/tmp/webcam-hls"),
        width=640,
        height=360,
        fps=5,
    )
    assert command[:3] == ["ffmpeg", "-hide_banner", "-loglevel"]
    assert "-re" in command
    assert command[command.index("-re") + 1 : command.index("-i")] == ["-f", "lavfi"]


def test_relay_video_webcam_testsrc_detection(monkeypatch) -> None:
    module = _load_module(
        "relay_video_webcam_example_detect",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    monkeypatch.delenv("VIDEO_SOURCE_KIND", raising=False)
    monkeypatch.setenv("VIDEO_SOURCE", "testsrc")
    assert module.detect_source_kind() == "testsrc"


def test_relay_video_webcam_default_encoder(monkeypatch) -> None:
    module = _load_module(
        "relay_video_webcam_example_encoder_defaults",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    monkeypatch.delenv("VIDEO_ENCODER", raising=False)
    assert module.resolve_video_encoder() == "libx264"


def test_relay_video_webcam_default_nvenc_settings(monkeypatch) -> None:
    module = _load_module(
        "relay_video_webcam_example_nvenc_defaults",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    monkeypatch.delenv("VIDEO_NVENC_PRESET", raising=False)
    monkeypatch.delenv("VIDEO_NVENC_CQ", raising=False)
    assert module.resolve_nvenc_preset() == "p5"
    assert module.resolve_nvenc_cq() == "28"


def test_relay_video_webcam_default_windows_source(monkeypatch) -> None:
    module = _load_module(
        "relay_video_webcam_example_defaults",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    monkeypatch.delenv("VIDEO_DEVICE", raising=False)
    assert module.default_source_for("dshow") == "OBS Virtual Camera"


def test_relay_video_webcam_default_output_format(monkeypatch) -> None:
    module = _load_module(
        "relay_video_webcam_example_output_format",
        EXAMPLES_DIR / "relay_video_webcam.py",
    )
    monkeypatch.delenv("VIDEO_OUTPUT_FORMAT", raising=False)
    assert module.resolve_output_format() == "adaptive"
