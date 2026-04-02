from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_relay_large_file_demo_generator(tmp_path: Path) -> None:
    module = _load_module(
        "relay_large_file_example",
        Path("/home/vscode/brokersystem/examples/relay_large_file.py"),
    )
    output = module.create_demo_large_file(tmp_path / "demo.bin", 1)
    assert output.exists()
    assert output.stat().st_size == 1024 * 1024
    assert len(module.sha256_file(output)) == 64


def test_relay_segmented_video_demo_generator(tmp_path: Path) -> None:
    module = _load_module(
        "relay_segmented_video_example",
        Path("/home/vscode/brokersystem/examples/relay_segmented_video.py"),
    )
    segments = module.create_demo_segments(
        tmp_path / "segments", segment_count=3, segment_size_kib=8
    )
    assert [path.name for path in segments] == [
        "segment_0000.ts",
        "segment_0001.ts",
        "segment_0002.ts",
    ]
    assert all(path.exists() for path in segments)
    assert all(path.stat().st_size == 8 * 1024 for path in segments)
