import pandas as pd
import pytest

from brokersystem.agent import (
    Bool,
    Choice,
    File,
    Number,
    RelayAssetSource,
    RelayFile,
    RelayMedia,
    RelaySession,
    RelaySessionSource,
    RelayStreamSource,
    String,
    Table,
    ValueTemplate,
)


def test_guess_from_value_number() -> None:
    template = ValueTemplate.guess_from_value(3)
    assert isinstance(template, Number)


def test_guess_from_value_bool() -> None:
    template = ValueTemplate.guess_from_value(True)
    assert isinstance(template, Bool)


def test_guess_from_value_string() -> None:
    template = ValueTemplate.guess_from_value("hello")
    assert isinstance(template, String)


def test_guess_from_value_choice() -> None:
    template = ValueTemplate.guess_from_value(["a", "b"])
    assert isinstance(template, Choice)
    assert template.constraint_dict["choices"] == ["a", "b"]


def test_guess_from_value_table_from_dataframe() -> None:
    df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
    template = ValueTemplate.guess_from_value(df)
    assert isinstance(template, Table)
    assert template.format_dict["@repr"]["type"] == "graph"


def test_from_dict_builds_bool_template() -> None:
    template = ValueTemplate.from_dict(
        {
            "@type": "bool",
            "@unit": None,
            "@help": "Enable the optional path.",
            "@constraints": {"default": True},
        },
        {},
    )
    assert isinstance(template, Bool)
    assert template.constraint_dict["default"] is True
    assert template.help == "Enable the optional path."


def test_table_format_for_output_from_list_of_dict() -> None:
    table = Table(unit_dict={"x": "m", "y": "s"})
    value, fmt = table.format_for_output([{"x": 1, "y": 2}], lambda *_: {})
    assert value == {"x": [1], "y": [2]}
    assert fmt["@table"] == ["x", "y"]


def test_table_format_for_output_preserves_missing_cells_as_null() -> None:
    table = Table(unit_dict={"a": None, "b": None})
    value, _fmt = table.format_for_output(
        [{"a": 1, "b": None}, {"a": 2}],
        lambda *_: {},
    )
    assert value == {"a": [1, 2], "b": [None, None]}


def test_table_format_for_output_normalizes_dataframe_nulls() -> None:
    table = Table(unit_dict={"a": None, "b": None})
    frame = pd.DataFrame([{"a": 1, "b": None}, {"a": 2}])
    value, _fmt = table.format_for_output(frame, lambda *_: {})
    assert value == {"a": [1, 2], "b": [None, None]}


def test_table_format_for_output_normalizes_dict_of_lists_nulls() -> None:
    table = Table(unit_dict={"a": None, "b": None})
    value, _fmt = table.format_for_output(
        {"a": [1, 2], "b": [None, float("nan")]},
        lambda *_: {},
    )
    assert value == {"a": [1, 2], "b": [None, None]}


def test_file_format_for_output_uploads_bytes() -> None:
    uploader_calls = []

    def fake_uploader(file_type: str, data: bytes):
        uploader_calls.append((file_type, data))
        return {"file_id": "file-123.png"}

    file_template = File("png", help="Rendered preview image.")
    value, fmt = file_template.format_for_output(b"data", fake_uploader)

    assert uploader_calls == [("png", b"data")]
    assert value == "file-123.png"
    assert fmt["@type"] == "image"
    assert fmt["@help"] == "Rendered preview image."


def test_relay_file_format_for_output_registers_source(tmp_path) -> None:
    relay_path = tmp_path / "all_png.zip"
    relay_path.write_bytes(b"data")
    calls = []

    def fake_registrar(path, name, content_type):
        calls.append((path, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_123",
            "runtime_instance_id": "runtime-1",
            "name": "all_png.zip",
            "size_bytes": 4,
            "content_type": "application/zip",
            "availability": "agent_online_required",
        }

    relay_template = RelayFile(help="Large archive kept on the producer host.")
    value, fmt = relay_template.format_for_output(
        relay_path, lambda *_: {}, fake_registrar
    )

    assert calls == [(relay_path, None, None)]
    assert value["source_id"] == "src_123"
    assert value["runtime_instance_id"] == "runtime-1"
    assert fmt["@type"] == "relay_file"
    assert fmt["@help"] == "Large archive kept on the producer host."


def test_relay_media_format_for_output_registers_source(tmp_path) -> None:
    relay_path = tmp_path / "preview.webm"
    relay_path.write_bytes(b"media")
    calls = []

    def fake_registrar(path, name, content_type):
        calls.append((path, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_media",
            "runtime_instance_id": "runtime-media",
            "name": "preview.webm",
            "size_bytes": 5,
            "content_type": "video/webm",
            "availability": "agent_online_required",
        }

    relay_template = RelayMedia(content_type="video/webm", help="Playable relay media.")
    value, fmt = relay_template.format_for_output(
        relay_path, lambda *_: {}, fake_registrar
    )

    assert calls == [(relay_path, None, "video/webm")]
    assert value["source_id"] == "src_media"
    assert value["live"] is False
    assert value["$brokersystem"]["kind"] == "relay_media"
    assert fmt["@type"] == "relay_media"
    assert fmt["@help"] == "Playable relay media."


def test_live_relay_media_format_for_output_registers_stream_source() -> None:
    calls = []

    def fake_registrar(value, name, content_type):
        calls.append((value, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_live",
            "runtime_instance_id": "runtime-live",
            "name": "preview.mjpeg",
            "size_bytes": 0,
            "content_type": "multipart/x-mixed-replace; boundary=frame",
            "availability": "agent_online_required",
        }

    relay_template = RelayMedia(
        content_type="multipart/x-mixed-replace; boundary=frame",
        live=True,
        help="Live relay media.",
    )
    source = RelayStreamSource(open_chunks=lambda _cancel: [b"frame"])
    value, fmt = relay_template.format_for_output(source, lambda *_: {}, fake_registrar)

    assert calls == [(source, None, "multipart/x-mixed-replace; boundary=frame")]
    assert value["source_id"] == "src_live"
    assert value["live"] is True
    assert value["$brokersystem"]["kind"] == "relay_media"
    assert fmt["@type"] == "relay_media"


def test_live_relay_media_format_for_output_registers_asset_source(tmp_path) -> None:
    calls = []

    def fake_registrar(value, name, content_type):
        calls.append((value, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_hls",
            "runtime_instance_id": "runtime-hls",
            "name": "preview.m3u8",
            "size_bytes": 18,
            "content_type": "application/vnd.apple.mpegurl",
            "availability": "agent_online_required",
            "entry_path": "index.m3u8",
        }

    asset_dir = tmp_path / "hls"
    asset_dir.mkdir()
    (asset_dir / "index.m3u8").write_text("#EXTM3U\nsegment000.ts\n", encoding="utf-8")
    source = RelayAssetSource(root_dir=asset_dir, entry_path="index.m3u8")

    relay_template = RelayMedia(
        content_type="application/vnd.apple.mpegurl",
        live=True,
        help="Live HLS relay media.",
    )
    value, fmt = relay_template.format_for_output(source, lambda *_: {}, fake_registrar)

    assert calls == [(source, None, "application/vnd.apple.mpegurl")]
    assert value["source_id"] == "src_hls"
    assert value["entry_path"] == "index.m3u8"
    assert value["live"] is True
    assert value["$brokersystem"]["kind"] == "relay_media"
    assert fmt["@type"] == "relay_media"


def test_relay_session_format_for_output_registers_source() -> None:
    calls = []

    def fake_registrar(value, name, protocol):
        calls.append((value, name, protocol))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_session",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_session",
            "runtime_instance_id": "runtime-session",
            "name": "python-repl",
            "protocol": "text",
            "availability": "agent_online_required",
        }

    relay_template = RelaySession(name="python-repl", help="Interactive relay session.")
    source = RelaySessionSource(
        open_session=lambda _cancel, _emit_text, _close_session: None
    )
    value, fmt = relay_template.format_for_output(source, lambda *_: {}, fake_registrar)

    assert calls == [(source, "python-repl", "text")]
    assert value["source_id"] == "src_session"
    assert value["$brokersystem"]["kind"] == "relay_session"
    assert fmt["@type"] == "relay_session"


def test_number_cast_preserves_float() -> None:
    template = Number(min=0, max=1)
    assert template.cast(0.5) == 0.5


def test_number_cast_parses_numeric_strings() -> None:
    template = Number(min=0, max=10)
    assert template.cast("2") == 2
    assert template.cast("0.5") == 0.5


def test_number_cast_rejects_bool() -> None:
    template = Number(min=0, max=10)
    with pytest.raises(TypeError):
        template.cast(True)


def test_choice_cast_preserves_float() -> None:
    template = Choice([0.5, 1.5])
    assert template.cast(0.5) == 0.5


def test_choice_cast_keeps_string_when_choices_are_strings() -> None:
    template = Choice(["1", "2"])
    assert template.cast("1") == "1"


def test_bool_cast_parses_string_literals() -> None:
    template = Bool(value=False)
    assert template.cast(True) is True
    assert template.cast("true") is True
    assert template.cast("false") is False


def test_bool_cast_rejects_non_bool_values() -> None:
    template = Bool(value=False)
    with pytest.raises(TypeError):
        template.cast(1)


def test_number_format_dict_includes_help() -> None:
    template = Number(value=1, help="Beam diameter in micrometers.")
    template.set_item_type("input")
    assert template.format_dict["@help"] == "Beam diameter in micrometers."


def test_choice_init_rejects_empty_choices() -> None:
    with pytest.raises(ValueError):
        Choice([])


def test_number_init_rejects_invalid_step() -> None:
    with pytest.raises(TypeError):
        Number(step=True)
    with pytest.raises(TypeError):
        Number(step="0.1")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        Number(step=0)
    with pytest.raises(ValueError):
        Number(step=-1)
