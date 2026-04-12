from typing import Any, cast

import pytest

from brokersystem.agent import (
    Agent,
    AgentInterface,
    Bool,
    File,
    Number,
    RelayAssetSource,
    RelayFile,
    RelayMedia,
    RelayMediaProfile,
    RelayStreamSource,
    Table,
    UserInfoField,
    need_revision_response,
    ng_response,
    ok_response,
)


def test_validate_returns_ok_for_valid_input() -> None:
    interface = AgentInterface()
    interface.input.value = Number(value=2, min=1, max=3)
    msg, template = interface.validate({"value": 2})
    assert msg == "ok"
    assert template["value"] == 2


def test_validate_returns_need_revision_for_invalid_input() -> None:
    interface = AgentInterface()
    interface.input.value = Number(value=2, min=1, max=3)
    msg, template = interface.validate({"value": 999})
    assert msg == "need_revision"
    assert "value" not in template or template["value"] != 999
    assert interface.last_feedback is not None
    assert interface.last_feedback["kind"] == "validation"
    assert interface.last_feedback["message"] == (
        "Some submitted values did not satisfy the declared input constraints."
    )
    assert interface.last_feedback["fields"] == {
        "value": "This value does not satisfy the declared input constraints."
    }


def test_validate_returns_need_revision_for_missing_input() -> None:
    interface = AgentInterface()
    interface.input.value = Number(value=2, min=1, max=3)
    msg, template = interface.validate({})
    assert msg == "need_revision"
    assert "value" not in template
    assert interface.last_feedback is not None
    assert interface.last_feedback["kind"] == "validation"
    assert interface.last_feedback["fields"] == {
        "value": "This required parameter is missing."
    }


def test_need_revision_response_adds_global_and_field_messages() -> None:
    msg, payload = need_revision_response(
        message="Please review the highlighted fields.",
        fields={"value": "Enter a smaller value."},
    )

    assert msg == "need_revision"
    assert payload["kind"] == "agent"
    assert payload["message"] == "Please review the highlighted fields."
    assert payload["fields"] == {"value": "Enter a smaller value."}


def test_ok_and_ng_response_support_feedback() -> None:
    ok_msg, ok_payload = ok_response(message="Accepted near the upper bound.")
    ng_msg, ng_payload = ng_response(
        fields={"mode": "This account is not allowed to use fast mode."}
    )

    assert ok_msg == "ok"
    assert ok_payload["kind"] == "agent"
    assert ok_payload["message"] == "Accepted near the upper bound."
    assert ng_msg == "ng"
    assert ng_payload["kind"] == "agent"
    assert ng_payload["fields"] == {
        "mode": "This account is not allowed to use fast mode."
    }


def test_validate_returns_ok_for_valid_bool_input() -> None:
    interface = AgentInterface()
    interface.input.enabled = Bool(value=False)
    msg, template = interface.validate({"enabled": True})
    assert msg == "ok"
    assert template["enabled"] is True


def test_user_info_request_rejects_invalid_fields() -> None:
    agent = Agent("https://example.test")
    with pytest.raises(ValueError):
        agent.user_info_request = [
            UserInfoField.USER_ID,
            cast(UserInfoField, "unknown"),
        ]


def test_format_for_output_includes_tables_and_files() -> None:
    interface = AgentInterface()
    interface.output.score = Number(unit="pt")
    interface.output.table = Table(unit_dict={"x": "m", "y": "s"})
    interface.output.image = File("png")

    upload_calls = []

    def fake_uploader(file_type: str, data: bytes):
        upload_calls.append((file_type, data))
        return {"file_id": "file-999.png"}

    result = interface.format_for_output(
        {"score": 10, "table": [{"x": 1, "y": 2}], "image": b"data"},
        fake_uploader,
    )

    assert result["score"] == 10
    assert result["table"] == {"x": [1], "y": [2]}
    assert result["image"] == "file-999.png"
    assert "table" in result["@keys"]
    assert "image" in result["@keys"]
    assert upload_calls == [("png", b"data")]


def test_format_for_output_registers_relay_files_without_upload(tmp_path) -> None:
    interface = AgentInterface()
    interface.output.archive = RelayFile(
        help="Large archive streamed through the broker."
    )

    relay_path = tmp_path / "archive.bin"
    relay_path.write_bytes(b"relay-bytes")

    upload_calls = []
    relay_calls = []

    def fake_uploader(file_type: str, data: bytes):
        upload_calls.append((file_type, data))
        return {"file_id": "unexpected"}

    def fake_relay_registrar(path, name, content_type):
        relay_calls.append((path, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_456",
            "runtime_instance_id": "runtime-2",
            "name": "archive.bin",
            "size_bytes": 11,
            "content_type": "application/octet-stream",
            "availability": "agent_online_required",
        }

    result = interface.format_for_output(
        {"archive": relay_path},
        fake_uploader,
        fake_relay_registrar,
    )

    assert upload_calls == []
    assert relay_calls == [(relay_path, None, None)]
    assert result["archive"]["source_id"] == "src_456"
    assert result["archive"]["runtime_instance_id"] == "runtime-2"
    assert result["@type"]["archive"] == "relay_file"


def test_format_for_output_registers_relay_media_without_upload(tmp_path) -> None:
    interface = AgentInterface()
    interface.output.preview = RelayMedia(
        content_type="video/webm", help="Preview video relayed through the broker."
    )

    relay_path = tmp_path / "preview.webm"
    relay_path.write_bytes(b"relay-media")

    upload_calls = []
    relay_calls = []

    def fake_uploader(file_type: str, data: bytes):
        upload_calls.append((file_type, data))
        return {"file_id": "unexpected"}

    def fake_relay_registrar(path, name, content_type):
        relay_calls.append((path, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_media",
            "runtime_instance_id": "runtime-media",
            "name": "preview.webm",
            "size_bytes": 11,
            "content_type": "video/webm",
            "availability": "agent_online_required",
        }

    result = interface.format_for_output(
        {"preview": relay_path},
        fake_uploader,
        fake_relay_registrar,
    )

    assert upload_calls == []
    assert relay_calls == [(relay_path, None, "video/webm")]
    assert result["preview"]["source_id"] == "src_media"
    assert result["preview"]["$brokersystem"]["kind"] == "relay_media"
    assert result["preview"]["live"] is False
    assert result["@type"]["preview"] == "relay_media"


def test_format_for_output_registers_live_relay_media_without_upload() -> None:
    interface = AgentInterface()
    interface.output.preview = RelayMedia(
        content_type="multipart/x-mixed-replace; boundary=frame",
        live=True,
        help="Live preview stream relayed through the broker.",
    )

    upload_calls = []
    relay_calls = []

    def fake_uploader(file_type: str, data: bytes):
        upload_calls.append((file_type, data))
        return {"file_id": "unexpected"}

    def fake_relay_registrar(source, name, content_type):
        relay_calls.append((source, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_live_media",
            "runtime_instance_id": "runtime-live-media",
            "name": "preview.mjpeg",
            "size_bytes": 0,
            "content_type": "multipart/x-mixed-replace; boundary=frame",
            "availability": "agent_online_required",
        }

    source = RelayStreamSource(open_chunks=lambda _cancel: [b"frame"])

    result = interface.format_for_output(
        {"preview": source},
        fake_uploader,
        fake_relay_registrar,
    )

    assert upload_calls == []
    assert relay_calls == [(source, None, "multipart/x-mixed-replace; boundary=frame")]
    assert result["preview"]["source_id"] == "src_live_media"
    assert result["preview"]["$brokersystem"]["kind"] == "relay_media"
    assert result["preview"]["live"] is True
    assert result["@type"]["preview"] == "relay_media"


def test_format_for_output_registers_live_asset_tree_media_without_upload(
    tmp_path,
) -> None:
    interface = AgentInterface()
    interface.output.preview = RelayMedia(
        content_type="application/vnd.apple.mpegurl",
        live=True,
        help="Live HLS preview relayed through the broker.",
    )

    asset_dir = tmp_path / "hls"
    asset_dir.mkdir()
    (asset_dir / "index.m3u8").write_text("#EXTM3U\nsegment000.ts\n", encoding="utf-8")
    (asset_dir / "segment000.ts").write_bytes(b"segment")

    upload_calls = []
    relay_calls = []

    def fake_uploader(file_type: str, data: bytes):
        upload_calls.append((file_type, data))
        return {"file_id": "unexpected"}

    def fake_relay_registrar(source, name, content_type):
        relay_calls.append((source, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_live_hls",
            "runtime_instance_id": "runtime-live-hls",
            "name": "preview.m3u8",
            "size_bytes": 0,
            "content_type": "application/vnd.apple.mpegurl",
            "availability": "agent_online_required",
            "entry_path": "index.m3u8",
        }

    source = RelayAssetSource(root_dir=asset_dir, entry_path="index.m3u8")

    result = interface.format_for_output(
        {"preview": source},
        fake_uploader,
        fake_relay_registrar,
    )

    assert upload_calls == []
    assert relay_calls == [(source, None, "application/vnd.apple.mpegurl")]
    assert result["preview"]["source_id"] == "src_live_hls"
    assert result["preview"]["entry_path"] == "index.m3u8"
    assert result["preview"]["live"] is True


def test_format_for_output_registers_live_adaptive_media_profiles(tmp_path) -> None:
    interface = AgentInterface()
    interface.output.preview = RelayMedia(
        live=True,
        profiles={
            "dash": RelayMediaProfile(
                entry_path="stream.mpd",
                content_type="application/dash+xml",
            ),
            "hls": RelayMediaProfile(
                entry_path="stream.m3u8",
                content_type="application/vnd.apple.mpegurl",
            ),
        },
        default_profile="hls",
        help="Adaptive live preview relayed through the broker.",
    )

    asset_dir = tmp_path / "adaptive"
    asset_dir.mkdir()
    (asset_dir / "stream.mpd").write_text("<MPD />", encoding="utf-8")
    (asset_dir / "stream.m3u8").write_text("#EXTM3U\n", encoding="utf-8")

    upload_calls = []
    relay_calls = []

    def fake_uploader(file_type: str, data: bytes):
        upload_calls.append((file_type, data))
        return {"file_id": "unexpected"}

    def fake_relay_registrar(source, name, content_type):
        relay_calls.append((source, name, content_type))
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": "src_live_adaptive",
            "runtime_instance_id": "runtime-live-adaptive",
            "name": "preview",
            "size_bytes": 0,
            "content_type": "application/vnd.apple.mpegurl",
            "availability": "agent_online_required",
            "entry_path": "stream.m3u8",
        }

    source = RelayAssetSource(root_dir=asset_dir, entry_path="stream.m3u8")

    result = interface.format_for_output(
        {"preview": source},
        fake_uploader,
        fake_relay_registrar,
    )

    assert upload_calls == []
    assert relay_calls == [(source, None, "application/vnd.apple.mpegurl")]
    assert result["preview"]["default_profile"] == "hls"
    assert result["preview"]["profiles"]["dash"]["entry_path"] == "stream.mpd"
    assert (
        result["preview"]["profiles"]["hls"]["content_type"]
        == "application/vnd.apple.mpegurl"
    )


def test_make_config_includes_user_info_request() -> None:
    interface = AgentInterface()
    interface.user_info_request = [UserInfoField.EMAIL, UserInfoField.USER_ID]

    config = interface.make_config()

    assert config["user_info_request"] == ["email", "user_id"]


def test_make_config_includes_number_step_constraint() -> None:
    interface = AgentInterface()
    interface.input.value = Number(value=2, min=1, max=3, step=0.1)

    config = interface.make_config()

    assert config["input"]["@constraints"]["value"]["step"] == 0.1


def test_make_config_includes_bool_default() -> None:
    interface = AgentInterface()
    interface.input.enabled = Bool(value=True)

    config = interface.make_config()

    assert config["input"]["@type"]["enabled"] == "bool"
    assert config["input"]["@constraints"]["enabled"]["default"] is True


def test_make_config_includes_help_text() -> None:
    interface = AgentInterface()
    interface.input.value = Number(value=2, help="Requested pulse count.")
    interface.output.image = File("png", help="Preview image.")

    config = interface.make_config()

    assert config["input"]["@help"]["value"] == "Requested pulse count."
    assert config["output"]["@help"]["image"] == "Preview image."


def test_make_config_preserves_input_declaration_order() -> None:
    interface = AgentInterface()
    interface.input.alpha = Number(value=1)
    interface.input.beta = Bool(value=False)
    interface.input.gamma = File("txt")

    config = interface.make_config()

    assert config["input"]["@value"] == ["alpha", "beta", "gamma"]
    assert config["input"]["@keys"] == ["alpha", "beta", "gamma"]


def test_make_config_preserves_output_declaration_order_with_tables_last() -> None:
    interface = AgentInterface()
    interface.output.score = Number()
    interface.output.image = File("png")
    interface.output.series = Table(unit_dict={"x": "m", "y": "s"})

    config = interface.make_config()

    assert config["output"]["@value"] == ["score", "image"]
    assert config["output"]["@keys"] == ["score", "image", "series"]
    assert config["output"]["@table"]["series"] == ["x", "y"]


def test_make_config_includes_ui_preview_when_set() -> None:
    interface = AgentInterface()
    interface.ui_preview = {
        "type": "vega",
        "spec": {"$schema": "https://vega.github.io/schema/vega/v6.json", "marks": []},
    }

    config = interface.make_config()
    assert config["ui_preview"]["type"] == "vega"


def test_make_config_rejects_non_dict_ui_preview() -> None:
    interface = AgentInterface()
    interface.ui_preview = cast(Any, 123)
    with pytest.raises(TypeError):
        interface.make_config()


def test_agent_ui_preview_rejects_non_mapping() -> None:
    agent = Agent("https://example.test")
    with pytest.raises(TypeError):
        agent.ui_preview = cast(Any, 123)


def test_agent_auth_alias_round_trips_with_legacy_secret_token() -> None:
    agent = Agent("https://example.test")

    agent.agent_auth = "agent-1:secret-1"
    assert agent.secret_token == "agent-1:secret-1"

    agent.secret_token = "agent-2:secret-2"
    assert agent.agent_auth == "agent-2:secret-2"


def test_add_config_rejects_mismatched_agent_auth_aliases() -> None:
    with pytest.raises(ValueError):
        Agent.add_config(
            broker_url="https://example.test",
            agent_auth="agent-1:secret-1",
            secret_token="agent-1:secret-2",
        )
