from typing import Any, cast

import pytest

from brokersystem.agent import (
    Agent,
    AgentInterface,
    Bool,
    File,
    Number,
    Table,
    UserInfoField,
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


def test_validate_returns_need_revision_for_missing_input() -> None:
    interface = AgentInterface()
    interface.input.value = Number(value=2, min=1, max=3)
    msg, template = interface.validate({})
    assert msg == "need_revision"
    assert "value" not in template


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
