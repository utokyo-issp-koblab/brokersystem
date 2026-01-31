from typing import cast

import pytest

from brokersystem.agent import Agent, AgentInterface, File, Number, Table, UserInfoField


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
