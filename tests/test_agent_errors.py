import json

import pytest
import responses

from brokersystem.agent import (
    Agent,
    AgentHTTPError,
    AgentResponseError,
    AgentUploadError,
)

BROKER_URL = "https://example.test"


def build_agent() -> Agent:
    agent = Agent(BROKER_URL)
    agent.auth = "agent-id:secret"
    agent.access_token = "agent-token"
    return agent


@responses.activate
def test_agent_get_raises_on_http_error() -> None:
    agent = build_agent()
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/agent/msgbox",
        status=500,
        body="oops",
    )

    with pytest.raises(AgentHTTPError):
        agent.get("msgbox", basic_auth=True)


@responses.activate
def test_agent_get_raises_on_non_json() -> None:
    agent = build_agent()
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/agent/msgbox",
        status=200,
        body="not-json",
        content_type="text/plain",
    )

    with pytest.raises(AgentResponseError):
        agent.get("msgbox", basic_auth=True)


@responses.activate
def test_agent_get_raises_on_empty_payload() -> None:
    agent = build_agent()
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/agent/msgbox",
        status=200,
        json={},
    )

    with pytest.raises(AgentResponseError):
        agent.get("msgbox", basic_auth=True)


@responses.activate
def test_agent_check_msgbox_raises_on_invalid_messages() -> None:
    agent = build_agent()
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/agent/msgbox",
        status=200,
        json={"messages": {}},
    )

    with pytest.raises(AgentResponseError):
        agent.check_msgbox()


@responses.activate
def test_agent_upload_raises_on_broker_rejection() -> None:
    agent = build_agent()
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/upload",
        status=200,
        json={
            "status": "error",
            "error": "storage_limit_exceeded",
            "error_msg": "Not enough storage is available for this upload.",
        },
    )

    with pytest.raises(AgentUploadError) as exc_info:
        agent.upload("txt", b"hello")

    assert exc_info.value.code == "storage_limit_exceeded"


@responses.activate
def test_process_contract_reports_traceback_details_by_default() -> None:
    agent = build_agent()

    def failing_job(_job):
        raise ZeroDivisionError("division by zero")

    agent.interface.func_dict["job_func"] = failing_job

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/contract/accept",
        status=200,
        json={},
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/report",
        status=200,
        json={"status": "ok"},
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/report",
        status=200,
        json={"status": "ok"},
    )

    agent.process_contract({"negotiation_id": "neg-1", "request": {}})

    report_call = responses.calls[-1]
    body = report_call.request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)

    assert payload["status"] == "error"
    assert payload["msg"] == "Job failed with ZeroDivisionError: division by zero."
    assert payload["result"]["error_type"] == "ZeroDivisionError"
    assert payload["result"]["error_message"] == "division by zero"
    assert "Traceback (most recent call last):" in payload["result"]["traceback"]


@responses.activate
def test_process_contract_can_report_summary_without_traceback() -> None:
    agent = build_agent()
    agent.job_error_detail_level = "summary"

    def failing_job(_job):
        raise RuntimeError("boom")

    agent.interface.func_dict["job_func"] = failing_job

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/contract/accept",
        status=200,
        json={},
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/report",
        status=200,
        json={"status": "ok"},
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/report",
        status=200,
        json={"status": "ok"},
    )

    agent.process_contract({"negotiation_id": "neg-2", "request": {}})

    report_call = responses.calls[-1]
    body = report_call.request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)

    assert payload["msg"] == "Job failed with RuntimeError: boom."
    assert payload["result"]["error_type"] == "RuntimeError"
    assert payload["result"]["error_message"] == "boom"
    assert "traceback" not in payload["result"]
