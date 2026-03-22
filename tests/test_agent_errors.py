import json

import pytest
import responses

from brokersystem.agent import (
    Agent,
    AgentHTTPError,
    AgentResponseError,
    AgentUploadError,
    Job,
)

BROKER_URL = "https://example.test"


def build_agent() -> Agent:
    agent = Agent(BROKER_URL)
    agent.auth = "agent-id:secret"
    agent.access_token = "agent-token"
    agent.REQUEST_RETRY_DEADLINE = 0.0
    agent.REQUEST_RETRY_BASE = 0.0
    agent.REQUEST_RETRY_MAX = 0.0
    agent.CONTRACT_LEASE_RETRY_DEADLINE = 0.0
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
def test_agent_upload_re_registers_after_401() -> None:
    agent = build_agent()

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/upload",
        status=401,
        body="Invalid token",
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/config",
        status=200,
        json={"status": "ok", "token": "fresh-token"},
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/upload",
        status=200,
        json={"file_id": "file-1"},
    )

    response = agent.upload("txt", b"hello")

    assert response["file_id"] == "file-1"
    assert agent.access_token == "fresh-token"


@responses.activate
def test_agent_acknowledges_claimed_message_after_successful_processing() -> None:
    agent = build_agent()
    seen: list[dict[str, object]] = []
    agent.process_negotiation_request = lambda body: seen.append(body)  # type: ignore[method-assign]

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/msgbox/ack",
        status=200,
        json={},
    )

    agent._process_and_ack_message(
        {
            "msg_type": "negotiation_request",
            "body": {"negotiation_id": "neg-1"},
            "_message_box_id": "msg-1",
            "_message_box_claim_token": "claim-1",
        }
    )

    assert seen == [{"negotiation_id": "neg-1"}]
    assert len(responses.calls) == 1


@responses.activate
def test_agent_ack_msgbox_returns_cleanly_after_success() -> None:
    agent = build_agent()

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/msgbox/ack",
        status=200,
        json={},
    )

    agent.ack_msgbox("msg-1", "claim-1")

    assert len(responses.calls) == 1


@responses.activate
def test_agent_renew_contract_lease_returns_cleanly_after_success() -> None:
    agent = build_agent()

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/contract/lease",
        status=200,
        json={},
    )

    agent.renew_contract_lease("neg-1")

    assert len(responses.calls) == 1


@responses.activate
def test_agent_leaves_claimed_message_unacked_after_processing_failure() -> None:
    agent = build_agent()

    def explode(_body):
        raise RuntimeError("boom")

    agent.process_negotiation_request = explode  # type: ignore[method-assign]

    agent._process_and_ack_message(
        {
            "msg_type": "negotiation_request",
            "body": {"negotiation_id": "neg-2"},
            "_message_box_id": "msg-2",
            "_message_box_claim_token": "claim-2",
        }
    )

    assert len(responses.calls) == 0


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
        f"{BROKER_URL}/api/v1/agent/contract/lease",
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

    job = Job(agent, "neg-1", {})
    agent._run_contract_job({"negotiation_id": "neg-1", "request": {}}, job)

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
def test_contract_message_is_not_acked_when_accept_fails() -> None:
    agent = build_agent()

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/agent/contract/accept",
        status=500,
        json={"status": "error"},
    )

    agent._process_and_ack_message(
        {
            "msg_type": "contract",
            "body": {"negotiation_id": "neg-accept-fail", "request": {}},
            "_message_box_id": "msg-1",
            "_message_box_claim_token": "claim-1",
        }
    )

    assert [call.request.url for call in responses.calls] == [
        f"{BROKER_URL}/api/v1/agent/contract/accept"
    ]


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
        f"{BROKER_URL}/api/v1/agent/contract/lease",
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

    job = Job(agent, "neg-2", {})
    agent._run_contract_job({"negotiation_id": "neg-2", "request": {}}, job)

    report_call = responses.calls[-1]
    body = report_call.request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)

    assert payload["msg"] == "Job failed with RuntimeError: boom."
    assert payload["result"]["error_type"] == "RuntimeError"
    assert payload["result"]["error_message"] == "boom"
    assert "traceback" not in payload["result"]
