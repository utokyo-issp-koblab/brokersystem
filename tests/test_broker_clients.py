import json

import pytest
import responses

from brokersystem.agent import (
    Broker,
    BrokerAdmin,
    BrokerHTTPError,
    BrokerResponseError,
    UserInfoField,
)

BROKER_URL = "https://example.test"


def _feedback(
    *,
    kind: str = "agent",
    message: str = "",
    fields: dict[str, str] | None = None,
) -> dict[str, object]:
    return {"kind": kind, "message": message, "fields": fields or {}}


def _negotiation_content(**extra: object) -> dict[str, object]:
    content: dict[str, object] = {
        "user_info_request": [],
        "input": {},
        "condition": {},
        "output": {},
        "charge": 100,
        "feedback": _feedback(),
    }
    content.update(extra)
    return content


def _agent_info() -> dict[str, object]:
    return {
        "input": {},
        "condition": {},
        "output": {},
        "description": "Example agent",
        "module_version": "0.3.0",
        "user_info_request": [],
    }


def _user_summary() -> dict[str, object]:
    return {
        "id": "u1",
        "auth": "auth0|owner",
        "name": "Owner User",
        "affiliation": "Owner Lab",
        "point": 100,
        "info": {},
    }


def _board_agent() -> dict[str, object]:
    return {
        "id": "a1",
        "name": "Agent 1",
        "type": "predict",
        "category": "cat",
        "is_public": True,
        "active": True,
        "info": _agent_info(),
        "owner": {
            "id": "u1",
            "name": "Owner User",
            "affiliation": "Owner Lab",
        },
    }


def _agent_summary() -> dict[str, object]:
    return {
        "id": "a1",
        "name": "Agent 1",
        "type": "predict",
        "category": "cat",
        "is_public": True,
        "point": 10,
        "info": _agent_info(),
    }


def _agent_detail(*, name: str = "Agent 1") -> dict[str, object]:
    return {
        **_agent_summary(),
        "name": name,
        "owner": _user_summary(),
        "secret_key": "secret-123",
    }


@responses.activate
def test_broker_begin_negotiation() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiation/begin",
        json={
            "negotiation_id": "n1",
            "state": "ok",
            "content": _negotiation_content(),
        },
        status=200,
    )

    response = broker.begin_negotiation("agent-1")
    assert response["negotiation_id"] == "n1"


@responses.activate
def test_broker_begin_negotiation_returns_required_feedback_shape() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiation/begin",
        json={
            "negotiation_id": "n1",
            "state": "ok",
            "content": _negotiation_content(
                feedback=_feedback(
                    message="Accepted with comments.",
                    fields={"x": "Set x to 5 or less."},
                )
            ),
        },
        status=200,
    )

    response = broker.begin_negotiation("agent-1")
    feedback = response["content"]["feedback"]

    assert feedback["kind"] == "agent"
    assert feedback["message"] == "Accepted with comments."
    assert feedback["fields"] == {"x": "Set x to 5 or less."}


@responses.activate
def test_broker_negotiate_sends_user_info_consent() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={
            "negotiation_id": "n1",
            "state": "ok",
            "content": _negotiation_content(),
        },
        status=200,
    )

    broker.negotiate("agent-1", {"x": 1}, user_info_consent=[UserInfoField.EMAIL])

    body = responses.calls[0].request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
    assert payload["request"]["_user_info_consent"] == ["email"]


@responses.activate
def test_broker_negotiate_returns_feedback_for_ok_state() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={
            "negotiation_id": "n1",
            "state": "ok",
            "content": _negotiation_content(
                feedback=_feedback(
                    message="Accepted with comments.",
                    fields={"x": "x is accepted near the upper bound."},
                )
            ),
        },
        status=200,
    )

    response = broker.negotiate("agent-1", {"x": 5})
    feedback = response["content"]["feedback"]

    assert response["state"] == "ok"
    assert feedback["kind"] == "agent"
    assert feedback["message"] == "Accepted with comments."
    assert feedback["fields"] == {"x": "x is accepted near the upper bound."}


@responses.activate
def test_broker_board() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/board",
        json={"agents": [_board_agent()]},
        status=200,
    )

    response = broker.board()
    assert response["agents"][0]["info"]["description"] == "Example agent"
    assert response["agents"][0]["info"]["user_info_request"] == []
    assert response["agents"][0]["info"]["input"] == {}


@responses.activate
def test_broker_board_raises_on_malformed_agent_payload() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    malformed = _board_agent()
    malformed.pop("owner")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/board",
        json={"agents": [malformed]},
        status=200,
    )

    with pytest.raises(BrokerResponseError, match="Malformed broker response"):
        broker.board()


@responses.activate
def test_broker_board_raises_on_http_error() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/board",
        status=500,
        body="oops",
    )

    with pytest.raises(BrokerHTTPError):
        broker.board()


@responses.activate
def test_broker_begin_negotiation_raises_on_missing_user_info_request() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiation/begin",
        json={"negotiation_id": "n1", "state": "ok", "content": {"input": {}}},
        status=200,
    )

    with pytest.raises(BrokerResponseError):
        broker.begin_negotiation("agent-1")


@responses.activate
def test_broker_begin_negotiation_raises_on_missing_feedback() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    content = _negotiation_content()
    content.pop("feedback")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiation/begin",
        json={"negotiation_id": "n1", "state": "ok", "content": content},
        status=200,
    )

    with pytest.raises(BrokerResponseError, match="Malformed broker response"):
        broker.begin_negotiation("agent-1")


@responses.activate
def test_broker_contract_flow_get_result() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={
            "negotiation_id": "n1",
            "state": "ok",
            "content": _negotiation_content(),
        },
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/contract",
        json={"status": "ok"},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/result/n1",
        json={"status": "running", "msg": "running", "progress": 0.2, "result": {}},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/result/n1",
        json={"status": "done", "msg": "done", "progress": 1, "result": {"x": 1}},
        status=200,
    )

    negotiation = broker.negotiate("agent-1", {"x": 1})
    assert negotiation["negotiation_id"] == "n1"

    contract = broker.contract("n1")
    assert contract["status"] == "ok"

    result = broker.get_result("n1")
    assert result["status"] == "done"
    assert result["result"]["x"] == 1


@responses.activate
def test_broker_ask_raises_on_non_ok_state() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={
            "negotiation_id": "n1",
            "state": "need_revision",
            "content": _negotiation_content(feedback=_feedback(message="x too large")),
        },
        status=200,
    )

    with pytest.raises(BrokerResponseError):
        broker.ask("agent-1", {"x": 1})


@responses.activate
def test_broker_ask_uses_feedback_message_when_error_msg_is_missing() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={
            "negotiation_id": "n1",
            "state": "need_revision",
            "content": _negotiation_content(
                feedback=_feedback(
                    message="Please reduce x before retrying.",
                    fields={"x": "Set x to 5 or less."},
                )
            ),
        },
        status=200,
    )

    with pytest.raises(BrokerResponseError, match="Please reduce x before retrying."):
        broker.ask("agent-1", {"x": 10})


@responses.activate
def test_broker_admin_agents_and_tokens() -> None:
    admin = BrokerAdmin(BROKER_URL, token="user-token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/agents",
        json={"agents": [_agent_summary()]},
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/broker/agents",
        json={"agent": _agent_detail(name="name")},
        status=200,
    )
    responses.add(
        responses.PATCH,
        f"{BROKER_URL}/api/v1/broker/agents/a1",
        json={"agent": _agent_detail(name="updated")},
        status=200,
    )
    responses.add(
        responses.DELETE,
        f"{BROKER_URL}/api/v1/broker/agents/a1",
        json={"status": "ok"},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/access_tokens",
        json={"tokens": []},
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/broker/access_tokens",
        json={"token": "t1", "label": "label"},
        status=200,
    )
    responses.add(
        responses.DELETE,
        f"{BROKER_URL}/api/v1/broker/access_tokens/t1",
        json={"status": "ok"},
        status=200,
    )

    agents = admin.list_agents()["agents"]
    assert agents[0]["info"]["description"] == "Example agent"
    assert agents[0]["info"]["output"] == {}
    assert (
        admin.create_agent("name", "predict", "cat", is_public=False)["agent"]["id"]
        == "a1"
    )
    create_call = next(
        call
        for call in responses.calls
        if call.request.method == "POST"
        and call.request.url == f"{BROKER_URL}/api/v1/broker/agents"
    )
    body = create_call.request.body
    assert body is not None
    if isinstance(body, bytes):
        body_text = body.decode("utf-8")
    else:
        body_text = body
    payload = json.loads(body_text)
    assert payload["servicer_agent"]["is_public"] is False
    assert admin.update_agent("a1", name="updated")["agent"]["name"] == "updated"
    assert admin.delete_agent("a1")["status"] == "ok"

    assert admin.list_access_tokens() == {"tokens": []}
    assert admin.issue_access_token("label")["token"] == "t1"
    assert admin.revoke_access_token("t1")["status"] == "ok"


@responses.activate
def test_broker_admin_user_and_results() -> None:
    admin = BrokerAdmin(BROKER_URL, token="user-token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/user",
        json={"user": {"id": "u1"}},
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/broker/user",
        json={"status": "created", "user": {"id": "u1"}},
        status=200,
    )
    responses.add(
        responses.PATCH,
        f"{BROKER_URL}/api/v1/broker/user",
        json={"user": {"id": "u1", "name": "updated"}},
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/results",
        json={
            "contracts": [
                {
                    "negotiation_id": "n1",
                    "status": "done",
                    "progress": 1.0,
                    "charge": 42,
                    "msg": "done",
                    "agent_id": "a1",
                    "agent_name": "Result Agent",
                    "requested_date": "2026-03-14 00:00:00",
                    "client": {
                        "auth": "auth0|owner",
                        "name": "Owner User",
                        "affiliation": "Owner Lab",
                    },
                    "mine": False,
                }
            ]
        },
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/board",
        json={"agents": []},
        status=200,
    )

    assert admin.get_user()["user"]["id"] == "u1"
    assert admin.create_user("name", "lab")["status"] == "created"
    assert admin.update_user(name="updated")["user"]["name"] == "updated"
    results = admin.list_results()
    assert results["contracts"][0]["agent_name"] == "Result Agent"
    assert results["contracts"][0]["client"]["affiliation"] == "Owner Lab"
    assert results["contracts"][0]["mine"] is False
    assert admin.board() == {"agents": []}


@responses.activate
def test_broker_admin_template_download() -> None:
    admin = BrokerAdmin(BROKER_URL, token="user-token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/agents/a1/template",
        body="print('hello')",
        status=200,
        content_type="text/plain",
    )

    template = admin.download_agent_template("a1")
    assert "print" in template


@responses.activate
def test_broker_admin_user_by_id_endpoints() -> None:
    admin = BrokerAdmin(BROKER_URL, token="user-token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/users/u1",
        json={"user": {"id": "u1"}},
        status=200,
    )
    responses.add(
        responses.PATCH,
        f"{BROKER_URL}/api/v1/broker/users/u1",
        json={"user": {"id": "u1", "name": "updated"}},
        status=200,
    )
    responses.add(
        responses.DELETE,
        f"{BROKER_URL}/api/v1/broker/users/u1",
        json={"status": "ok"},
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/broker/users/u1/deposit",
        json={"user": {"id": "u1", "point": 1000}},
        status=200,
    )

    assert admin.get_user_by_id("u1")["user"]["id"] == "u1"
    assert admin.update_user_by_id("u1", name="updated")["user"]["name"] == "updated"
    assert admin.delete_user("u1")["status"] == "ok"
    assert admin.deposit_user("u1")["user"]["point"] == 1000
