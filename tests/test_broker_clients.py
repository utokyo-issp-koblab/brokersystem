import json

import responses

from brokersystem.agent import Broker, BrokerAdmin

BROKER_URL = "https://example.test"


@responses.activate
def test_broker_begin_negotiation() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiation/begin",
        json={"negotiation_id": "n1", "state": "ok", "content": {}},
        status=200,
    )

    response = broker.begin_negotiation("agent-1")
    assert response["negotiation_id"] == "n1"


@responses.activate
def test_broker_negotiate_sends_user_info_consent() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={"negotiation_id": "n1", "state": "ok", "content": {}},
        status=200,
    )

    broker.negotiate("agent-1", {"x": 1}, user_info_consent=["email"])

    body = responses.calls[0].request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
    assert payload["request"]["_user_info_consent"] == ["email"]


@responses.activate
def test_broker_board() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/board",
        json={"agents": []},
        status=200,
    )

    assert broker.board() == {"agents": []}


@responses.activate
def test_broker_contract_flow_get_result() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={"negotiation_id": "n1", "state": "ok", "content": {}},
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
def test_broker_admin_agents_and_tokens() -> None:
    admin = BrokerAdmin(BROKER_URL, token="user-token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/broker/agents",
        json={"agents": []},
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/broker/agents",
        json={"agent": {"id": "a1", "name": "name"}},
        status=200,
    )
    responses.add(
        responses.PATCH,
        f"{BROKER_URL}/api/v1/broker/agents/a1",
        json={"agent": {"id": "a1", "name": "updated"}},
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

    assert admin.list_agents() == {"agents": []}
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
        json={"contracts": []},
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
    assert admin.list_results() == {"contracts": []}
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
