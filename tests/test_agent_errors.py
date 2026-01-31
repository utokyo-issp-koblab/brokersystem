import pytest
import responses

from brokersystem.agent import Agent, AgentHTTPError, AgentResponseError

BROKER_URL = "https://example.test"


def build_agent() -> Agent:
    agent = Agent(BROKER_URL)
    agent.auth = "agent-id:secret"
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
