"""Recommended usage patterns for brokersystem.

Configure via env vars:
- BROKER_URL: base URL of your broker server
- BROKER_TOKEN: broker access token (client API + broker admin API)
- AGENT_ID / AGENT_SECRET: agent credentials (agent API)

Run examples:
  python examples/usage_guide.py agent [--enable-upload]
  python examples/usage_guide.py client --agent-id <agent_id>
  python examples/usage_guide.py admin
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

from brokersystem import Agent, Broker, BrokerAdmin, Choice, File, Number, Table


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def build_agent(enable_upload: bool) -> Agent:
    """Create an Agent with config, negotiation, and job handlers."""
    broker_url = require_env("BROKER_URL")
    agent_id = require_env("AGENT_ID")
    agent_secret = require_env("AGENT_SECRET")

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "example-agent"
        agent.secret_token = f"{agent_id}:{agent_secret}"
        agent.description = "Example agent showing input/output templates"
        agent.charge = 100

        agent.input.x = Number(1, min=0, max=10, unit="unit")
        agent.input.mode = Choice(["fast", "safe"])
        agent.output.score = Number(unit="pt")
        agent.output.table = Table(unit_dict={"x": "unit", "y": "unit"})
        if enable_upload:
            agent.output.image = File("png")

    @agent.charge_func
    def charge_func(input_values: dict[str, Any]) -> int:
        if input_values.get("mode") == "fast":
            return 200
        return 100

    @agent.negotiation
    def negotiation(
        request: dict[str, Any], response: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        if request.get("x") is None:
            return "need_revision", response
        return "ok", response

    @agent.job_func
    def job_func(job) -> dict[str, Any]:
        x_value = job["x"]
        job.msg("Starting job")
        job.progress(0.5)
        result = {
            "score": x_value * 2,
            "table": [{"x": x_value, "y": x_value * 3}],
        }
        if enable_upload:
            result["image"] = b"fake-bytes"
        job.progress(1.0, msg="Done")
        return result

    return agent


def run_agent(enable_upload: bool) -> None:
    agent = build_agent(enable_upload)
    agent.run()


def run_client(agent_id: str) -> None:
    """Run a client flow using the broker client API."""
    broker_url = require_env("BROKER_URL")
    token = require_env("BROKER_TOKEN")

    broker = Broker(broker_url=broker_url, auth=token)

    begin = broker.begin_negotiation(agent_id)
    print("Begin negotiation:", begin)

    request = {"x": 2, "mode": "safe"}
    negotiation = broker.negotiate(agent_id, request)
    print("Negotiation:", negotiation)

    negotiation_id = negotiation.get("negotiation_id")
    if not negotiation_id:
        raise RuntimeError("Negotiation failed")

    contract = broker.contract(negotiation_id)
    print("Contract:", contract)

    result = broker.get_result(negotiation_id)
    print("Result:", result)


def run_admin() -> None:
    """Run broker admin automation calls."""
    broker_url = require_env("BROKER_URL")
    token = require_env("BROKER_TOKEN")

    admin = BrokerAdmin(broker_url, token=token)

    print("Board:", admin.board())
    print("Results:", admin.list_results())
    print("Agents:", admin.list_agents())
    print("Tokens:", admin.list_access_tokens())
    print("User:", admin.get_user())


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    agent_parser = sub.add_parser("agent", help="Run the example agent")
    agent_parser.add_argument(
        "--enable-upload",
        action="store_true",
        help="Include file upload example output",
    )

    client_parser = sub.add_parser("client", help="Run the client flow")
    client_parser.add_argument("--agent-id", required=True, help="Target agent id")

    sub.add_parser("admin", help="Run broker admin calls")

    args = parser.parse_args()

    try:
        if args.command == "agent":
            run_agent(args.enable_upload)
        elif args.command == "client":
            run_client(args.agent_id)
        elif args.command == "admin":
            run_admin()
    except RuntimeError as exc:
        print(exc)
        print("Set BROKER_URL, BROKER_TOKEN, AGENT_ID, AGENT_SECRET as needed.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
