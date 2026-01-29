"""Recommended usage patterns for brokersystem.

Configure via env vars:
- BROKER_URL: base URL of your broker server
- BROKER_TOKEN: broker access token (client API + broker admin API)
- AGENT_ID / AGENT_SECRET: agent credentials (agent API)

Notes:
- `/api/v1/client/board` and `/api/v1/broker/board` return the same payload.
- Broker (client API) accepts agent secret *or* user token.
- BrokerAdmin (broker API) accepts user token only.

Run examples:
  BROKER_URL=https://... AGENT_ID=... AGENT_SECRET=... \
    python examples/usage_guide.py agent [--enable-upload]
  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/usage_guide.py client --agent-id <agent_id> [--step-by-step]
  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/usage_guide.py admin [--deposit-user] [--delete-user]
"""

from __future__ import annotations

import argparse
import os
import sys
import uuid
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
        agent.request_user_info(user_id=True, email=True, name_affiliation=True)

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
        if job.user_info:
            print("User info:", job.user_info)
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


def run_client(agent_id: str, step_by_step: bool) -> None:
    """Run a client flow using the broker client API.

    Broker.ask() returns the same result shape as the explicit
    negotiate/contract/get_result sequence; the example below prints
    comparable keys to make the equivalence visible.
    Note: contracts can fail if the user has insufficient points.
    """
    broker_url = require_env("BROKER_URL")
    token = require_env("BROKER_TOKEN")

    broker = Broker(broker_url=broker_url, auth=token)

    board = broker.board()
    print("Client board:", board)

    request = {"x": 2, "mode": "safe"}
    begin = broker.begin_negotiation(agent_id)
    requested_user_info = begin.get("content", {}).get("user_info_request", [])
    if requested_user_info:
        print("Agent requests user info:", requested_user_info)
        # email may be an empty string depending on auth provider settings.
        request["_user_info_consent"] = requested_user_info
    if step_by_step:
        print("Begin negotiation:", begin)

        negotiation = broker.negotiate(agent_id, request)
        print("Negotiation:", negotiation)

        negotiation_id = negotiation.get("negotiation_id")
        if not negotiation_id:
            raise RuntimeError("Negotiation failed")

        contract = broker.contract(negotiation_id)
        print("Contract:", contract)
        if contract.get("status") == "error":
            print("Contract failed (insufficient points or unavailable agent).")
            return

        result = broker.get_result(negotiation_id)
        source = "step-by-step"
    else:
        result = broker.ask(agent_id, request)
        source = "ask"

    expected_keys = {"status", "progress", "msg", "result"}
    result_keys = set(result.keys())
    result_payload = result.get("result")
    result_payload_keys = (
        sorted(result_payload.keys()) if isinstance(result_payload, dict) else None
    )

    print(f"{source} result:", result)
    print(f"{source} result keys:", sorted(result_keys))
    print(f"{source} result payload keys:", result_payload_keys)
    print("Result keys match expected:", expected_keys.issubset(result_keys))

    if isinstance(result_payload, dict):
        file_id = next(
            (
                value
                for value in result_payload.values()
                if isinstance(value, str)
                and "." in value
                and value.rsplit(".", 1)[-1] in {"png", "jpg", "gif", "csv", "pptx"}
            ),
            None,
        )
        if file_id:
            response = broker.get_file(f"files/{file_id}")
            print(
                "Downloaded file:",
                file_id,
                "content-type:",
                response.headers.get("content-type"),
            )


def run_admin(allow_deposit: bool, allow_delete: bool) -> None:
    """Run broker admin automation calls."""
    broker_url = require_env("BROKER_URL")
    token = require_env("BROKER_TOKEN")

    admin = BrokerAdmin(broker_url, token=token)

    board = admin.board()
    print("Board:", board)
    agents = board.get("agents", [])
    print("Board agent count:", len(agents))
    if agents:
        sample = agents[0]
        print("Board agent keys:", sorted(sample.keys()))
        owner = sample.get("owner", {})
        print("Board agent owner keys:", sorted(owner.keys()))
        info = sample.get("info", {})
        if isinstance(info, dict):
            print("Board agent info keys:", sorted(info.keys()))
    print("Results:", admin.list_results())
    print("Agents (admin sees all; non-admin sees own):", admin.list_agents())
    print("Tokens:", admin.list_access_tokens())
    user_response = admin.get_user()
    print("User:", user_response)

    current_user = user_response.get("user", {})
    current_user_id = current_user.get("id")

    if allow_delete and current_user_id:
        print(
            "Delete user (self-only; fails if user owns agents):",
            admin.delete_user(current_user_id),
        )

    created_user = admin.create_user(
        current_user.get("name", "Example User"),
        current_user.get("affiliation", "example"),
    )
    print("Create user:", created_user)
    current_user = created_user.get("user", current_user)
    current_user_id = current_user.get("id")

    updated_user = admin.update_user(
        name=current_user.get("name"), affiliation=current_user.get("affiliation")
    )
    print("Update user:", updated_user)

    users = admin.list_users()
    print("Users (admin sees all; non-admin sees self):", users)

    if current_user_id:
        print(
            "User by id (self; admin can access others):",
            admin.get_user_by_id(current_user_id),
        )
        print(
            "Update user by id (self-only):",
            admin.update_user_by_id(current_user_id, name=current_user.get("name")),
        )
        if allow_deposit:
            print(
                "Deposit user (self-only):",
                admin.deposit_user(current_user_id),
            )

    temp_name = f"example-agent-{uuid.uuid4().hex[:8]}"
    created = admin.create_agent(temp_name, "predict", "demo", is_public=True)
    agent = created.get("agent", {})
    agent_id = agent.get("id")
    print("Created agent:", agent)

    if agent_id:
        print("Agent detail (owner or admin only):", admin.get_agent(agent_id))
        updated = admin.update_agent(agent_id, is_public=False)
        print("Updated agent:", updated.get("agent", {}))

        full_template = admin.download_agent_template(agent_id, simple=False)
        simple_template = admin.download_agent_template(agent_id, simple=True)
        print("Agent template (full) length:", len(full_template))
        print("Agent template (simple) length:", len(simple_template))

        deleted = admin.delete_agent(agent_id)
        print("Deleted agent:", deleted)

    temp_label = f"example-token-{uuid.uuid4().hex[:6]}"
    issued = admin.issue_access_token(temp_label)
    print("Issued token:", issued)

    issued_token = issued.get("token")
    if issued_token:
        revoked = admin.revoke_access_token(issued_token)
        print("Revoked token:", revoked)


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
    client_parser.add_argument(
        "--step-by-step",
        action="store_true",
        help="Use negotiate/contract/get_result instead of ask",
    )

    admin_parser = sub.add_parser("admin", help="Run broker admin calls")
    admin_parser.add_argument(
        "--deposit-user",
        action="store_true",
        help="Deposit points to the current user",
    )
    admin_parser.add_argument(
        "--delete-user",
        action="store_true",
        help="Delete the current user (fails if the user owns agents)",
    )

    args = parser.parse_args()

    try:
        if args.command == "agent":
            run_agent(args.enable_upload)
        elif args.command == "client":
            run_client(args.agent_id, args.step_by_step)
        elif args.command == "admin":
            run_admin(args.deposit_user, args.delete_user)
    except RuntimeError as exc:
        print(exc)
        print("Set BROKER_URL, BROKER_TOKEN, AGENT_ID, AGENT_SECRET as needed.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
