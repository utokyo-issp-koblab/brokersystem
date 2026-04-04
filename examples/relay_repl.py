"""Bidirectional text relay example for brokersystem.

This example returns an interactive text session from a single broker job.
Both the browser UI and SDK clients can open the same relay-session handle.

Run:
  BROKER_URL=https://... AGENT_AUTH='<agent_auth>' \
    python examples/relay_repl.py agent

  BROKER_URL=https://... BROKER_TOKEN=... \
    python examples/relay_repl.py client --agent-id <agent_id> [--command help --command exit]
"""

from __future__ import annotations

import argparse
import os
import threading
import time
from collections.abc import Callable, Mapping

from brokersystem import (
    Agent,
    Broker,
    Job,
    RelaySession,
    RelaySessionConnection,
    RelaySessionHandle,
    RelaySessionSource,
)


def require_env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise RuntimeError(f"Missing required env var: {key}")
    return value


def parse_session_result(
    broker: Broker, result_payload: Mapping[str, object]
) -> RelaySessionHandle:
    match result_payload:
        case {"console": dict() as console_obj}:
            return broker.parse_relay_session(console_obj)
        case _:
            raise RuntimeError("Unexpected broker result payload.")


def build_demo_session() -> RelaySessionSource:
    def open_session(
        cancel_event: threading.Event,
        emit_text: Callable[[str], None],
        close_session: Callable[[], None],
    ) -> Callable[[str], None]:
        emit_text(
            "brokersystem relay repl demo\n"
            "commands: help, echo <text>, time, exit\n"
            "repl> "
        )

        def handle_input(text: str) -> None:
            if cancel_event.is_set():
                return

            command = text.strip()
            if command == "":
                emit_text("repl> ")
                return
            if command == "help":
                emit_text("help | echo <text> | time | exit\nrepl> ")
                return
            if command.startswith("echo "):
                emit_text(command[5:] + "\nrepl> ")
                return
            if command == "time":
                emit_text(
                    time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime()) + "\nrepl> "
                )
                return
            if command in {"exit", "quit"}:
                emit_text("bye\n")
                close_session()
                return

            emit_text(f"unknown command: {command}\nrepl> ")

        return handle_input

    return RelaySessionSource(open_session=open_session)


def build_agent() -> Agent:
    broker_url = require_env("BROKER_URL")
    agent_auth = require_env("AGENT_AUTH")

    agent = Agent(broker_url)

    @agent.config
    def make_config() -> None:
        agent.name = "relay-repl-example-sdk"
        agent.agent_auth = agent_auth
        agent.description = "Returns a broker-relayed interactive text session."
        agent.charge = 1
        agent.output.console = RelaySession(
            name="python-repl",
            help="Interactive text session relayed through the broker.",
        )

    @agent.job_func
    def job(_job: Job) -> dict[str, object]:
        return {"console": build_demo_session()}

    return agent


def run_agent() -> None:
    print("Serving relay text session.")
    build_agent().run()


def print_session_output(session: RelaySessionConnection, timeout: float = 5.0) -> bool:
    text = session.recv_text(timeout=timeout)
    if text is None:
        return False
    print(text, end="")
    return True


def run_client(agent_id: str, commands: list[str]) -> None:
    broker = Broker(
        broker_url=require_env("BROKER_URL"), auth=require_env("BROKER_TOKEN")
    )
    result_payload = broker.ask(agent_id, {})["result"]
    relay_session = parse_session_result(broker, result_payload)

    print("Session URI:", relay_session.session_uri)
    with broker.open_session(relay_session) as session:
        if not print_session_output(session):
            return

        if commands:
            for command in commands:
                print(f">>> {command}")
                session.send_text(command)
                if not print_session_output(session):
                    break
            return

        while True:
            try:
                command = input()
            except EOFError:
                break
            session.send_text(command)
            if not print_session_output(session):
                break


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode", required=True)

    subparsers.add_parser("agent")

    client_parser = subparsers.add_parser("client")
    client_parser.add_argument("--agent-id", required=True)
    client_parser.add_argument(
        "--command",
        action="append",
        default=[],
        help="Command to send to the relay session. Repeat for multiple commands.",
    )

    args = parser.parse_args()
    if args.mode == "agent":
        run_agent()
    else:
        run_client(args.agent_id, args.command)


if __name__ == "__main__":
    main()
