import json
from pathlib import Path

import pytest
import responses

from brokersystem.agent import (
    Agent,
    Broker,
    BrokerAdmin,
    BrokerHTTPError,
    BrokerUploadError,
    BrokerResponseError,
    RelayFileHandle,
    RelayMediaHandle,
    RelaySessionHandle,
    UserInfoField,
)

BROKER_URL = "https://example.test"

Agent.REQUEST_RETRY_DEADLINE = 0.0
Agent.REQUEST_RETRY_BASE = 0.0
Agent.REQUEST_RETRY_MAX = 0.0


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


def _relay_time_billing(**extra: object) -> dict[str, object]:
    billing: dict[str, object] = {
        "mode": "relay_time",
        "points_per_minute": 7,
        "max_duration_required": True,
        "max_duration_minutes": 5,
        "estimated_charge": 35,
        "unit": "points/min",
    }
    billing.update(extra)
    return billing


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


def _relay_file_value() -> dict[str, object]:
    return {
        "$brokersystem": {
            "version": 1,
            "kind": "relay_file",
            "transport": "broker_relay_v1",
        },
        "uri": "/api/v1/client/relay/files/neg-1/archive",
        "name": "archive.bin",
        "size_bytes": 1024,
        "content_type": "application/octet-stream",
        "availability": "agent_online_required",
    }


def _relay_media_value(
    *,
    live: bool = False,
    playback_uri: str = "/api/v1/client/relay/files/neg-1/preview",
    download_uri: str | None = "/api/v1/client/relay/files/neg-1/preview",
    content_type: str = "video/webm",
    profiles: dict[str, dict[str, str]] | None = None,
    default_profile: str | None = None,
) -> dict[str, object]:
    payload = {
        "$brokersystem": {
            "version": 1,
            "kind": "relay_media",
            "transport": "broker_relay_v1",
        },
        "playback_uri": playback_uri,
        "name": "preview.webm",
        "size_bytes": 2048,
        "content_type": content_type,
        "availability": "agent_online_required",
        "live": live,
    }
    if download_uri is not None:
        payload["download_uri"] = download_uri
    if profiles is not None:
        payload["profiles"] = profiles
    if default_profile is not None:
        payload["default_profile"] = default_profile
    return payload


def _relay_session_value() -> dict[str, object]:
    return {
        "$brokersystem": {
            "version": 1,
            "kind": "relay_session",
            "transport": "broker_relay_v1",
        },
        "session_uri": "/api/v1/client/relay/sessions/neg-1/console/socket",
        "name": "python-repl",
        "protocol": "text",
        "availability": "agent_online_required",
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
    assert response["job_id"] == "n1"


@responses.activate
def test_broker_negotiate_parses_relay_time_billing() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={
            "negotiation_id": "n1",
            "state": "ok",
            "content": _negotiation_content(billing=_relay_time_billing()),
        },
        status=200,
    )

    response = broker.negotiate("agent-1", {"x": 1})

    assert response["job_id"] == "n1"
    content = response["content"]
    assert "billing" in content
    billing = content["billing"]
    assert billing["mode"] == "relay_time"
    assert billing["points_per_minute"] == 7
    assert billing["unit"] == "points/min"


@responses.activate
def test_broker_negotiate_injects_max_duration_metadata() -> None:
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

    broker.negotiate("agent-1", {"x": 1}, max_duration_minutes=3)

    body = responses.calls[0].request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
    assert payload["request"]["_broker"] == {"max_duration_minutes": 3}


@responses.activate
def test_broker_negotiate_merges_existing_broker_metadata() -> None:
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

    broker.negotiate(
        "agent-1",
        {"x": 1, "_broker": {"client_tag": "demo"}},
        max_duration_minutes=4,
    )

    body = responses.calls[0].request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
    assert payload["request"]["_broker"] == {
        "client_tag": "demo",
        "max_duration_minutes": 4,
    }


def test_broker_parse_relay_session() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    handle = broker.parse_relay_session(_relay_session_value())
    assert isinstance(handle, RelaySessionHandle)
    assert handle.protocol == "text"
    assert handle.session_uri == "/api/v1/client/relay/sessions/neg-1/console/socket"


def test_broker_open_session(monkeypatch: pytest.MonkeyPatch) -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")

    class DummySocket:
        def __init__(self) -> None:
            self.timeout = None
            self.sent: list[str] = []
            self.closed = False
            self.messages = [
                json.dumps({"type": "ready", "protocol": "text"}),
                json.dumps({"type": "output", "text": "hello\n"}),
                json.dumps({"type": "eof"}),
            ]

        def gettimeout(self):
            return self.timeout

        def settimeout(self, timeout):
            self.timeout = timeout

        def recv(self):
            return self.messages.pop(0)

        def send(self, payload):
            self.sent.append(payload)

        def close(self):
            self.closed = True

    dummy_socket = DummySocket()
    captured: dict[str, object] = {}

    def fake_create_connection(url: str, *, header: list[str], timeout: float):
        captured["url"] = url
        captured["header"] = header
        captured["timeout"] = timeout
        return dummy_socket

    monkeypatch.setattr(
        "brokersystem.agent.websocket.create_connection", fake_create_connection
    )

    with broker.open_session(_relay_session_value()) as session:
        assert (
            captured["url"]
            == "wss://example.test/api/v1/client/relay/sessions/neg-1/console/socket"
        )
        assert captured["header"] == ["authorization: Basic token"]
        session.send_text("help")
        assert json.loads(dummy_socket.sent[0]) == {"type": "input", "text": "help"}
        assert session.recv_text() == "hello\n"
        assert session.recv_text() is None

    assert dummy_socket.closed is True


def test_broker_upload_parallel_returns_key_preserving_mapping() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    calls: list[tuple[str, object]] = []

    def fake_upload(file_type: str, value: object) -> str:
        calls.append((file_type, value))
        return f"{file_type}-id"

    broker.upload = fake_upload  # type: ignore[method-assign]

    uploaded = broker.upload_parallel(
        {
            "preview": ("png", b"preview"),
            "archive": ("zip", Path("/tmp/archive.zip")),
        },
        max_upload_workers=1,
    )

    assert uploaded == {"preview": "png-id", "archive": "zip-id"}
    assert calls == [
        ("png", b"preview"),
        ("zip", Path("/tmp/archive.zip")),
    ]


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
def test_broker_upload_returns_file_id(tmp_path: Path) -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    file_path = tmp_path / "note.txt"
    file_path.write_text("hello", encoding="utf-8")

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/upload",
        json={"status": "ok", "file_id": "file-1.txt"},
        status=200,
    )

    file_id = broker.upload("txt", file_path)

    assert file_id == "file-1.txt"


@responses.activate
def test_broker_upload_raises_on_storage_rejection(tmp_path: Path) -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    file_path = tmp_path / "note.txt"
    file_path.write_text("hello", encoding="utf-8")

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/upload",
        json={
            "status": "error",
            "error": "storage_limit_exceeded",
            "error_msg": "Not enough storage is available for this upload.",
        },
        status=200,
    )

    with pytest.raises(BrokerUploadError, match="Not enough storage") as exc:
        broker.upload("txt", file_path)

    assert exc.value.code == "storage_limit_exceeded"


def test_broker_parse_relay_file_returns_handle() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")

    relay_file = broker.parse_relay_file(_relay_file_value())

    assert isinstance(relay_file, RelayFileHandle)
    assert relay_file.uri == "/api/v1/client/relay/files/neg-1/archive"
    assert relay_file.size_bytes == 1024


def test_broker_parse_relay_media_returns_handle() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")

    relay_media = broker.parse_relay_media(_relay_media_value())

    assert isinstance(relay_media, RelayMediaHandle)
    assert relay_media.playback_uri == "/api/v1/client/relay/files/neg-1/preview"
    assert relay_media.download_uri == "/api/v1/client/relay/files/neg-1/preview"
    assert relay_media.live is False


def test_broker_parse_relay_media_profiles_returns_named_handles() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")

    relay_media = broker.parse_relay_media(
        _relay_media_value(
            live=True,
            playback_uri="/api/v1/client/relay/assets/neg-1/preview/stream.m3u8",
            download_uri=None,
            content_type="application/vnd.apple.mpegurl",
            default_profile="hls",
            profiles={
                "dash": {
                    "playback_uri": "/api/v1/client/relay/assets/neg-1/preview/stream.mpd",
                    "content_type": "application/dash+xml",
                },
                "hls": {
                    "playback_uri": "/api/v1/client/relay/assets/neg-1/preview/stream.m3u8",
                    "content_type": "application/vnd.apple.mpegurl",
                },
            },
        )
    )

    assert relay_media.default_profile == "hls"
    dash_profile = relay_media.get_profile("dash")
    assert dash_profile is not None
    assert (
        dash_profile.playback_uri
        == "/api/v1/client/relay/assets/neg-1/preview/stream.mpd"
    )
    assert relay_media.get_profile("missing") is None


def test_broker_parse_relay_media_allows_missing_download_uri() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")

    relay_media = broker.parse_relay_media(
        _relay_media_value(
            live=True,
            playback_uri="/api/v1/client/relay/assets/neg-1/preview/index.m3u8",
            download_uri=None,
            content_type="application/vnd.apple.mpegurl",
        )
    )

    assert (
        relay_media.playback_uri
        == "/api/v1/client/relay/assets/neg-1/preview/index.m3u8"
    )
    assert relay_media.download_uri is None
    assert relay_media.live is True


def test_broker_parse_relay_media_rejects_missing_playback_uri() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    payload = _relay_media_value()
    del payload["playback_uri"]

    with pytest.raises(BrokerResponseError, match="Malformed broker response"):
        broker.parse_relay_media(payload)


def test_broker_parse_relay_media_rejects_unknown_default_profile() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")

    with pytest.raises(BrokerResponseError, match="default_profile"):
        broker.parse_relay_media(
            _relay_media_value(
                default_profile="dash",
                profiles={
                    "hls": {
                        "playback_uri": "/api/v1/client/relay/assets/neg-1/preview/stream.m3u8",
                        "content_type": "application/vnd.apple.mpegurl",
                    }
                },
            )
        )


@responses.activate
def test_broker_open_file_accepts_absolute_relay_path_and_range() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    relay_file = broker.parse_relay_file(_relay_file_value())
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/relay/files/neg-1/archive",
        body=b"bc",
        status=206,
    )

    response = broker.open_file(relay_file, stream=False, byte_range=(1, 2))

    assert response.content == b"bc"
    request = responses.calls[0].request
    assert request.headers["authorization"] == "Basic token"
    assert request.headers["Range"] == "bytes=1-2"


@responses.activate
def test_broker_open_media_uses_download_uri_for_download_purpose() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    relay_media = broker.parse_relay_media(_relay_media_value())
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/relay/files/neg-1/preview",
        body=b"media-bytes",
        status=200,
    )

    response = broker.open_media(relay_media, purpose="download", stream=False)

    assert response.content == b"media-bytes"
    assert responses.calls[0].request.headers["authorization"] == "Basic token"


@responses.activate
def test_broker_open_media_uses_selected_profile_for_playback() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    relay_media = broker.parse_relay_media(
        _relay_media_value(
            live=True,
            playback_uri="/api/v1/client/relay/assets/neg-1/preview/stream.m3u8",
            download_uri=None,
            content_type="application/vnd.apple.mpegurl",
            default_profile="hls",
            profiles={
                "dash": {
                    "playback_uri": "/api/v1/client/relay/assets/neg-1/preview/stream.mpd",
                    "content_type": "application/dash+xml",
                },
                "hls": {
                    "playback_uri": "/api/v1/client/relay/assets/neg-1/preview/stream.m3u8",
                    "content_type": "application/vnd.apple.mpegurl",
                },
            },
        )
    )
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/relay/assets/neg-1/preview/stream.mpd",
        body=b"<MPD />",
        status=200,
    )

    response = broker.open_media(
        relay_media,
        purpose="playback",
        profile="dash",
        stream=False,
    )

    assert response.content == b"<MPD />"
    assert responses.calls[0].request.headers["authorization"] == "Basic token"


@responses.activate
def test_broker_download_file_writes_destination(tmp_path: Path) -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    relay_file = broker.parse_relay_file(_relay_file_value())
    destination = tmp_path / "archive.bin"
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/relay/files/neg-1/archive",
        body=b"archive-bytes",
        status=200,
    )

    written_path = broker.download_file(relay_file, destination)

    assert written_path == destination.resolve()
    assert destination.read_bytes() == b"archive-bytes"


@responses.activate
def test_broker_negotiate_uploads_local_file_inputs(tmp_path: Path) -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    file_path = tmp_path / "input.txt"
    file_path.write_text("payload", encoding="utf-8")

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiation/begin",
        json={
            "negotiation_id": "n-begin",
            "state": "begin",
            "content": _negotiation_content(
                input={
                    "@type": {"attachment": "file"},
                    "@constraints": {"attachment": {"file_type": "txt"}},
                    "@value": ["attachment"],
                }
            ),
        },
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/upload",
        json={"status": "ok", "file_id": "uploaded.txt"},
        status=200,
    )
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiate",
        json={
            "negotiation_id": "n-final",
            "state": "ok",
            "content": _negotiation_content(),
        },
        status=200,
    )

    response = broker.negotiate("agent-1", {"attachment": file_path})

    assert response["negotiation_id"] == "n-final"
    begin_body = responses.calls[0].request.body
    assert begin_body is not None
    begin_payload = json.loads(
        begin_body.decode("utf-8") if isinstance(begin_body, bytes) else begin_body
    )
    assert begin_payload == {"agent_id": "agent-1"}

    negotiate_body = responses.calls[2].request.body
    assert negotiate_body is not None
    negotiate_payload = json.loads(
        negotiate_body.decode("utf-8")
        if isinstance(negotiate_body, bytes)
        else negotiate_body
    )
    assert negotiate_payload["negotiation_id"] == "n-begin"
    assert negotiate_payload["request"]["attachment"] == "uploaded.txt"


@responses.activate
def test_broker_negotiate_rejects_local_file_for_non_file_input(tmp_path: Path) -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    file_path = tmp_path / "input.txt"
    file_path.write_text("payload", encoding="utf-8")

    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/negotiation/begin",
        json={
            "negotiation_id": "n-begin",
            "state": "begin",
            "content": _negotiation_content(
                input={
                    "@type": {"attachment": "string"},
                    "@constraints": {},
                    "@value": ["attachment"],
                }
            ),
        },
        status=200,
    )

    with pytest.raises(BrokerResponseError, match="local file data was provided"):
        broker.negotiate("agent-1", {"attachment": file_path})


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
        json={
            "negotiation_id": "n1",
            "job_id": "n1",
            "status": "running",
            "msg": "running",
            "progress": 0.2,
            "result": {},
        },
        status=200,
    )
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/result/n1",
        json={
            "negotiation_id": "n1",
            "job_id": "n1",
            "status": "done",
            "msg": "done",
            "progress": 1,
            "result": {"x": 1},
        },
        status=200,
    )

    negotiation = broker.negotiate("agent-1", {"x": 1})
    assert negotiation["negotiation_id"] == "n1"

    contract = broker.contract("n1")
    assert contract["status"] == "ok"

    result = broker.get_result("n1")
    assert result["status"] == "done"
    assert result["job_id"] == "n1"
    assert result["result"]["x"] == 1


@responses.activate
def test_broker_get_result_raises_agent_error_payload() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/result/n1",
        json={
            "negotiation_id": "n1",
            "job_id": "n1",
            "status": "error",
            "msg": "Maximum duration exceeded.",
            "error_msg": "Maximum duration exceeded.",
            "error": "MaxDurationExceeded",
            "error_type": "MaxDurationExceeded",
            "progress": 0.2,
            "result": {
                "error_msg": "Maximum duration exceeded.",
                "error_type": "MaxDurationExceeded",
            },
        },
        status=200,
    )

    with pytest.raises(BrokerResponseError, match="Maximum duration exceeded") as exc:
        broker.get_result("n1")

    payload = exc.value.payload
    assert isinstance(payload, dict)
    assert payload["error_type"] == "MaxDurationExceeded"


@responses.activate
def test_broker_terminate_job_posts_client_route() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/client/contract/terminate",
        json={"status": "ok"},
        status=200,
    )

    response = broker.terminate_job("n1", reason="client_requested")

    assert response["status"] == "ok"
    body = responses.calls[0].request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
    assert payload == {
        "job_id": "n1",
        "reason": "client_requested",
    }


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
def test_broker_board_raises_on_status_error_without_canonical_detail() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/board",
        json={"status": "error"},
        status=200,
    )

    with pytest.raises(
        BrokerResponseError,
        match=r"Malformed broker response.*status=error payload must include error_msg or error",
    ):
        broker.board()


@responses.activate
def test_broker_board_uses_error_code_when_error_msg_is_missing() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/board",
        json={"status": "error", "error": "forbidden"},
        status=200,
    )

    with pytest.raises(BrokerResponseError, match=r"GET board failed: forbidden"):
        broker.board()


@responses.activate
def test_broker_board_includes_error_code_when_error_msg_exists() -> None:
    broker = Broker(broker_url=BROKER_URL, auth="token")
    responses.add(
        responses.GET,
        f"{BROKER_URL}/api/v1/client/board",
        json={
            "status": "error",
            "error": "agent_unresponsive",
            "error_msg": "The agent did not respond.",
        },
        status=200,
    )

    with pytest.raises(
        BrokerResponseError,
        match=r"GET board failed \[agent_unresponsive\]: The agent did not respond.",
    ):
        broker.board()


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
def test_broker_admin_terminate_job_posts_user_token_route() -> None:
    admin = BrokerAdmin(BROKER_URL, token="user-token")
    responses.add(
        responses.POST,
        f"{BROKER_URL}/api/v1/broker/jobs/n1/terminate",
        json={"status": "ok"},
        status=200,
    )

    response = admin.terminate_job("n1", reason="client_requested")

    assert response["status"] == "ok"
    body = responses.calls[0].request.body
    assert body is not None
    payload = json.loads(body.decode("utf-8") if isinstance(body, bytes) else body)
    assert payload == {"reason": "client_requested"}


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
