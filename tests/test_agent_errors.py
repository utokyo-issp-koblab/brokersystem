import json
import threading

import pytest
import responses

from brokersystem.agent import (
    Agent,
    AgentHTTPError,
    AgentResponseError,
    AgentUploadError,
    File,
    Job,
    RelayAssetSource,
    RelayStreamSource,
    UploadedFile,
    _relay_socket_url,
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


def test_relay_socket_url_normalizes_http_and_https() -> None:
    assert (
        _relay_socket_url("http://localhost:4000")
        == "ws://localhost:4000/api/v1/agent/relay/socket"
    )
    assert (
        _relay_socket_url("https://broker.example.com/base")
        == "wss://broker.example.com/base/api/v1/agent/relay/socket"
    )


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


def test_job_upload_returns_uploaded_file() -> None:
    agent = build_agent()

    def fake_upload(file_type: str, binary_data: bytes) -> dict[str, str]:
        assert file_type == "png"
        assert binary_data == b"preview"
        return {"file_id": "file-1"}

    agent.upload = fake_upload  # type: ignore[method-assign]

    job = Job(agent, "neg-upload", {})
    uploaded = job.upload("png", b"preview")

    assert uploaded == UploadedFile(file_id="file-1", file_type="png")


def test_job_upload_parallel_returns_report_ready_mapping() -> None:
    agent = build_agent()
    job = Job(agent, "neg-upload-parallel", {})
    calls: list[tuple[str, object]] = []

    def fake_upload(file_type: str, value: object) -> UploadedFile:
        calls.append((file_type, value))
        return UploadedFile(file_id=f"{file_type}-id", file_type=file_type)

    job.upload = fake_upload  # type: ignore[method-assign]

    uploaded = job.upload_parallel(
        {
            "preview": ("png", b"preview"),
            "archive": ("zip", b"archive"),
        },
        max_upload_workers=1,
    )

    assert uploaded == {
        "preview": UploadedFile(file_id="png-id", file_type="png"),
        "archive": UploadedFile(file_id="zip-id", file_type="zip"),
    }
    assert calls == [
        ("png", b"preview"),
        ("zip", b"archive"),
    ]


def test_job_report_preuploads_file_outputs_in_parallel_path() -> None:
    agent = build_agent()
    agent.interface.output.preview = File("png")
    agent.interface.output.archive = File("zip")
    job = Job(agent, "neg-report-uploads", {})

    captured_uploads: dict[str, object] = {}
    captured_payload: dict[str, object] = {}

    def fake_upload_parallel(
        files: dict[str, tuple[str, object]],
        *,
        max_upload_workers: int | None = None,
    ) -> dict[str, UploadedFile]:
        captured_uploads["files"] = files
        captured_uploads["max_upload_workers"] = max_upload_workers
        return {
            "preview": UploadedFile(file_id="preview-file", file_type="png"),
            "archive": UploadedFile(file_id="archive-file", file_type="zip"),
        }

    def fake_post(
        uri: str, payload: dict[str, object], **_kwargs: object
    ) -> dict[str, str]:
        captured_payload["uri"] = uri
        captured_payload["payload"] = payload
        return {"status": "ok"}

    job.upload_parallel = fake_upload_parallel  # type: ignore[method-assign]
    agent.post = fake_post  # type: ignore[method-assign]

    job.report(
        result={"preview": b"preview", "archive": b"archive"},
        max_upload_workers=5,
    )

    assert captured_uploads == {
        "files": {
            "preview": ("png", b"preview"),
            "archive": ("zip", b"archive"),
        },
        "max_upload_workers": 5,
    }
    payload = captured_payload["payload"]
    assert isinstance(payload, dict)
    result_payload = payload["result"]
    assert isinstance(result_payload, dict)
    assert result_payload["preview"] == "preview-file"
    assert result_payload["archive"] == "archive-file"


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


def test_serve_relay_file_sends_binary_chunks_and_eof(tmp_path) -> None:
    agent = build_agent()
    relay_path = tmp_path / "archive.bin"
    relay_path.write_bytes(b"hello relay")
    metadata = agent.register_relay_file(relay_path, None, None)
    assert metadata["runtime_instance_id"] == agent.runtime_instance_id

    sent_frames: list[tuple[object, int | None]] = []

    class DummyWebSocket:
        def send(self, payload: object, opcode: int | None = None) -> None:
            sent_frames.append((payload, opcode))

    agent._relay_socket = DummyWebSocket()  # type: ignore[assignment]

    agent._serve_relay_file(
        "12345678-1234-1234-1234-123456789012",
        str(metadata["source_id"]),
        0,
        relay_path.stat().st_size - 1,
        4,
        threading.Event(),
    )

    binary_frames = [
        payload
        for payload, opcode in sent_frames
        if opcode is not None and isinstance(payload, bytes)
    ]
    text_frames = [payload for payload, opcode in sent_frames if opcode is None]

    assert len(binary_frames) == 3
    assert b"hello relay" == b"".join(frame[46:] for frame in binary_frames)
    assert text_frames[-1] == json.dumps(
        {"type": "eof", "stream_id": "12345678-1234-1234-1234-123456789012"}
    )


def test_serve_relay_live_stream_sends_binary_chunks_and_eof() -> None:
    agent = build_agent()
    metadata = agent.register_relay_stream(
        RelayStreamSource(open_chunks=lambda _cancel: [b"frame-1", b"frame-2"]),
        "preview.mjpeg",
        "multipart/x-mixed-replace; boundary=frame",
    )
    assert metadata["runtime_instance_id"] == agent.runtime_instance_id

    sent_frames: list[tuple[object, int | None]] = []

    class DummyWebSocket:
        def send(self, payload: object, opcode: int | None = None) -> None:
            sent_frames.append((payload, opcode))

    agent._relay_socket = DummyWebSocket()  # type: ignore[assignment]

    agent._serve_relay_live_stream(
        "12345678-1234-1234-1234-123456789013",
        str(metadata["source_id"]),
        threading.Event(),
    )

    binary_frames = [
        payload
        for payload, opcode in sent_frames
        if opcode is not None and isinstance(payload, bytes)
    ]
    text_frames = [payload for payload, opcode in sent_frames if opcode is None]

    assert b"frame-1frame-2" == b"".join(frame[46:] for frame in binary_frames)
    assert text_frames[-1] == json.dumps(
        {"type": "eof", "stream_id": "12345678-1234-1234-1234-123456789013"}
    )


def test_serve_relay_asset_sends_binary_chunks_and_eof(tmp_path) -> None:
    agent = build_agent()
    asset_dir = tmp_path / "hls"
    asset_dir.mkdir()
    (asset_dir / "index.m3u8").write_text("#EXTM3U\nsegment000.ts\n", encoding="utf-8")
    metadata = agent.register_relay_assets(
        RelayAssetSource(root_dir=asset_dir, entry_path="index.m3u8"),
        "preview.m3u8",
        "application/vnd.apple.mpegurl",
    )
    assert metadata["runtime_instance_id"] == agent.runtime_instance_id

    sent_frames: list[tuple[object, int | None]] = []

    class DummyWebSocket:
        def send(self, payload: object, opcode: int | None = None) -> None:
            sent_frames.append((payload, opcode))

    agent._relay_socket = DummyWebSocket()  # type: ignore[assignment]

    agent._serve_relay_asset(
        "12345678-1234-1234-1234-123456789015",
        str(metadata["source_id"]),
        "index.m3u8",
        threading.Event(),
    )

    binary_frames = [
        payload
        for payload, opcode in sent_frames
        if opcode is not None and isinstance(payload, bytes)
    ]
    text_frames = [payload for payload, opcode in sent_frames if opcode is None]

    assert b"#EXTM3U\nsegment000.ts\n" == b"".join(
        frame[46:] for frame in binary_frames
    )
    assert text_frames[-1] == json.dumps(
        {"type": "eof", "stream_id": "12345678-1234-1234-1234-123456789015"}
    )


def test_handle_relay_cancel_sets_stream_event() -> None:
    agent = build_agent()
    cancel_event = threading.Event()
    with agent._relay_cancel_events_lock:
        agent._relay_cancel_events["stream-1"] = cancel_event

    agent._handle_relay_control_message({"type": "cancel", "stream_id": "stream-1"})

    assert cancel_event.is_set() is True


def test_handle_relay_control_message_starts_file_server_thread(tmp_path) -> None:
    agent = build_agent()
    relay_path = tmp_path / "archive.bin"
    relay_path.write_bytes(b"relay-thread")
    metadata = agent.register_relay_file(relay_path, None, None)

    seen_calls: list[tuple[str, str, int, int, int]] = []
    done = threading.Event()

    def fake_serve(
        stream_id, source_id, start_byte, end_byte, chunk_size, cancel_event
    ):
        seen_calls.append((stream_id, source_id, start_byte, end_byte, chunk_size))
        done.set()

    agent._serve_relay_file = fake_serve  # type: ignore[method-assign]

    agent._handle_relay_control_message(
        {
            "type": "serve_file",
            "stream_id": "stream-2",
            "source_id": metadata["source_id"],
            "start_byte": 1,
            "end_byte": 5,
            "chunk_size": 3,
        }
    )

    assert done.wait(timeout=1.0)
    assert seen_calls == [("stream-2", metadata["source_id"], 1, 5, 3)]


def test_handle_relay_control_message_starts_live_server_thread() -> None:
    agent = build_agent()
    metadata = agent.register_relay_stream(
        RelayStreamSource(open_chunks=lambda _cancel: [b"frame"]),
        "preview.mjpeg",
        "multipart/x-mixed-replace; boundary=frame",
    )

    seen_calls: list[tuple[str, str]] = []
    done = threading.Event()

    def fake_serve(stream_id, source_id, cancel_event):
        assert cancel_event.is_set() is False
        seen_calls.append((stream_id, source_id))
        done.set()

    agent._serve_relay_live_stream = fake_serve  # type: ignore[method-assign]

    agent._handle_relay_control_message(
        {
            "type": "serve_live_stream",
            "stream_id": "stream-live",
            "source_id": metadata["source_id"],
        }
    )

    assert done.wait(timeout=1.0)
    assert seen_calls == [("stream-live", metadata["source_id"])]


def test_handle_relay_control_message_starts_asset_server_thread(tmp_path) -> None:
    agent = build_agent()
    asset_dir = tmp_path / "hls"
    asset_dir.mkdir()
    (asset_dir / "index.m3u8").write_text("#EXTM3U\nsegment000.ts\n", encoding="utf-8")
    metadata = agent.register_relay_assets(
        RelayAssetSource(root_dir=asset_dir, entry_path="index.m3u8"),
        "preview.m3u8",
        "application/vnd.apple.mpegurl",
    )

    seen_calls: list[tuple[str, str, str]] = []
    done = threading.Event()

    def fake_serve(stream_id, source_id, asset_path, cancel_event):
        assert cancel_event.is_set() is False
        seen_calls.append((stream_id, source_id, asset_path))
        done.set()

    agent._serve_relay_asset = fake_serve  # type: ignore[method-assign]

    agent._handle_relay_control_message(
        {
            "type": "serve_asset",
            "stream_id": "stream-asset",
            "source_id": metadata["source_id"],
            "asset_path": "index.m3u8",
        }
    )

    assert done.wait(timeout=1.0)
    assert seen_calls == [("stream-asset", metadata["source_id"], "index.m3u8")]


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
