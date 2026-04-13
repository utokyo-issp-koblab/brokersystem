import functools
import importlib
import inspect
import io
import json
import mimetypes
import re
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from enum import StrEnum
from os import PathLike
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import (
    Any,
    Literal,
    NoReturn,
    NotRequired,
    ParamSpec,
    TypedDict,
    TypeVar,
    cast,
)
from urllib.parse import urlsplit, urlunsplit

import pandas as pd
import PIL.Image
import requests
import websocket
from logzero import logger
from requests.adapters import HTTPAdapter

from . import __version__

JsonDict = dict[str, Any]
JsonPayload = dict[str, Any]
BinaryData = bytes | bytearray | memoryview
UploadValue = BinaryData | PathLike[str] | str
DEFAULT_HTTP_POOL_MAXSIZE = 8
DEFAULT_429_RETRY_BASE_SECONDS = 2.0
DEFAULT_429_RETRY_MAX_SECONDS = 15.0
AgentFactory = Callable[[Callable[..., JsonDict | None]], Callable[..., "Agent"]]
P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class RelayStreamSource:
    """Generic broker-relayed byte stream source.

    The `open_chunks` callback should return an iterable of byte chunks. The
    callback receives a cancellation event that becomes set when the broker or
    client closes the relay stream.
    """

    open_chunks: Callable[[threading.Event], Iterable[bytes]]


@dataclass
class RelaySessionSource:
    """Generic broker-relayed text session source.

    `open_session` is called when a client opens the relay session. The
    callback receives:

    - `cancel_event`: set when the broker or client closes the session
    - `emit_text(text)`: send text output to the client
    - `close_session()`: close the session cleanly

    The callback should return a handler for inbound client text. The returned
    handler receives each text payload sent by the client.
    """

    open_session: Callable[
        [threading.Event, Callable[[str], None], Callable[[], None]],
        Callable[[str], None] | None,
    ]


@dataclass
class RelayAssetSource:
    """Generic broker-relayed asset tree rooted on the agent host.

    Use this when one logical relay handle needs multiple files under one root
    directory, for example an HLS playlist plus media segments.
    """

    root_dir: PathLike[str] | str
    entry_path: str


@dataclass(frozen=True)
class RelayMediaProfile:
    """One playback profile exposed from a relay-backed media asset tree."""

    entry_path: str
    content_type: str


RelaySourceValue = (
    PathLike[str] | str | RelayStreamSource | RelaySessionSource | RelayAssetSource
)
RelayRegistrar = Callable[[RelaySourceValue, str | None, str | None], JsonDict]

ResultValue = str | int | float | bool | None | list[Any] | dict[str, Any]
NegotiationStatus = Literal["ok", "need_revision", "ng"]
NegotiationFeedbackKind = Literal["agent", "validation"]
JobErrorDetailLevel = Literal["summary", "traceback"]
ContractRunPhase = Literal["starting", "running", "done", "error", "terminated"]
BillingMode = Literal["relay_time"]
BillingUnit = Literal["points/min"]
NegotiationState = Literal[
    "begin",
    "request",
    "ok",
    "need_revision",
    "ng",
    "error",
    "contract_requested",
    "contract_accepted",
    "contract_denied",
]
ResultStatus = Literal["requested", "accepted", "running", "done", "error", "denied"]

TemplateSchema = TypedDict(
    "TemplateSchema",
    {
        "@type": dict[str, str],
        "@help": dict[str, str | None],
        "@unit": dict[str, str | None],
        "@keys": list[str],
        "@value": list[str],
        "@table": dict[str, list[str]],
        "@repr": dict[str, Any],
        "@necessity": dict[str, str],
        "@constraints": dict[str, dict[str, Any]],
        "@option": dict[str, Any],
        "@file": dict[str, Any],
        "@ui": dict[str, Any],
    },
    total=False,
)


class UiPreviewVega(TypedDict):
    """UI preview declaration for Vega specs.

    Notes:
    - The broker UI should treat this as untrusted input and render it in a sandbox.
    - Agents should not embed secrets in the spec.
    """

    type: Literal["vega"]
    spec: JsonDict


# Future: `UiPreview` will become a union of multiple preview types.
UiPreview = UiPreviewVega


ResultPayload = dict[str, ResultValue]


class UserInfoField(StrEnum):
    USER_ID = "user_id"
    EMAIL = "email"
    NAME_AFFILIATION = "name_affiliation"


UserInfoFieldValue = Literal["user_id", "email", "name_affiliation"]
USER_INFO_FIELDS: tuple[UserInfoField, ...] = tuple(UserInfoField)


class UserInfoNameAffiliation(TypedDict):
    name: str | None
    affiliation: str | None


class UserInfo(TypedDict):
    user_id: str | None
    email: str | None
    name_affiliation: UserInfoNameAffiliation


class NegotiationFeedback(TypedDict):
    kind: NegotiationFeedbackKind
    message: str
    fields: dict[str, str]


NegotiationDecision = tuple[NegotiationStatus, NegotiationFeedback]


class NegotiationBilling(TypedDict):
    mode: BillingMode
    points_per_minute: int | float
    max_duration_required: bool
    max_duration_minutes: int | float | None
    estimated_charge: int | float | None
    unit: BillingUnit


class NegotiationContent(TypedDict):
    user_info_request: list[UserInfoField]
    input: TemplateSchema
    condition: TemplateSchema
    output: TemplateSchema
    charge: int
    ui_preview: NotRequired[UiPreview]
    feedback: NegotiationFeedback
    billing: NotRequired[NegotiationBilling]


class NegotiationResponse(TypedDict):
    negotiation_id: str
    job_id: str
    state: NegotiationState
    content: NegotiationContent


class ContractResponse(TypedDict):
    status: Literal["ok"]


class ResultResponse(TypedDict):
    negotiation_id: str
    job_id: str
    status: Literal["done"]
    msg: str
    progress: float
    result: ResultPayload
    agent_id: NotRequired[str | None]
    request: NotRequired[JsonDict]
    input: NotRequired[TemplateSchema]
    billing: NotRequired[dict[str, Any]]


RelayFileValue = TypedDict(
    "RelayFileValue",
    {
        "$brokersystem": dict[str, str | int],
        "source_id": str,
        "runtime_instance_id": str,
        "name": str,
        "size_bytes": int,
        "content_type": str,
        "availability": str,
    },
)


RelayMediaValue = TypedDict(
    "RelayMediaValue",
    {
        "$brokersystem": dict[str, str | int],
        "source_id": str,
        "runtime_instance_id": str,
        "name": str,
        "size_bytes": int,
        "content_type": str,
        "availability": str,
        "live": bool,
        "playback_uri": str,
        "download_uri": str,
        "default_profile": str,
        "profiles": dict[str, dict[str, str]],
    },
    total=False,
)


class RelayMediaProfileMetadata(TypedDict):
    entry_path: str
    content_type: str


class RelayMediaProfilesMetadata(TypedDict):
    default_profile: str
    profiles: dict[str, RelayMediaProfileMetadata]


RelaySessionValue = TypedDict(
    "RelaySessionValue",
    {
        "$brokersystem": dict[str, str | int],
        "source_id": str,
        "runtime_instance_id": str,
        "name": str,
        "protocol": str,
        "availability": str,
        "session_uri": str,
    },
)


@dataclass(frozen=True)
class RelayFileHandle:
    """Parsed relay-file handle returned by the client API."""

    uri: str
    name: str
    size_bytes: int
    content_type: str
    availability: str


@dataclass(frozen=True)
class RelayMediaProfileHandle:
    """One parsed playback profile available on a relay-media handle."""

    name: str
    playback_uri: str
    content_type: str


@dataclass(frozen=True)
class RelayMediaHandle:
    """Parsed relay-media handle returned by the client API."""

    playback_uri: str
    download_uri: str | None
    name: str
    size_bytes: int
    content_type: str
    availability: str
    live: bool
    default_profile: str | None = None
    profiles: tuple[RelayMediaProfileHandle, ...] = ()

    def get_profile(self, name: str) -> RelayMediaProfileHandle | None:
        """Return the named playback profile when present."""
        for profile in self.profiles:
            if profile.name == name:
                return profile
        return None


@dataclass(frozen=True)
class RelaySessionHandle:
    """Parsed relay-session handle returned by the client API."""

    session_uri: str
    name: str
    protocol: str
    availability: str


@dataclass(frozen=True)
class UploadedFile:
    """Uploaded job-result file handle accepted by `File(...)` outputs."""

    file_id: str
    file_type: str


@dataclass
class RelaySessionEvent:
    """One event received from a relay-session connection."""

    type: str
    text: str | None = None
    code: str | None = None
    message: str | None = None


class RelaySessionConnection:
    """Bidirectional text session opened from a relay-session handle."""

    def __init__(self, ws: websocket.WebSocket) -> None:
        self._ws = ws
        self._closed = False
        self._socket_closed = False

    def close(self) -> None:
        """Close the underlying session socket."""
        if self._socket_closed:
            return
        self._closed = True
        self._socket_closed = True
        self._ws.close()

    def __enter__(self) -> "RelaySessionConnection":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        _ = (exc_type, exc, tb)
        self.close()

    def send_text(self, text: str) -> None:
        """Send one text payload to the relay session."""
        if self._closed:
            raise BrokerConnectionError("Relay session is already closed.")
        self._ws.send(json.dumps({"type": "input", "text": text}))

    def recv_event(self, timeout: float | None = None) -> RelaySessionEvent:
        """Receive one relay-session event."""
        if self._closed:
            raise BrokerConnectionError("Relay session is already closed.")
        previous_timeout = self._ws.gettimeout()
        if timeout is not None:
            self._ws.settimeout(timeout)
        try:
            payload = self._ws.recv()
        except websocket.WebSocketTimeoutException as exc:
            raise TimeoutError("Timed out waiting for relay session event.") from exc
        except websocket.WebSocketConnectionClosedException as exc:
            self._closed = True
            raise BrokerConnectionError("Relay session closed unexpectedly.") from exc
        finally:
            if timeout is not None:
                self._ws.settimeout(previous_timeout)

        if not isinstance(payload, str):
            raise BrokerResponseError(
                "Relay session received a non-text websocket frame.",
                payload=payload,
            )

        try:
            message = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise BrokerResponseError(
                "Relay session received invalid JSON.",
                payload=payload,
            ) from exc

        if not isinstance(message, dict):
            raise BrokerResponseError(
                "Relay session received a non-object event.",
                payload=message,
            )

        event_type = message.get("type")
        if not isinstance(event_type, str):
            raise BrokerResponseError(
                "Relay session event is missing a string 'type'.",
                payload=message,
            )

        text = message.get("text")
        code = message.get("code")
        detail = message.get("message")

        if text is not None and not isinstance(text, str):
            raise BrokerResponseError(
                "Relay session event field 'text' must be a string.",
                payload=message,
            )
        if code is not None and not isinstance(code, str):
            raise BrokerResponseError(
                "Relay session event field 'code' must be a string.",
                payload=message,
            )
        if detail is not None and not isinstance(detail, str):
            raise BrokerResponseError(
                "Relay session event field 'message' must be a string.",
                payload=message,
            )

        if event_type in {"eof", "error"}:
            self._closed = True

        return RelaySessionEvent(type=event_type, text=text, code=code, message=detail)

    def recv_text(self, timeout: float | None = None) -> str | None:
        """Receive the next output text, returning None on EOF."""
        while True:
            event = self.recv_event(timeout=timeout)
            if event.type == "ready":
                continue
            if event.type == "output":
                return event.text or ""
            if event.type == "eof":
                return None
            if event.type == "error":
                raise BrokerResponseError(
                    f"Relay session failed [{event.code or 'relay_error'}]: {event.message or 'Unknown error.'}"
                )
            raise BrokerResponseError(
                f"Relay session received unknown event type: {event.type!r}",
                payload=event,
            )


class BoardResponse(TypedDict):
    agents: list["BoardAgent"]


class BoardAgentOwner(TypedDict):
    id: str
    name: str
    affiliation: str


class AgentInfo(TypedDict):
    input: TemplateSchema
    condition: TemplateSchema
    output: TemplateSchema
    description: str
    module_version: str
    user_info_request: list[UserInfoFieldValue]
    billing: NotRequired[NegotiationBilling]


class BoardAgent(TypedDict):
    id: str
    name: str
    type: str
    category: str
    info: AgentInfo
    is_public: bool
    active: bool
    owner: BoardAgentOwner


class AgentSummary(TypedDict):
    id: str
    name: str
    type: str
    category: str
    is_public: bool
    point: int
    info: AgentInfo


class UserSummary(TypedDict):
    id: str
    auth: str
    name: str
    affiliation: str
    point: int
    info: dict[str, str]


class ResultClientSummary(TypedDict):
    auth: str
    name: str
    affiliation: str


class AgentDetailBase(AgentSummary):
    owner: UserSummary


class AgentDetail(AgentDetailBase):
    secret_key: str


class ResultSummary(TypedDict):
    negotiation_id: str
    status: ResultStatus
    progress: float
    charge: float
    msg: str
    agent_id: str
    agent_name: str
    requested_date: str
    client: ResultClientSummary
    mine: bool


class TokenSummary(TypedDict):
    token: str
    label: str
    created_at: str
    last_seen: str


class ResultsResponse(TypedDict):
    contracts: list[ResultSummary]


class AgentsResponse(TypedDict):
    agents: list[AgentSummary]


class TokensResponse(TypedDict):
    tokens: list[TokenSummary]


class AccessTokenResponse(TypedDict):
    token: str
    label: str


class AgentResponse(TypedDict):
    agent: AgentDetail


class UserResponse(TypedDict):
    user: UserSummary


class UserResponseWithStatus(UserResponse):
    status: Literal["exists", "created"]


class UsersResponse(TypedDict):
    users: list[UserSummary]


class StatusResponse(TypedDict):
    status: Literal["ok"]


class BrokerError(RuntimeError):
    """Base class for broker client errors."""


class BrokerConnectionError(BrokerError):
    """Raised when the broker URL cannot be reached."""


class BrokerHTTPError(BrokerError):
    """Raised for non-200 HTTP responses."""

    def __init__(self, status_code: int, uri: str, content: bytes | str | None) -> None:
        self.status_code = status_code
        self.uri = uri
        self.content = content
        message = f"HTTP {status_code} for {uri}"
        if content:
            message = f"{message}: {content!r}"
        super().__init__(message)


class BrokerResponseError(BrokerError):
    """Raised for malformed or error response payloads."""

    def __init__(self, message: str, payload: Any | None = None) -> None:
        super().__init__(message)
        self.payload = payload


class BrokerUploadError(BrokerError):
    """Raised when the broker rejects a client-side upload request."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        payload: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.payload = payload


class AgentError(RuntimeError):
    """Base class for agent runtime errors."""


class JobCancelledError(AgentError):
    """Raised by `Job.raise_if_cancelled()` after cooperative termination."""

    def __init__(self, job_id: str, reason: str | None = None) -> None:
        self.job_id = job_id
        self.reason = reason
        message = f"Job {job_id} was cancelled"
        if reason:
            message = f"{message}: {reason}"
        super().__init__(message)


class AgentConnectionError(AgentError):
    """Raised when an Agent request cannot reach the broker URL."""


class AgentHTTPError(AgentError):
    """Raised for non-200 HTTP responses to agent requests."""

    def __init__(self, status_code: int, uri: str, content: bytes | str | None) -> None:
        self.status_code = status_code
        self.uri = uri
        self.content = content
        message = f"HTTP {status_code} for {uri}"
        if content:
            message = f"{message}: {content!r}"
        super().__init__(message)


class AgentResponseError(AgentError):
    """Raised for malformed agent response payloads."""

    def __init__(self, message: str, payload: Any | None = None) -> None:
        super().__init__(message)
        self.payload = payload


class AgentUploadError(AgentError):
    """Raised when the broker rejects an upload request."""

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        payload: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.payload = payload


def _coerce_error_content(content: bytes | str | None) -> str | None:
    if content is None:
        return None
    if isinstance(content, bytes):
        try:
            return content.decode("utf-8", errors="replace")
        except Exception:
            return repr(content)
    return str(content)


def _raise_malformed_response(
    context: str, detail: str, payload: object | None = None
) -> NoReturn:
    raise BrokerResponseError(
        f"Malformed broker response in {context}: {detail}", payload=payload
    )


def _ensure_response_dict(
    payload: object, context: str, *, allow_status_error: bool = False
) -> JsonDict:
    """Validate a generic broker JSON envelope.

    Generic broker transport errors must use the canonical envelope:
    `{"status": "error", "error_msg": <detail>, "error": <optional code>}`.

    `error_msg` is the human-readable detail. `error` is an optional machine-
    readable code. User agent callbacks should not construct this transport
    envelope directly; they should use the public negotiation helpers or raise
    exceptions and let the SDK/server translate them.
    """
    if not isinstance(payload, dict):
        _raise_malformed_response(
            context, f"expected a JSON object, got {payload!r}", payload
        )
    if not payload:
        _raise_malformed_response(context, "returned an empty payload", payload)
    if payload.get("status") == "error" and not allow_status_error:
        code = payload.get("error")
        detail = payload.get("error_msg") or code
        if detail is None:
            _raise_malformed_response(
                context,
                "status=error payload must include error_msg or error",
                payload,
            )
        if code and payload.get("error_msg"):
            message = f"{context} failed [{code}]: {detail}"
        else:
            message = f"{context} failed: {detail}"
        raise BrokerResponseError(
            message,
            payload=payload,
        )
    if "error_msg" in payload:
        raise BrokerResponseError(f"{context} failed: {payload['error_msg']}", payload)
    if "error" in payload:
        raise BrokerResponseError(f"{context} failed: {payload['error']}", payload)
    return payload


def _normalize_negotiation_content_feedback(
    content: Mapping[str, object],
    *,
    context: str,
) -> NegotiationContent:
    normalized = dict(content)
    normalized["user_info_request"] = _normalize_user_info_fields(
        _require_str_list(normalized, "user_info_request", context)
    )
    normalized["input"] = cast(
        TemplateSchema, _require_mapping(normalized, "input", context)
    )
    normalized["condition"] = cast(
        TemplateSchema, _require_mapping(normalized, "condition", context)
    )
    normalized["output"] = cast(
        TemplateSchema, _require_mapping(normalized, "output", context)
    )
    normalized["charge"] = _require_int(normalized, "charge", context)
    if "ui_preview" in normalized:
        normalized["ui_preview"] = _normalize_ui_preview(
            _require_mapping(normalized, "ui_preview", context)
        )
    normalized["feedback"] = _require_feedback_payload(
        _require_mapping(normalized, "feedback", context),
        context=f"{context}.feedback",
    )
    if "billing" in normalized:
        normalized["billing"] = _normalize_negotiation_billing(
            _require_mapping(normalized, "billing", context),
            context=f"{context}.billing",
        )
    return cast(NegotiationContent, normalized)


def _normalize_negotiation_billing(
    payload: Mapping[str, object],
    *,
    context: str,
) -> NegotiationBilling:
    mode = _require_str(payload, "mode", context)
    if mode != "relay_time":
        _raise_malformed_response(
            context, f"field 'mode' is not 'relay_time': {mode!r}", payload
        )
    unit = _require_str(payload, "unit", context)
    if unit != "points/min":
        _raise_malformed_response(
            context, f"field 'unit' is not 'points/min': {unit!r}", payload
        )
    return {
        "mode": "relay_time",
        "points_per_minute": _require_number(payload, "points_per_minute", context),
        "max_duration_required": _require_bool(
            payload, "max_duration_required", context
        ),
        "max_duration_minutes": _require_optional_number(
            payload, "max_duration_minutes", context
        ),
        "estimated_charge": _require_optional_number(
            payload, "estimated_charge", context
        ),
        "unit": "points/min",
    }


def _normalize_agent_info_payload(value: object, *, context: str) -> AgentInfo:
    info = _require_object_mapping(value, context)
    normalized: AgentInfo = {
        "input": cast(TemplateSchema, _require_mapping(info, "input", context)),
        "condition": cast(TemplateSchema, _require_mapping(info, "condition", context)),
        "output": cast(TemplateSchema, _require_mapping(info, "output", context)),
        "description": _require_str(info, "description", context),
        "module_version": _require_str(info, "module_version", context),
        "user_info_request": [
            field.value
            for field in _normalize_user_info_fields(
                cast(
                    Iterable[str | UserInfoField],
                    _require_str_list(info, "user_info_request", context),
                )
            )
        ],
    }
    if "billing" in info:
        normalized["billing"] = _normalize_negotiation_billing(
            _require_mapping(info, "billing", context),
            context=f"{context}.billing",
        )
    return normalized


def _normalize_agent_summary_payload(payload: object, *, context: str) -> AgentSummary:
    agent = _require_object_mapping(payload, context)
    return {
        "id": _require_str(agent, "id", context),
        "name": _require_str(agent, "name", context),
        "type": _require_str(agent, "type", context),
        "category": _require_str(agent, "category", context),
        "is_public": _require_bool(agent, "is_public", context),
        "point": _require_int(agent, "point", context),
        "info": _normalize_agent_info_payload(
            _require_mapping(agent, "info", context),
            context=f"{context}.info",
        ),
    }


def _normalize_agent_detail_payload(payload: object, *, context: str) -> AgentDetail:
    agent = _require_object_mapping(payload, context)
    summary = _normalize_agent_summary_payload(agent, context=context)
    return {
        **summary,
        "owner": _normalize_user_summary_payload(
            _require_mapping(agent, "owner", context),
            context=f"{context}.owner",
        ),
        "secret_key": _require_str(agent, "secret_key", context),
    }


def _normalize_board_agent_owner_payload(
    value: object, *, context: str
) -> BoardAgentOwner:
    owner = _require_object_mapping(value, context)
    return {
        "id": _require_str(owner, "id", context),
        "name": _require_str(owner, "name", context),
        "affiliation": _require_str(owner, "affiliation", context),
    }


def _normalize_user_summary_payload(value: object, *, context: str) -> UserSummary:
    user = _require_object_mapping(value, context)
    info = _require_mapping(user, "info", context)
    user_info: dict[str, str] = {}
    for key, entry in info.items():
        if not isinstance(key, str):
            _raise_malformed_response(
                context,
                f"user info key is not a string: {key!r}",
                info,
            )
        if not isinstance(entry, str):
            _raise_malformed_response(
                context,
                f"user info field '{key}' is not a string: {entry!r}",
                info,
            )
        user_info[key] = entry

    return {
        "id": _require_str(user, "id", context),
        "auth": _require_str(user, "auth", context),
        "name": _require_str(user, "name", context),
        "affiliation": _require_str(user, "affiliation", context),
        "point": _require_int(user, "point", context),
        "info": user_info,
    }


def _normalize_board_agent_payload(payload: object, *, context: str) -> BoardAgent:
    agent = _require_object_mapping(payload, context)
    return {
        "id": _require_str(agent, "id", context),
        "name": _require_str(agent, "name", context),
        "type": _require_str(agent, "type", context),
        "category": _require_str(agent, "category", context),
        "info": _normalize_agent_info_payload(
            _require_mapping(agent, "info", context),
            context=f"{context}.info",
        ),
        "is_public": _require_bool(agent, "is_public", context),
        "active": _require_bool(agent, "active", context),
        "owner": _normalize_board_agent_owner_payload(
            _require_mapping(agent, "owner", context),
            context=f"{context}.owner",
        ),
    }


def _require_object_mapping(value: object, context: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        _raise_malformed_response(
            context, f"expected a JSON object, got {value!r}", value
        )
    return cast(Mapping[str, object], value)


def _require_key(payload: Mapping[str, object], key: str, context: str) -> object:
    if key not in payload:
        _raise_malformed_response(context, f"missing '{key}'", payload)
    return payload[key]


def _require_mapping(payload: Mapping[str, object], key: str, context: str) -> JsonDict:
    value = _require_key(payload, key, context)
    if not isinstance(value, dict):
        _raise_malformed_response(
            context, f"field '{key}' is not a dict: {value!r}", payload
        )
    return value


def _require_list(
    payload: Mapping[str, object], key: str, context: str
) -> list[object]:
    value = _require_key(payload, key, context)
    if not isinstance(value, list):
        _raise_malformed_response(
            context, f"field '{key}' is not a list: {value!r}", payload
        )
    return value


def _require_str_list(
    payload: Mapping[str, object], key: str, context: str
) -> list[str]:
    value = _require_list(payload, key, context)
    if not all(isinstance(item, str) for item in value):
        _raise_malformed_response(
            context, f"field '{key}' is not a list of strings", payload
        )
    return [item for item in value if isinstance(item, str)]


def _require_int(payload: Mapping[str, object], key: str, context: str) -> int:
    value = _require_key(payload, key, context)
    if not isinstance(value, int) or isinstance(value, bool):
        _raise_malformed_response(
            context, f"field '{key}' is not an int: {value!r}", payload
        )
    return value


def _require_number(
    payload: Mapping[str, object], key: str, context: str
) -> int | float:
    value = _require_key(payload, key, context)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        _raise_malformed_response(
            context, f"field '{key}' is not a number: {value!r}", payload
        )
    return value


def _require_optional_number(
    payload: Mapping[str, object], key: str, context: str
) -> int | float | None:
    value = _require_key(payload, key, context)
    if value is None:
        return None
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        _raise_malformed_response(
            context, f"field '{key}' is not a number or null: {value!r}", payload
        )
    return value


def _require_str(payload: Mapping[str, object], key: str, context: str) -> str:
    value = _require_key(payload, key, context)
    if not isinstance(value, str):
        _raise_malformed_response(
            context, f"field '{key}' is not a string: {value!r}", payload
        )
    return value


def _require_bool(payload: Mapping[str, object], key: str, context: str) -> bool:
    value = _require_key(payload, key, context)
    if not isinstance(value, bool):
        _raise_malformed_response(
            context, f"field '{key}' is not a bool: {value!r}", payload
        )
    return value


def _require_optional_str(
    payload: Mapping[str, object], key: str, context: str
) -> str | None:
    value = _require_key(payload, key, context)
    if value is None:
        return None
    if not isinstance(value, str):
        _raise_malformed_response(
            context, f"field '{key}' is not a string or null: {value!r}", payload
        )
    return value


def _get_optional_str(
    payload: Mapping[str, object], key: str, context: str
) -> str | None:
    if key not in payload:
        return None
    return _require_optional_str(payload, key, context)


def _parse_relay_file_handle(
    value: Mapping[str, object], *, context: str
) -> RelayFileHandle:
    relay_value = _require_object_mapping(value, context)
    relay_tag = _require_mapping(relay_value, "$brokersystem", context)
    relay_kind = _require_str(relay_tag, "kind", f"{context}.$brokersystem")
    if relay_kind != "relay_file":
        _raise_malformed_response(
            context,
            f"expected relay_file metadata but got kind={relay_kind!r}",
            relay_value,
        )
    return RelayFileHandle(
        uri=_require_str(relay_value, "uri", context),
        name=_require_str(relay_value, "name", context),
        size_bytes=_require_int(relay_value, "size_bytes", context),
        content_type=_require_str(relay_value, "content_type", context),
        availability=_require_str(relay_value, "availability", context),
    )


def _parse_relay_media_handle(
    value: Mapping[str, object], *, context: str
) -> RelayMediaHandle:
    relay_value = _require_object_mapping(value, context)
    relay_tag = _require_mapping(relay_value, "$brokersystem", context)
    relay_kind = _require_str(relay_tag, "kind", f"{context}.$brokersystem")
    if relay_kind != "relay_media":
        _raise_malformed_response(
            context,
            f"expected relay_media metadata but got kind={relay_kind!r}",
            relay_value,
        )
    profiles = _parse_relay_media_profiles(relay_value, context=context)
    default_profile = _get_optional_str(relay_value, "default_profile", context)
    if default_profile is not None and all(
        profile.name != default_profile for profile in profiles
    ):
        _raise_malformed_response(
            context,
            f"default_profile={default_profile!r} is missing from relay_media profiles",
            relay_value,
        )
    return RelayMediaHandle(
        playback_uri=_require_str(relay_value, "playback_uri", context),
        download_uri=_get_optional_str(relay_value, "download_uri", context),
        name=_require_str(relay_value, "name", context),
        size_bytes=_require_int(relay_value, "size_bytes", context),
        content_type=_require_str(relay_value, "content_type", context),
        availability=_require_str(relay_value, "availability", context),
        live=_require_bool(relay_value, "live", context),
        default_profile=default_profile,
        profiles=profiles,
    )


def _parse_relay_media_profiles(
    value: Mapping[str, object], *, context: str
) -> tuple[RelayMediaProfileHandle, ...]:
    raw_profiles_obj = value.get("profiles")
    if raw_profiles_obj is None:
        return ()
    if not isinstance(raw_profiles_obj, Mapping):
        _raise_malformed_response(context, "field 'profiles' is not an object", value)
    profiles = cast(Mapping[object, object], raw_profiles_obj)
    parsed: list[RelayMediaProfileHandle] = []
    for name, profile_obj in profiles.items():
        if not isinstance(name, str) or name == "":
            _raise_malformed_response(
                context,
                "relay_media profiles must use non-empty string names",
                value,
            )
        profile = _require_object_mapping(profile_obj, f"{context}.profiles.{name}")
        parsed.append(
            RelayMediaProfileHandle(
                name=name,
                playback_uri=_require_str(
                    profile, "playback_uri", f"{context}.profiles.{name}"
                ),
                content_type=_require_str(
                    profile, "content_type", f"{context}.profiles.{name}"
                ),
            )
        )
    return tuple(parsed)


def _parse_relay_session_handle(
    value: Mapping[str, object], *, context: str
) -> RelaySessionHandle:
    relay_value = _require_object_mapping(value, context)
    relay_tag = _require_mapping(relay_value, "$brokersystem", context)
    relay_kind = _require_str(relay_tag, "kind", f"{context}.$brokersystem")
    if relay_kind != "relay_session":
        _raise_malformed_response(
            context,
            f"expected relay_session metadata but got kind={relay_kind!r}",
            relay_value,
        )
    return RelaySessionHandle(
        session_uri=_require_str(relay_value, "session_uri", context),
        name=_require_str(relay_value, "name", context),
        protocol=_require_str(relay_value, "protocol", context),
        availability=_require_str(relay_value, "availability", context),
    )


def _is_local_file_value(value: object) -> bool:
    return isinstance(value, (bytes, bytearray, memoryview, PathLike))


def _normalize_file_type(file_type: str) -> str:
    return file_type.strip().lower().lstrip(".")


def _resolve_upload_workers(total_items: int, max_upload_workers: int | None) -> int:
    if total_items <= 0:
        return 0
    if max_upload_workers is None:
        return min(4, total_items)
    if max_upload_workers < 1:
        raise ValueError("max_upload_workers must be >= 1")
    return min(max_upload_workers, total_items)


def _upload_parallel_map(
    items: Mapping[str, R],
    *,
    max_upload_workers: int | None,
    upload_one: Callable[[str, R], Any],
) -> dict[str, Any]:
    ordered_items = list(items.items())
    if not ordered_items:
        return {}

    workers = _resolve_upload_workers(len(ordered_items), max_upload_workers)
    if workers <= 1:
        return {key: upload_one(key, value) for key, value in ordered_items}

    completed: dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(upload_one, key, value): key for key, value in ordered_items
        }
        for future in as_completed(futures):
            key = futures[future]
            completed[key] = future.result()
    return {key: completed[key] for key, _value in ordered_items}


def _backoff_seconds(attempt: int, base: float, cap: float) -> float:
    if base <= 0 or cap <= 0:
        return 0.0
    return min(cap, base * (2**attempt))


def _retryable_http_status(status_code: int) -> bool:
    return status_code in {408, 425, 429, 500, 502, 503, 504}


def _retry_deadline_remaining(started_at: float, deadline_seconds: float) -> float:
    return deadline_seconds - (time.perf_counter() - started_at)


def _retry_after_seconds(
    headers: Mapping[str, str],
    *,
    now: datetime | None = None,
) -> float | None:
    raw_value = headers.get("Retry-After")
    if raw_value is None:
        return None
    value = raw_value.strip()
    if not value:
        return None
    try:
        return max(float(value), 0.0)
    except ValueError:
        pass
    try:
        retry_at = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    reference_time = datetime.now(timezone.utc) if now is None else now
    return max((retry_at - reference_time).total_seconds(), 0.0)


def _retry_delay_seconds(
    attempt: int,
    base: float,
    cap: float,
    *,
    status_code: int | None = None,
    retry_after: float | None = None,
) -> float:
    if retry_after is not None:
        return max(retry_after, 0.0)
    effective_base = base
    effective_cap = cap
    if status_code == 429:
        effective_base = max(base, DEFAULT_429_RETRY_BASE_SECONDS)
        effective_cap = max(cap, DEFAULT_429_RETRY_MAX_SECONDS)
    return _backoff_seconds(attempt, effective_base, effective_cap)


def _create_http_session(
    *, pool_maxsize: int = DEFAULT_HTTP_POOL_MAXSIZE
) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=pool_maxsize,
        pool_maxsize=pool_maxsize,
        max_retries=0,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _get_thread_session(
    storage: threading.local,
    *,
    pool_maxsize: int = DEFAULT_HTTP_POOL_MAXSIZE,
) -> requests.Session:
    session = getattr(storage, "session", None)
    if session is None:
        session = _create_http_session(pool_maxsize=pool_maxsize)
        storage.session = session
    return cast(requests.Session, session)


def _sleep_for_retry(
    started_at: float,
    deadline_seconds: float,
    attempt: int,
    base: float,
    cap: float,
    *,
    status_code: int | None = None,
    retry_after: float | None = None,
) -> bool:
    remaining = _retry_deadline_remaining(started_at, deadline_seconds)
    if remaining <= 0:
        return False
    delay = min(
        remaining,
        _retry_delay_seconds(
            attempt,
            base,
            cap,
            status_code=status_code,
            retry_after=retry_after,
        ),
    )
    if delay <= 0:
        return False
    time.sleep(delay)
    return True


def _request_timeout_tuple(
    started_at: float,
    deadline_seconds: float,
    *,
    connect_timeout: float,
    read_timeout_cap: float,
) -> tuple[float, float]:
    remaining = max(1.0, _retry_deadline_remaining(started_at, deadline_seconds))
    return (
        min(connect_timeout, remaining),
        min(read_timeout_cap, remaining),
    )


def _normalize_user_info_fields(
    fields: Iterable[str | UserInfoField],
) -> list[UserInfoField]:
    """Normalize user info request fields to supported values.

    Raises:
        TypeError: If a field is not a string or UserInfoField.
        ValueError: If a field is not a supported user info field.
    """
    normalized: list[UserInfoField] = []
    allowed_values = ", ".join(field.value for field in USER_INFO_FIELDS)
    for field in fields:
        if isinstance(field, UserInfoField):
            value = field
        elif isinstance(field, str):
            try:
                value = UserInfoField(field)
            except ValueError as exc:
                raise ValueError(
                    f"Unsupported user_info_request field: {field!r}. "
                    f"Allowed: {allowed_values}"
                ) from exc
        else:
            raise TypeError(
                "user_info_request fields must be strings or UserInfoField: "
                f"{field!r}"
            )

        if value not in USER_INFO_FIELDS:
            raise ValueError(
                f"Unsupported user_info_request field: {value!r}. "
                f"Allowed: {allowed_values}"
            )
        if value not in normalized:
            normalized.append(value)
    return normalized


def _normalize_user_info_payload(payload: Any) -> UserInfo:
    """Normalize user info payload to ensure all keys exist."""
    if not isinstance(payload, Mapping):
        payload = {}
    name_affiliation = payload.get("name_affiliation")
    if isinstance(name_affiliation, Mapping):
        name = name_affiliation.get("name")
        affiliation = name_affiliation.get("affiliation")
    else:
        name = None
        affiliation = None
    return {
        "user_id": payload.get("user_id"),
        "email": payload.get("email"),
        "name_affiliation": {"name": name, "affiliation": affiliation},
    }


def _normalize_ui_preview(value: Mapping[str, Any]) -> UiPreview:
    """Validate and normalize agent-provided UI preview declarations.

    Raises:
        TypeError: If ui_preview is not a mapping, or if its fields are malformed.
        ValueError: If ui_preview has an unsupported `type`.
    """
    preview = dict(value)
    preview_type = preview.get("type")
    if preview_type != "vega":
        raise ValueError(
            "ui_preview.type must be 'vega' when set " f"(got {preview_type!r})"
        )
    spec = preview.get("spec")
    if not isinstance(spec, Mapping):
        raise TypeError("ui_preview.spec must be a mapping when set")
    preview["spec"] = dict(spec)
    return cast(UiPreviewVega, preview)


def _normalize_feedback_kind(value: object) -> NegotiationFeedbackKind | None:
    if value in ("agent", "validation"):
        return cast(NegotiationFeedbackKind, value)
    return None


def _build_feedback(
    message: str | None = None,
    fields: Mapping[str, str] = {},
    *,
    kind: NegotiationFeedbackKind = "agent",
) -> NegotiationFeedback:
    """Build a local feedback payload in the public negotiation shape."""
    normalized_fields: dict[str, str] = {}
    for key, value in fields.items():
        if not isinstance(key, str):
            raise TypeError(f"feedback field names must be strings: {key!r}")
        if not isinstance(value, str):
            raise TypeError(f"feedback field messages must be strings: {value!r}")
        key_text = key.strip()
        value_text = value.strip()
        if key_text and value_text:
            normalized_fields[key_text] = value_text
    return {
        "kind": kind,
        "message": message.strip() if message is not None else "",
        "fields": normalized_fields,
    }


def _normalize_job_error_detail_level(value: object) -> JobErrorDetailLevel:
    if value in ("summary", "traceback"):
        return cast(JobErrorDetailLevel, value)
    raise ValueError(
        "job_error_detail_level must be 'summary' or 'traceback' " f"(got {value!r})"
    )


def _job_error_summary(exc: BaseException) -> str:
    error_type = type(exc).__name__
    error_message = str(exc).strip()
    if error_message:
        return f"{error_type}: {error_message}"
    return error_type


def _job_error_result(
    exc: BaseException, detail_level: JobErrorDetailLevel
) -> JsonDict:
    error_type = type(exc).__name__
    error_message = str(exc).strip() or repr(exc)
    value_keys = ["error_type", "error_message"]

    result: JsonDict = {
        "@keys": list(value_keys),
        "@value": list(value_keys),
        "@type": {
            "error_type": "string",
            "error_message": "string",
        },
        "@help": {
            "error_type": "Exception class raised by the agent job.",
            "error_message": "Exception message raised by the agent job.",
        },
        "@unit": {},
        "error_type": error_type,
        "error_message": error_message,
    }

    if detail_level == "traceback":
        traceback_text = "".join(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        ).rstrip()
        result["@keys"].append("traceback")
        result["@value"].append("traceback")
        cast(dict[str, str], result["@type"])["traceback"] = "string"
        cast(dict[str, str], result["@help"])[
            "traceback"
        ] = "Python traceback captured from the agent process."
        result["traceback"] = traceback_text

    return result


def _require_feedback_payload(
    payload: Mapping[str, object], *, context: str
) -> NegotiationFeedback:
    kind_value = _normalize_feedback_kind(_require_key(payload, "kind", context))
    if kind_value is None:
        _raise_malformed_response(
            context,
            f"field 'kind' is not one of ['agent', 'validation']: "
            f"{payload.get('kind')!r}",
            payload,
        )

    fields_mapping = _require_mapping(payload, "fields", context)
    fields: dict[str, str] = {}
    for key, value in fields_mapping.items():
        if not isinstance(key, str):
            _raise_malformed_response(
                context,
                f"feedback field name is not a string: {key!r}",
                fields_mapping,
            )
        if not isinstance(value, str):
            _raise_malformed_response(
                context,
                f"feedback field '{key}' is not a string: {value!r}",
                fields_mapping,
            )
        fields[key] = value

    return {
        "kind": kind_value,
        "message": _require_str(payload, "message", context),
        "fields": fields,
    }


def _feedback_response(
    status: NegotiationStatus,
    *,
    message: str | None = None,
    fields: Mapping[str, str] = {},
) -> NegotiationDecision:
    return status, _build_feedback(message, fields, kind="agent")


def ok_response(
    *,
    message: str | None = None,
    fields: Mapping[str, str] = {},
) -> tuple[Literal["ok"], NegotiationFeedback]:
    """Return an `ok` negotiation decision with optional feedback.

    This returns a negotiation helper result, not a raw broker transport
    envelope such as `{"status": "error", ...}`.
    """
    return cast(
        tuple[Literal["ok"], NegotiationFeedback],
        _feedback_response("ok", message=message, fields=fields),
    )


def need_revision_response(
    *,
    message: str | None = None,
    fields: Mapping[str, str] = {},
) -> tuple[Literal["need_revision"], NegotiationFeedback]:
    """Return a `need_revision` negotiation decision with optional feedback.

    This returns a negotiation helper result, not a raw broker transport
    envelope such as `{"status": "error", ...}`.
    """
    return cast(
        tuple[Literal["need_revision"], NegotiationFeedback],
        _feedback_response("need_revision", message=message, fields=fields),
    )


def ng_response(
    *,
    message: str | None = None,
    fields: Mapping[str, str] = {},
) -> tuple[Literal["ng"], NegotiationFeedback]:
    """Return an `ng` negotiation decision with optional feedback.

    This returns a negotiation helper result, not a raw broker transport
    envelope such as `{"status": "error", ...}`.
    """
    return cast(
        tuple[Literal["ng"], NegotiationFeedback],
        _feedback_response("ng", message=message, fields=fields),
    )


def revision_response(
    *,
    message: str | None = None,
    fields: Mapping[str, str] = {},
) -> tuple[Literal["need_revision"], NegotiationFeedback]:
    """Alias for `need_revision_response(...)`."""
    return need_revision_response(message=message, fields=fields)


def _attach_feedback(
    response: JsonDict,
    status: NegotiationStatus,
    feedback: NegotiationFeedback | None,
) -> JsonDict:
    payload = dict(response)
    normalized = (
        {
            "kind": feedback["kind"],
            "message": feedback["message"],
            "fields": dict(feedback["fields"]),
        }
        if feedback is not None
        else _build_feedback(kind="agent")
    )

    default_message = None
    if normalized["message"] == "" and status == "need_revision":
        default_message = "The agent asked for revisions before it can continue."
    if normalized["message"] == "" and status == "ng":
        default_message = "The agent rejected this request."
    if default_message is not None:
        normalized["message"] = default_message

    payload["feedback"] = normalized
    payload.pop("revision", None)
    # For non-ok negotiation outcomes, propagate the round-level feedback
    # message into the canonical broker error detail field.
    if status in ["need_revision", "ng"] and normalized["message"] != "":
        payload["error_msg"] = normalized["message"]
    elif status == "ok":
        payload.pop("error_msg", None)

    return payload


def _relay_socket_url(broker_url: str) -> str:
    """Build the broker relay WebSocket URL from the broker base URL."""
    parsed = urlsplit(broker_url)
    if parsed.scheme == "https":
        ws_scheme = "wss"
    elif parsed.scheme == "http":
        ws_scheme = "ws"
    elif parsed.scheme in ("ws", "wss"):
        ws_scheme = parsed.scheme
    else:
        raise ValueError(
            f"Unsupported broker URL scheme for relay socket: {parsed.scheme}"
        )

    relay_path = parsed.path.rstrip("/") + "/api/v1/agent/relay/socket"
    return urlunsplit((ws_scheme, parsed.netloc, relay_path, "", ""))


def _safe_debug_value(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return f"<{len(value)} bytes>"
    if isinstance(value, dict):
        return {key: _safe_debug_value(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_safe_debug_value(item) for item in value]
    return value


class ValueTemplate:
    """Base schema item for inputs/conditions/outputs.

    Stores the value type, optional unit, and type-specific constraints.
    Subclasses implement type-specific casting and output formatting.
    """

    def __init__(self, unit: str | None = None, help: str | None = None) -> None:
        self.type: str = self.__class__.__name__.lower()
        self.unit: str | None = unit
        self.help: str | None = help
        self.constraint_dict: dict[str, Any] = {}
        self.item_type: str | None = None

    def set_constraint(self, key: str, value: Any) -> None:
        """Register a constraint if the value is not None."""
        if value is not None:
            self.constraint_dict[key] = value

    @property
    def format_dict(self) -> JsonDict:
        """Return the schema fragment used by the broker protocol."""
        format_dict = {"@type": self.type, "@unit": self.unit}
        if self.help is not None:
            format_dict["@help"] = self.help
        if self.item_type == "input":
            format_dict.update(
                {"@necessity": "required", "@constraints": self.constraint_dict}
            )
        if self.item_type == "output":
            format_dict.update({})
        return format_dict

    def cast(self, value: Any) -> Any:
        """Cast/validate a request value for this template."""
        return value

    def format_for_output(
        self,
        value: Any,
        uploader: Callable[[str, bytes], JsonDict],
        relay_registrar: RelayRegistrar | None = None,
    ) -> tuple[Any, JsonDict]:
        """Format an output value and return (value, format_dict)."""
        format_dict = {"@type": self.type, "@unit": self.unit}
        if self.help is not None:
            format_dict["@help"] = self.help
        return value, format_dict

    def set_item_type(self, item_type: str) -> None:
        """Mark whether this template is used for input/condition/output."""
        self.item_type = item_type

    @classmethod
    def guess_from_value(
        cls,
        value: Any,
        unit_callback_func: Callable[[str], str | None] | None = None,
        key: str | None = None,
    ) -> "ValueTemplate":
        """Infer a template class from a Python value."""
        unit = None
        if isinstance(value, bool):
            return Bool(value)
        if isinstance(value, (int, float)):
            if unit_callback_func is not None and key is not None:
                unit = unit_callback_func(key)
            return Number(value, unit=unit)
        if isinstance(value, str):
            return String(value)

        if (
            isinstance(value, pd.DataFrame)
            or (isinstance(value, list) and all([isinstance(x, dict) for x in value]))
            or (
                isinstance(value, dict)
                and all([isinstance(x, list) for x in value.values()])
            )
        ):
            if not isinstance(value, pd.DataFrame):
                value = pd.DataFrame(value)
            unit_dict = dict[str, str | None]()
            for column in value.columns:
                if unit_callback_func is not None and column is not None:
                    unit_dict[column] = unit_callback_func(column)
                else:
                    unit_dict[column] = None
            graph = None
            if len(value.columns) >= 2:
                graph = {"x": value.columns[0], "y": value.columns[1]}
            return Table(unit_dict=unit_dict, graph=graph)

        if isinstance(value, list):
            return Choice(value)

        assert False, f"Cannot find a matching value template... {value}:{type(value)}"

    @classmethod
    def from_dict(
        cls, format_dict: JsonDict, unit_dict: dict[str, str | None]
    ) -> "ValueTemplate":
        """Build a template from an API schema dict."""
        match format_dict["@type"]:
            case "number":
                template = Number()
            case "bool":
                template = Bool()
            case "string":
                template = String()
            case "choice":
                template = Choice(format_dict["@constraints"]["choices"])
            case "table":
                unit_dict = {key: unit_dict[key] for key in format_dict["@table"]}
                graph = None
                if "@repr" in format_dict and format_dict["@repr"]["type"] == "graph":
                    graph = {
                        "x": format_dict["@repr"]["key_x"],
                        "y": format_dict["@repr"]["key_y"],
                    }
                template = Table(unit_dict=unit_dict, graph=graph)
            case "relay_file":
                template = RelayFile()
            case "relay_media":
                template = RelayMedia()
            case "relay_session":
                template = RelaySession()
            case _ as unknown_type:
                raise NotImplementedError(f"Unknown type: {unknown_type}")
        if "@constraints" in format_dict:
            for key, value in format_dict["@constraints"].items():
                template.set_constraint(key, value)
        if "@unit" in format_dict:
            template.unit = format_dict["@unit"]
        if "@help" in format_dict:
            template.help = format_dict["@help"]

        return template


class Number(ValueTemplate):
    """Numeric template with optional min/max/step constraints."""

    def __init__(
        self,
        value: int | float | None = None,
        unit: str | None = None,
        min: int | float | None = None,
        max: int | float | None = None,
        step: int | float | None = None,
        help: str | None = None,
    ) -> None:
        """Initialize numeric constraints.

        Args:
            value: Default value.
            unit: Unit label for UI display.
            min: Inclusive minimum bound.
            max: Inclusive maximum bound.
            step: Optional UI hint for increment/decrement controls.
                Direct user input may still use non-multiple values.
            help: Optional helper text for UI display.
        """
        super().__init__(unit, help=help)
        if step is not None:
            if isinstance(step, bool):
                raise TypeError("step must not be bool")
            if not isinstance(step, (int, float)):
                raise TypeError("step must be int or float")
            if step <= 0:
                raise ValueError("step must be > 0")
        self.set_constraint("default", value)
        self.set_constraint("min", min)
        self.set_constraint("max", max)
        self.set_constraint("step", step)

    def cast(self, value: Any) -> int | float:
        """Cast to int/float and validate min/max constraints."""
        # Preserve fractional parts. (int(0.5) == 0 would silently corrupt user input.)
        if isinstance(value, bool):
            raise TypeError("number value must not be bool")

        if isinstance(value, (int, float)):
            pass
        elif isinstance(value, str):
            s = value.strip()
            if re.fullmatch(r"[+-]?\d+", s):
                value = int(s)
            else:
                value = float(s)
        else:
            # Accept other numeric-like values (e.g., Decimal, numpy scalars) as float.
            value = float(value)
        assert not (
            "min" in self.constraint_dict and value < self.constraint_dict["min"]
        )
        assert not (
            "max" in self.constraint_dict and value > self.constraint_dict["max"]
        )
        return value


class Range(ValueTemplate):
    """Range template expressed as min/max for UI purposes."""

    def __init__(
        self,
        range_min: int | float | None = None,
        range_max: int | float | None = None,
        unit: str | None = None,
        help: str | None = None,
    ) -> None:
        """Initialize a range-like template with optional UI help text.

        Args:
            range_min: Lower bound shown in the default range payload.
            range_max: Upper bound shown in the default range payload.
            unit: Unit label for UI display.
            help: Optional helper text for UI display.
        """
        super().__init__(unit, help=help)
        self.set_constraint("default", {"min": range_min, "max": range_max})


class String(ValueTemplate):
    """String template with an optional default value."""

    def __init__(self, string: str | None = None, help: str | None = None) -> None:
        """Initialize a string template.

        Args:
            string: Default string value.
            help: Optional helper text for UI display.
        """
        super().__init__(help=help)
        self.set_constraint("default", string)


class Bool(ValueTemplate):
    """Boolean template with an optional default value."""

    def __init__(self, value: bool | None = None, help: str | None = None) -> None:
        """Initialize a boolean template.

        Args:
            value: Default boolean value.
            help: Optional helper text for UI display.
        """
        super().__init__(help=help)
        if value is not None and not isinstance(value, bool):
            raise TypeError("bool default must be a bool")
        self.set_constraint("default", value)

    def cast(self, value: Any) -> bool:
        """Cast to bool and accept common string literals."""
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in ["true", "1", "yes", "on"]:
                return True
            if normalized in ["false", "0", "no", "off"]:
                return False
        raise TypeError("bool value must be a bool")


class File(ValueTemplate):
    """File template supporting binary uploads and file ids."""

    def __init__(self, file_type: str, help: str | None = None) -> None:
        """Initialize a file or image template.

        Args:
            file_type: File extension/type handled by the broker.
            help: Optional helper text for UI display.
        """
        super().__init__(help=help)
        if file_type in ["png", "jpg", "jpeg", "gif"]:
            self.type = "image"
        self.file_type = file_type
        self.set_constraint("file_type", file_type)

    def format_for_output(
        self,
        value: UploadedFile | UploadValue,
        uploader: Callable[[str, bytes], JsonDict],
        relay_registrar: RelayRegistrar | None = None,
    ) -> tuple[Any, JsonDict]:
        """Upload a file and return the API file id."""
        if isinstance(value, UploadedFile):
            expected_type = _normalize_file_type(self.file_type)
            actual_type = _normalize_file_type(value.file_type)
            if actual_type != expected_type:
                raise TypeError(
                    f"UploadedFile file_type mismatch: expected {self.file_type!r}, got {value.file_type!r}"
                )
            return value.file_id, self.format_dict

        if isinstance(value, (bytes, bytearray, memoryview)):
            binary_data = value
        else:
            try:
                path = Path(value)
            except Exception as e:
                raise TypeError(
                    f"a File value must be a file path but got: {value} (type: {type(value)}); {e}"
                )
            match self.type:
                case "image":
                    match self.file_type:
                        case "png" | "jpeg" | "jpg":
                            img = PIL.Image.open(path)
                            output = io.BytesIO()
                            format_name = (
                                "jpeg"
                                if self.file_type in ["jpeg", "jpg"]
                                else self.file_type
                            )
                            img.save(output, format=format_name)
                            binary_data = output.getvalue()
                        case "gif":
                            with path.open("rb") as f:
                                binary_data = f.read()
                        case _ as unknown_type:
                            raise NotImplementedError(
                                f"Unknown image file type: {unknown_type}"
                            )
                case "file":
                    with path.open("rb") as f:
                        binary_data = f.read()
                case _ as unknown_type:
                    raise NotImplementedError(f"Unknown file type: {unknown_type}")

        if not isinstance(binary_data, bytes):
            binary_data = bytes(binary_data)
        response = uploader(self.file_type, binary_data)
        logger.debug("upload result: %s", response)
        if "file_id" not in response:
            raise RuntimeError(f"Upload failed; response={response!r}")
        value = response["file_id"]

        return value, self.format_dict


class RelayFile(ValueTemplate):
    """Relay-backed file template for large artifacts kept on the agent host."""

    def __init__(
        self,
        name: str | None = None,
        content_type: str | None = None,
        help: str | None = None,
    ) -> None:
        super().__init__(help=help)
        self.type = "relay_file"
        self.name = name
        self.content_type = content_type

    def format_for_output(
        self,
        value: PathLike[str] | str,
        uploader: Callable[[str, bytes], JsonDict],
        relay_registrar: RelayRegistrar | None = None,
    ) -> tuple[Any, JsonDict]:
        """Register a relay source and return tagged relay metadata."""
        if relay_registrar is None:
            raise RuntimeError("Relay registrar is required for RelayFile outputs")

        try:
            path = Path(value)
        except Exception as exc:
            raise TypeError(
                f"a RelayFile value must be a file path but got: {value} (type: {type(value)}); {exc}"
            ) from exc

        metadata = relay_registrar(path, self.name, self.content_type)
        if not isinstance(metadata, dict):
            raise RuntimeError(
                f"Relay registration returned invalid metadata: {metadata!r}"
            )
        return metadata, self.format_dict


class RelayMedia(ValueTemplate):
    """Relay-backed media template for playable stored or live media.

    Use plain `content_type` for single-format media such as a standalone file or
    a simple HLS relay. Use `profiles` when one relay-backed asset tree exposes
    multiple playback manifests, for example DASH plus HLS on CMAF.
    """

    def __init__(
        self,
        name: str | None = None,
        content_type: str | None = None,
        live: bool = False,
        profiles: Mapping[str, RelayMediaProfile] | None = None,
        default_profile: str | None = None,
        help: str | None = None,
    ) -> None:
        super().__init__(help=help)
        self.type = "relay_media"
        self.name = name
        self.content_type = content_type
        self.live = live
        self.profiles = dict(profiles or {})
        """Named playback manifests exposed from one relay asset tree."""
        self.default_profile = default_profile
        """Profile name used for the top-level playback URI/content type."""

    def format_for_output(
        self,
        value: PathLike[str] | str | RelayStreamSource | RelayAssetSource,
        uploader: Callable[[str, bytes], JsonDict],
        relay_registrar: RelayRegistrar | None = None,
    ) -> tuple[Any, JsonDict]:
        """Register a relay media source and return tagged media metadata.

        When `profiles` is configured, `value` must be a `RelayAssetSource` so
        each profile can point at one manifest under the same rooted asset tree.
        """
        if relay_registrar is None:
            raise RuntimeError("Relay registrar is required for RelayMedia outputs")

        effective_content_type = self.content_type
        profile_metadata = self._build_profile_metadata(value)
        if effective_content_type is None and profile_metadata is not None:
            default_profile = profile_metadata["default_profile"]
            effective_content_type = profile_metadata["profiles"][default_profile][
                "content_type"
            ]

        if self.live and isinstance(value, (RelayStreamSource, RelayAssetSource)):
            metadata = relay_registrar(value, self.name, effective_content_type)
        else:
            try:
                path_value = cast(PathLike[str] | str, value)
                path = Path(path_value)
            except Exception as exc:
                raise TypeError(
                    "a RelayMedia value must be a file path, RelayStreamSource,"
                    " or RelayAssetSource"
                    f" but got: {value} (type: {type(value)}); {exc}"
                ) from exc

            metadata = relay_registrar(path, self.name, effective_content_type)
        if not isinstance(metadata, dict):
            raise RuntimeError(
                f"Relay registration returned invalid metadata: {metadata!r}"
            )

        tagged_metadata = dict(metadata)
        relay_tag = tagged_metadata.get("$brokersystem")
        if isinstance(relay_tag, dict):
            relay_tag = dict(relay_tag)
            relay_tag["kind"] = "relay_media"
            tagged_metadata["$brokersystem"] = relay_tag
        tagged_metadata["live"] = self.live
        if profile_metadata is not None:
            tagged_metadata["profiles"] = profile_metadata["profiles"]
            tagged_metadata["default_profile"] = profile_metadata["default_profile"]
            tagged_metadata["entry_path"] = profile_metadata["profiles"][
                profile_metadata["default_profile"]
            ]["entry_path"]
            tagged_metadata["content_type"] = profile_metadata["profiles"][
                profile_metadata["default_profile"]
            ]["content_type"]
        return tagged_metadata, self.format_dict

    def _build_profile_metadata(
        self, value: PathLike[str] | str | RelayStreamSource | RelayAssetSource
    ) -> RelayMediaProfilesMetadata | None:
        if not self.profiles:
            return None
        if not isinstance(value, RelayAssetSource):
            raise TypeError(
                "RelayMedia profiles require a RelayAssetSource value so that each"
                " playback profile can point at a file under one rooted asset tree."
            )

        raw_profiles: dict[str, RelayMediaProfileMetadata] = {
            name: {
                "entry_path": self._normalize_profile_entry_path(profile.entry_path),
                "content_type": profile.content_type,
            }
            for name, profile in self.profiles.items()
        }
        default_profile = self._resolve_default_profile(value, raw_profiles)
        return {
            "default_profile": default_profile,
            "profiles": raw_profiles,
        }

    def _resolve_default_profile(
        self,
        value: RelayAssetSource,
        profiles: Mapping[str, RelayMediaProfileMetadata],
    ) -> str:
        if self.default_profile is not None:
            if self.default_profile not in profiles:
                raise ValueError(
                    "RelayMedia default_profile must match one of the declared"
                    f" profiles but got: {self.default_profile!r}"
                )
            return self.default_profile

        for name, profile in profiles.items():
            if profile["entry_path"] == value.entry_path:
                return name

        if len(profiles) == 1:
            return next(iter(profiles))

        raise ValueError(
            "RelayMedia with multiple profiles needs default_profile or a"
            " RelayAssetSource entry_path that matches one declared profile."
        )

    def _normalize_profile_entry_path(self, entry_path: str) -> str:
        normalized = str(PurePosixPath(entry_path))
        if (
            normalized in {"", ".", ".."}
            or normalized.startswith("../")
            or normalized.startswith("/")
        ):
            raise ValueError(f"Invalid RelayMedia profile entry_path: {entry_path!r}")
        return normalized


class RelaySession(ValueTemplate):
    """Relay-backed text session template for interactive outputs."""

    def __init__(
        self,
        name: str | None = None,
        protocol: str = "text",
        help: str | None = None,
    ) -> None:
        super().__init__(help=help)
        self.type = "relay_session"
        self.name = name
        self.protocol = protocol

    def format_for_output(
        self,
        value: RelaySessionSource,
        uploader: Callable[[str, bytes], JsonDict],
        relay_registrar: RelayRegistrar | None = None,
    ) -> tuple[Any, JsonDict]:
        """Register a relay text session and return tagged session metadata."""
        _ = uploader
        if relay_registrar is None:
            raise RuntimeError("Relay registrar is required for RelaySession outputs")
        if not isinstance(value, RelaySessionSource):
            raise TypeError(
                "a RelaySession value must be a RelaySessionSource "
                f"but got: {value} (type: {type(value)})"
            )
        metadata = relay_registrar(value, self.name, self.protocol)
        if not isinstance(metadata, dict):
            raise RuntimeError(
                f"Relay registration returned invalid metadata: {metadata!r}"
            )

        tagged_metadata = dict(metadata)
        relay_tag = tagged_metadata.get("$brokersystem")
        if isinstance(relay_tag, dict):
            relay_tag = dict(relay_tag)
            relay_tag["kind"] = "relay_session"
            tagged_metadata["$brokersystem"] = relay_tag
        tagged_metadata["protocol"] = self.protocol
        return tagged_metadata, self.format_dict


class Choice(ValueTemplate):
    """Choice template with a fixed list of valid values."""

    def __init__(
        self,
        choices: Iterable[int | float | str],
        unit: str | None = None,
        help: str | None = None,
    ) -> None:
        """Initialize a choice template.

        Args:
            choices: Allowed values for the parameter.
            unit: Unit label for UI display.
            help: Optional helper text for UI display.
        """
        super().__init__(unit, help=help)
        choice_list = list(choices)
        if len(choice_list) == 0:
            raise ValueError("choices must not be empty")
        self.set_constraint("choices", choice_list)

    def cast(self, value: Any) -> int | float | str:
        """Cast input to the appropriate type (based on choices) and validate membership."""
        if isinstance(value, bool):
            raise TypeError("choice value must not be bool")

        choices = self.constraint_dict["choices"]
        has_string_choice = any(isinstance(choice, str) for choice in choices)

        # Only coerce string inputs when the choice-set is purely numeric.
        if isinstance(value, str) and not has_string_choice:
            s = value.strip()
            if re.fullmatch(r"[+-]?\d+", s):
                value = int(s)
            else:
                value = float(s)
        assert value in self.constraint_dict["choices"]
        return value


class Table(ValueTemplate):
    """Table template for tabular outputs (supports graph metadata)."""

    def __init__(
        self,
        unit_dict: Mapping[str, str | None],
        graph: Mapping[str, Any] | None = None,
        help: str | None = None,
    ) -> None:
        """Initialize a table template.

        Args:
            unit_dict: Units for each numeric column.
            graph: Optional graph metadata for API-rendered previews.
            help: Optional helper text for UI display.
        """
        super().__init__(unit=None, help=help)
        self.unit_dict: dict[str, str | None] = dict(unit_dict)
        self.graph: Mapping[str, Any] | None = graph

    def cast(self, value: Any) -> Any:
        """Table inputs are validated by higher-level logic."""
        return value

    @staticmethod
    def _normalize_cell(value: Any) -> Any:
        """Normalize transport-facing table cells while preserving null semantics."""
        if value is None:
            return None
        try:
            if pd.isna(value):
                return None
        except TypeError:
            pass
        return value

    def _normalize_dataframe(self, value: pd.DataFrame) -> dict[str, list[Any]]:
        declared_columns = list(self.unit_dict.keys())
        observed_columns = [str(column) for column in value.columns]
        columns = declared_columns + [
            column for column in observed_columns if column not in declared_columns
        ]
        rows = value.to_dict(orient="records")
        return self._normalize_row_dicts(rows, columns=columns)

    def _normalize_row_dicts(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        columns: list[str] | None = None,
    ) -> dict[str, list[Any]]:
        declared_columns = list(self.unit_dict.keys())
        if columns is None:
            observed_columns: list[str] = []
            seen_columns = set[str]()
            for row in rows:
                for key in row.keys():
                    if key not in seen_columns:
                        observed_columns.append(key)
                        seen_columns.add(key)
            columns = declared_columns + [
                column for column in observed_columns if column not in declared_columns
            ]

        normalized: dict[str, list[Any]] = {column: [] for column in columns}
        for row in rows:
            for column in columns:
                normalized[column].append(self._normalize_cell(row.get(column)))
        return normalized

    def _normalize_column_mapping(
        self, value: Mapping[str, list[Any]]
    ) -> dict[str, list[Any]]:
        declared_columns = list(self.unit_dict.keys())
        observed_columns = [str(column) for column in value.keys()]
        columns = declared_columns + [
            column for column in observed_columns if column not in declared_columns
        ]
        lengths = {len(column_values) for column_values in value.values()}
        if not lengths:
            return {column: [] for column in columns}
        if len(lengths) != 1:
            raise Exception("Table column lists must all have the same length")

        row_count = next(iter(lengths))
        normalized: dict[str, list[Any]] = {}
        for column in columns:
            if column in value:
                normalized[column] = [
                    self._normalize_cell(cell) for cell in value[column]
                ]
            else:
                normalized[column] = [None] * row_count
        return normalized

    @property
    def format_dict(self) -> JsonDict:
        format_dict = super().format_dict
        format_dict["@table"] = list(self.unit_dict.keys())
        format_dict["_unit_dict"] = self.unit_dict
        if self.graph is not None:
            format_dict["@repr"] = {
                "type": "graph",
                "key_x": self.graph["x"],
                "key_y": self.graph["y"],
            }
        return format_dict

    def format_for_output(
        self,
        value: Any,
        uploader: Callable[[str, bytes], JsonDict],
        relay_registrar: RelayRegistrar | None = None,
    ) -> tuple[Any, JsonDict]:
        """Convert a table value into the broker output schema."""
        format_dict = self.format_dict
        if isinstance(value, pd.DataFrame):
            value = self._normalize_dataframe(value)
        elif (
            isinstance(value, list) and all([isinstance(x, dict) for x in value])
        ) or (
            isinstance(value, dict)
            and all([isinstance(x, list) for x in value.values()])
        ):
            if isinstance(value, list):
                value = self._normalize_row_dicts(value)
            else:
                value = self._normalize_column_mapping(value)
        else:
            raise Exception(
                "The return value for table should be given by a list of dict, a dict of list or pandas DataFrame"
            )

        keys = list(value.keys())
        if self.graph is None and len(keys) >= 2:
            format_dict["@repr"] = {"type": "graph", "key_x": keys[0], "key_y": keys[1]}

        return value, format_dict


class TemplateContainer:
    """Holds ValueTemplate objects for a specific schema section."""

    def __init__(self, item_type: str) -> None:
        self._container_dict: dict[str, ValueTemplate] = {}
        self._item_type: str = item_type

    def __setattr__(self, key: str, value: Any) -> None:
        if key.startswith("_"):
            super().__setattr__(key, value)
            return
        if not isinstance(value, ValueTemplate):
            logger.exception(
                f"You can only set ValueTemplate object(such as Number or Table) for input, condition and output: {key} ({type(value)})",
            )
            return
        self._container_dict[key] = value
        value.set_item_type(self._item_type)

    def _get_template(self) -> JsonDict:
        """Serialize the container into the broker schema format."""
        match self._item_type:
            case "input":
                format_keys = ["@type", "@help", "@unit", "@necessity", "@constraints"]
            case "output":
                format_keys = ["@type", "@help", "@unit", "@repr"]
            case "condition":
                format_keys = ["@type", "@help", "@unit", "@value"]
            case _ as unknown_type:
                raise NotImplementedError(f"Unknown item type: {unknown_type}")
        template_dict = dict[str, Any](**{fkey: {} for fkey in format_keys})

        value_keys = [
            key
            for key, template in self._container_dict.items()
            if not isinstance(template, Table)
        ]
        table_keys = [
            key
            for key, template in self._container_dict.items()
            if isinstance(template, Table)
        ]

        template_dict["@keys"] = value_keys + table_keys
        if len(value_keys) > 0:
            template_dict["@value"] = value_keys

        if len(table_keys) > 0:
            template_dict["@table"] = {}  # will be given by Table instance

        for key, template in self._container_dict.items():
            for fkey, fvalue in template.format_dict.items():
                if fkey == "_unit_dict":
                    for k, v in fvalue.items():
                        assert (
                            k not in template_dict["@unit"]
                            and k not in template_dict["@type"]
                        ), f"Duplicated key error: {k}"
                        template_dict["@unit"][k] = v
                        template_dict["@type"][k] = "number"
                else:
                    assert (
                        key not in template_dict[fkey]
                    ), f"Duplicated key error: {key}"
                    template_dict[fkey][key] = fvalue
        return template_dict

    def _load(self, config_dict: Mapping[str, Any]) -> None:
        """Populate the container from a broker schema dict."""
        for key in config_dict["@keys"]:
            format_dict = {
                prop_key: config_dict[prop_key][key]
                for prop_key in config_dict.keys()
                if prop_key.startswith("@")
                and isinstance(config_dict[prop_key], dict)
                and key in config_dict[prop_key]
            }
            setattr(
                self, key, ValueTemplate.from_dict(format_dict, config_dict["@unit"])
            )

    def __contains__(self, key: str) -> bool:
        return key in self._container_dict

    def __getitem__(self, key: str) -> ValueTemplate:
        return self._container_dict[key]


class ValueContainer:
    """Simple accessor wrapper for broker result dicts."""

    def __init__(self, value_dict: Mapping[str, Any]) -> None:
        self._container_dict: dict[str, Any] = dict(value_dict)

    def __getattr__(self, key: str) -> Any:
        if key.startswith("_"):
            raise AttributeError(f"{key}")
        return self._container_dict[key]

    def __getitem__(self, key: str) -> Any:
        return self._container_dict[key]


class AgentInterface:
    """In-memory representation of agent settings and schemas."""

    def __init__(self) -> None:
        self.agent_auth: str = ""
        """`agent_auth` credential string used by `Agent` for `/api/v1/agent/*` requests."""
        self.name: str | None = None
        """Display name shown in broker listings."""
        self.charge: int = 10000
        """Default charge in points before any charge_func override."""
        self.relay_points_per_minute: int | float | None = None
        """Interactive relay price in points/minute; batch `charge` remains separate."""
        self.job_error_detail_level: JobErrorDetailLevel = "traceback"
        """How much structured error detail to expose when a job fails."""
        self.convention: str = ""
        self.description: str = ""
        self.user_info_request: list[UserInfoField] = []
        self.ui_preview: UiPreview | None = None
        self.input: TemplateContainer = TemplateContainer("input")
        self.condition: TemplateContainer = TemplateContainer("condition")
        self.output: TemplateContainer = TemplateContainer("output")
        self.func_dict: dict[str, Callable[..., Any]] = {}
        self.last_feedback: NegotiationFeedback | None = None

    def prepare(self, func_dict: Mapping[str, Callable[..., Any]]) -> None:
        """Validate required agent hooks and refresh config templates."""
        self.func_dict = dict(func_dict)
        if "make_config" not in self.func_dict and (
            self.agent_auth == "" or self.name is None
        ):
            message = "agent_auth and name should be specified."
            logger.exception(message)
            raise RuntimeError(message)
        if "job_func" not in self.func_dict:
            message = "job execution function is required: Prepare a decorated function with @job_func."
            logger.exception(message)
            raise RuntimeError(message)
        self.make_config()

    def make_config(self, for_registration: bool = False) -> JsonDict:
        """Build the agent config payload sent to the broker."""
        if "make_config" in self.func_dict:
            self.func_dict["make_config"]()
            if self.name is None:
                message = "Agent's name should be set in config function"
                logger.exception(message)
                raise RuntimeError(message)
            if self.agent_auth == "":
                message = "Agent's agent_auth should be set in config function"
                logger.exception(message)
                raise RuntimeError(message)
        config_dict = dict[str, Any]()
        for item_type in ["input", "condition", "output"]:
            config_dict[item_type] = getattr(
                getattr(self, item_type), "_get_template"
            )()

        relay_points_per_minute = self._active_relay_points_per_minute(config_dict)
        config_dict["charge"] = (
            relay_points_per_minute
            if relay_points_per_minute is not None
            else self.charge
        )
        if relay_points_per_minute is not None:
            config_dict["billing"] = {
                "mode": "relay_time",
                "points_per_minute": relay_points_per_minute,
                "max_duration_required": True,
                "max_duration_minutes": None,
                "estimated_charge": None,
                "unit": "points/min",
            }
        config_dict["user_info_request"] = [
            field.value for field in self.user_info_request
        ]
        if self.ui_preview is not None:
            if not isinstance(self.ui_preview, dict):
                raise TypeError("Agent ui_preview must be a dict when set")
            config_dict["ui_preview"] = _normalize_ui_preview(self.ui_preview)
        if for_registration:
            config_dict["module_version"] = (
                __version__  # pyright: ignore[reportUndefinedVariable]
                if "__version__" in globals()
                else "-1.0.0"
            )
            config_dict["convention"] = self.convention
            config_dict["description"] = self.description

        return config_dict

    def _active_relay_points_per_minute(
        self, config_dict: Mapping[str, Any]
    ) -> int | float | None:
        if self.relay_points_per_minute is None:
            return None
        output = config_dict.get("output")
        if not isinstance(output, Mapping):
            return None
        output_types = output.get("@type")
        if not isinstance(output_types, Mapping):
            return None
        if any(
            value in ("relay_file", "relay_media", "relay_session")
            for value in output_types.values()
        ):
            return self.relay_points_per_minute
        return None

    def has_func(self, func_name: str) -> bool:
        return func_name in self.func_dict

    def validate(
        self, input_dict: JsonDict
    ) -> tuple[Literal["ok", "need_revision"], JsonDict]:
        """Validate a request payload and return (msg, input_template)."""
        template_dict = self.input._get_template()
        msg = "ok"
        field_feedback: dict[str, str] = {}
        self.last_feedback = None
        for key in template_dict["@keys"]:
            if key in input_dict:
                try:
                    value = self.input[key].cast(input_dict[key])
                    template_dict[key] = value
                except (AssertionError, TypeError, ValueError):
                    msg = "need_revision"
                    field_feedback[key] = (
                        "This value does not satisfy the declared input constraints."
                    )
            else:
                msg = "need_revision"
                field_feedback[key] = "This required parameter is missing."

        if msg == "need_revision":
            self.last_feedback = _build_feedback(
                "Some submitted values did not satisfy the declared input constraints.",
                field_feedback,
                kind="validation",
            )
        return msg, template_dict

    def cast(self, key: str, input_value: Any) -> Any:
        """Cast a request field according to its template."""
        if key not in self.input:
            logger.exception(f"{key} is not registered as input.")
            raise Exception
        return self.input[key].cast(input_value)

    def format_for_output(
        self,
        result_dict: JsonDict,
        uploader: Callable[[str, bytes], JsonDict],
        relay_registrar: RelayRegistrar | None = None,
    ) -> JsonDict:
        """Convert raw job results to the broker output schema."""
        output_dict = {
            "@help": {},
            "@unit": {},
            "@type": {},
            "@option": {},
            "@repr": {},
            "@keys": [],
        }

        for key, value in result_dict.items():
            if key not in self.output:
                continue
            output_value, format_dict = self.output[key].format_for_output(
                value, uploader, relay_registrar
            )
            if output_value is None:
                continue
            output_dict[key] = output_value
            output_dict["@keys"].append(key)
            ### manage type specific properties
            if format_dict["@type"] == "table":
                if "@table" not in output_dict:
                    output_dict["@table"] = {}
                table_keys = list(format_dict["_unit_dict"].keys())
                output_dict["@unit"].update(format_dict["_unit_dict"])
                output_dict["@type"].update({k: "number" for k in table_keys})

            elif format_dict["@type"] in [
                "image",
                "file",
                "relay_file",
                "relay_media",
                "relay_session",
            ]:
                if "@file" not in output_dict:
                    output_dict["@file"] = {}
            else:
                if "@value" not in output_dict:
                    output_dict["@value"] = []
                output_dict["@value"].append(key)

            for fk, fv in format_dict.items():
                if not fk.startswith("_"):
                    output_dict[fk][key] = fv
        # remove empty property keys

        return output_dict


class DummyJob(Mapping[str, Any]):
    """Fallback job used during local config generation."""

    _to_show_i_am_job_class = True

    def __init__(
        self, request_params: Mapping[str, Any], config: dict[str, str]
    ) -> None:
        class DummyAgentInterface:
            def __init__(self):
                self.agent_auth = config["agent_auth"]

        class DummyAgent:
            def __init__(self):
                if config is not None:
                    self.broker_url = config["broker_url"]
                    self.interface = DummyAgentInterface()

        self._agent = DummyAgent()
        self._request = dict(request_params)
        self.id: str = "dummy-job"
        self.job_id: str = self.id
        self._cancel_event = threading.Event()
        self.termination_reason: str | None = None

    def report(
        self,
        msg: str | None = None,
        progress: float | None = None,
        result: JsonDict | None = None,
        *,
        max_upload_workers: int | None = None,
    ) -> None:
        _ = max_upload_workers
        logger.info(f"[DUMMY JOB] REPORT: {msg}, {progress}, {result}")

    def __getitem__(self, key: str) -> Any:
        return self._request[key]

    def __contains__(self, key: object) -> bool:
        return key in self._request

    def __iter__(self):
        return iter(self._request)

    def __len__(self) -> int:
        return len(self._request)

    @property
    def broker_metadata(self) -> JsonDict:
        metadata = self._request.get("_broker")
        return dict(metadata) if isinstance(metadata, Mapping) else {}

    @property
    def max_duration_minutes(self) -> int | float | None:
        value = self.broker_metadata.get("max_duration_minutes")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        return None

    @property
    def cancel_requested(self) -> bool:
        return self._cancel_event.is_set()

    @property
    def cancel_event(self) -> threading.Event:
        return self._cancel_event

    def raise_if_cancelled(self) -> None:
        if self.cancel_requested:
            raise JobCancelledError(self.job_id, self.termination_reason)


class Job(Mapping[str, Any]):
    """Runtime job wrapper used by agent job functions.

    Behaves like a read-only mapping of request fields.
    """

    _to_show_i_am_job_class = True

    def __init__(self, agent: "Agent", negotiation_id: str, request: JsonDict) -> None:
        self._agent = agent
        self._negotiation_id = negotiation_id
        self._request: JsonDict = request
        self._request["_user_info"] = _normalize_user_info_payload(
            self._request.get("_user_info")
        )
        self._broker_metadata: JsonDict = self._normalize_broker_metadata(
            self._request.get("_broker")
        )
        self._request["_broker"] = self._broker_metadata
        self._status: str = "init"
        self.id: str = negotiation_id
        self.job_id: str = negotiation_id
        self._cancel_event = threading.Event()
        self.termination_reason: str | None = None

    def _normalize_broker_metadata(self, metadata: object) -> JsonDict:
        if not isinstance(metadata, Mapping):
            return {}
        normalized: JsonDict = {
            key: value for key, value in metadata.items() if isinstance(key, str)
        }
        value = normalized.get("max_duration_minutes")
        if value is not None and (
            not isinstance(value, (int, float)) or isinstance(value, bool)
        ):
            logger.warning(
                "Ignoring malformed _broker.max_duration_minutes for %s: %r",
                self._negotiation_id,
                value,
            )
            normalized.pop("max_duration_minutes", None)
        return normalized

    def report(
        self,
        msg: str | None = None,
        progress: float | None = None,
        result: JsonDict | None = None,
        *,
        max_upload_workers: int | None = None,
    ) -> None:
        """Post a progress or result update to the broker.

        The broker stores the latest `msg`, `progress`, and `result` values,
        and the UI/client APIs read the latest stored snapshot. Fields omitted
        from a report keep their previous values. The broker merges partial
        result payloads into the stored snapshot.

        Args:
            msg: Optional status text to show in the broker UI.
            progress: Optional fractional progress in the range 0..1.
            result: Optional partial result payload. If this includes `File` or
                image outputs, the SDK uploads the corresponding binary data
                in parallel before sending the report.
            max_upload_workers: Maximum number of concurrent file/image uploads
                for this report. Use `1` to force serial uploads.

        Raises:
            AgentConnectionError: If the broker cannot be reached after retries.
            AgentHTTPError: If the broker responds with a non-200 HTTP status.
            AgentResponseError: If the broker responds with malformed JSON.
            AgentUploadError: If file/image data in `result` cannot be uploaded,
                including API storage-capacity rejections.
        """
        payload: JsonDict = dict(
            negotiation_id=self._negotiation_id, status=self._status
        )
        logger.debug(
            "REPORT payload: msg=%r progress=%r result=%r",
            msg,
            progress,
            _safe_debug_value(result),
        )
        if msg is not None:
            payload["msg"] = msg
        if progress is not None:
            payload["progress"] = progress
        if result is not None:
            assert isinstance(
                result, dict
            ), "Result should be given as a dict: {result_key: result_value}"
            prepared_result = self._prepare_report_uploads(
                result,
                max_upload_workers=max_upload_workers,
            )
            self._agent._set_current_relay_job_id(self.job_id)
            try:
                payload["result"] = self._agent.interface.format_for_output(
                    prepared_result,
                    self._agent.upload,
                    self._agent.register_relay_source,
                )
            finally:
                self._agent._clear_current_relay_job_id()

        self._agent.post("report", payload)

    def upload(
        self,
        file_type: str,
        value: UploadValue,
    ) -> UploadedFile:
        """Upload a job-result file and return a handle for `report(result=...)`.

        Args:
            file_type: File extension/type declared by the agent output schema.
            value: Bytes-like content or a local file path.

        Returns:
            Uploaded file handle accepted by `File(...)` outputs.
        """
        file_id, _format = File(file_type).format_for_output(value, self._agent.upload)
        if not isinstance(file_id, str):
            raise AgentResponseError(
                f"Upload returned malformed payload for {file_type!r}",
                payload=file_id,
            )
        return UploadedFile(file_id=file_id, file_type=file_type)

    def upload_parallel(
        self,
        files: Mapping[str, tuple[str, UploadValue]],
        *,
        max_upload_workers: int | None = None,
    ) -> dict[str, UploadedFile]:
        """Upload multiple job-result files concurrently.

        Returns a key-preserving mapping that can be merged directly into
        `report(result=...)`.
        """

        def upload_one(_key: str, item: tuple[str, UploadValue]) -> UploadedFile:
            file_type, value = item
            return self.upload(file_type, value)

        return cast(
            dict[str, UploadedFile],
            _upload_parallel_map(
                files,
                max_upload_workers=max_upload_workers,
                upload_one=upload_one,
            ),
        )

    def _report_internal(
        self,
        *,
        msg: str | None = None,
        progress: float | None = None,
        result: JsonDict | None = None,
    ) -> None:
        """Send a broker-internal report payload without output-schema formatting."""
        payload: JsonDict = dict(
            negotiation_id=self._negotiation_id, status=self._status
        )
        if msg is not None:
            payload["msg"] = msg
        if progress is not None:
            payload["progress"] = progress
        if result is not None:
            payload["result"] = result
        self._agent.post("report", payload)

    def _prepare_report_uploads(
        self,
        result: JsonDict,
        *,
        max_upload_workers: int | None,
    ) -> JsonDict:
        pending: dict[str, tuple[str, UploadValue]] = {}
        for key, value in result.items():
            if key not in self._agent.interface.output:
                continue
            template = self._agent.interface.output[key]
            if isinstance(template, File) and not isinstance(value, UploadedFile):
                pending[key] = (template.file_type, cast(UploadValue, value))

        if not pending:
            return result

        uploaded = self.upload_parallel(
            pending,
            max_upload_workers=max_upload_workers,
        )
        prepared = dict(result)
        prepared.update(uploaded)
        return prepared

    def msg(self, msg: str) -> None:
        """Send a message-only update.

        Raises:
            AgentConnectionError: If the broker cannot be reached after retries.
            AgentHTTPError: If the broker responds with a non-200 HTTP status.
            AgentResponseError: If the broker responds with malformed JSON.
        """
        self.report(msg=msg)

    def progress(self, progress: float, msg: str | None = None) -> None:
        """Send a progress update (0..1).

        Raises:
            AgentConnectionError: If the broker cannot be reached after retries.
            AgentHTTPError: If the broker responds with a non-200 HTTP status.
            AgentResponseError: If the broker responds with malformed JSON.
        """
        self.report(progress=progress, msg=msg)

    def periodic_report(
        self,
        estimated_time: timedelta | float | None = None,
        interval: timedelta = timedelta(seconds=2),
        callback_func: Callable[..., str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Send periodic progress updates until job completion.

        Raises:
            AgentConnectionError: If the broker cannot be reached after retries.
            AgentHTTPError: If the broker responds with a non-200 HTTP status.
            AgentResponseError: If the broker responds with malformed JSON.
        """
        if "start_time" not in kwargs:
            kwargs["start_time"] = time.perf_counter()
        if self._status not in ["done", "error"]:
            msg = None
            if callback_func is not None:
                kwargs["callback_func"] = callback_func
                msg = callback_func(self, **kwargs)
            if estimated_time is not None:
                if not isinstance(estimated_time, timedelta):
                    estimated_time = timedelta(seconds=estimated_time)
                kwargs["estimated_time"] = estimated_time
                self.progress(
                    (time.perf_counter() - kwargs["start_time"])
                    / estimated_time.total_seconds(),
                    msg=msg,
                )
            elif msg is not None:
                self.msg(msg)
            timer = threading.Timer(
                interval.total_seconds(), self.periodic_report, kwargs=kwargs
            )
            timer.daemon = True
            timer.start()

    def __getitem__(self, key: str) -> Any:
        return self._agent.interface.cast(key, self._request[key])

    def __contains__(self, key: object) -> bool:
        return key in self._request

    @property
    def user_info(self) -> UserInfo:
        """User info injected by the broker.

        Fields are populated based on the agent's `user_info_request`.
        """
        return cast(UserInfo, self._request["_user_info"])

    @property
    def broker_metadata(self) -> JsonDict:
        """Reserved broker metadata supplied under request `_broker`."""
        return dict(self._broker_metadata)

    @property
    def max_duration_minutes(self) -> int | float | None:
        """Requested maximum interactive relay duration, when supplied."""
        value = self._broker_metadata.get("max_duration_minutes")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return value
        return None

    @property
    def cancel_requested(self) -> bool:
        """Whether broker/client termination has been requested."""
        return self._cancel_event.is_set()

    @property
    def cancel_event(self) -> threading.Event:
        """Event set when broker/client termination has been requested."""
        return self._cancel_event

    def raise_if_cancelled(self) -> None:
        """Raise `JobCancelledError` if cooperative cancellation was requested."""
        if self.cancel_requested:
            raise JobCancelledError(self.job_id, self.termination_reason)

    def __iter__(self):
        return iter(self._request)

    def __len__(self) -> int:
        return len(self._request)

    def _set_status(self, status: str) -> None:
        self._status = status

    def _request_cancel(self, reason: str | None = None) -> None:
        self.termination_reason = reason
        self._cancel_event.set()
        self._set_status("error")


@dataclass
class _ContractRunState:
    negotiation_id: str
    job: Job
    thread: threading.Thread
    started_at: float
    phase: ContractRunPhase = "starting"
    finished: bool = False
    cleanup_reported: bool = False
    last_lease_sent: float = 0.0


@dataclass
class _RelayFileSource:
    source_id: str
    job_id: str | None
    path: Path
    name: str
    size_bytes: int
    content_type: str
    created_at: float
    last_accessed_at: float


@dataclass
class _RelayLiveSource:
    source_id: str
    job_id: str | None
    name: str
    content_type: str
    created_at: float
    last_accessed_at: float
    open_chunks: Callable[[threading.Event], Iterable[bytes]]


@dataclass
class _RelayAssetTreeSource:
    source_id: str
    job_id: str | None
    root_dir: Path
    entry_path: str
    name: str
    size_bytes: int
    content_type: str
    created_at: float
    last_accessed_at: float


@dataclass
class _RelaySessionRuntime:
    stream_id: str
    source_id: str
    cancel_event: threading.Event
    handle_input: Callable[[str], None] | None


@dataclass
class _RelayBinaryRuntime:
    stream_id: str
    source_id: str
    cancel_event: threading.Event
    credit_condition: threading.Condition
    available_credit: int


@dataclass
class _RelaySessionRuntimeSource:
    source_id: str
    job_id: str | None
    name: str
    protocol: str
    created_at: float
    last_accessed_at: float
    open_session: Callable[
        [threading.Event, Callable[[str], None], Callable[[], None]],
        Callable[[str], None] | None,
    ]


class Agent:
    """Agent runtime that connects to broker and handles negotiation/contract flow."""

    RESTART_INTERVAL_CRITERIA = 30
    HEARTBEAT_INTERVAL = 2
    REQUEST_RETRY_DEADLINE = 180.0
    REQUEST_RETRY_BASE = 0.5
    REQUEST_RETRY_MAX = 5.0
    REQUEST_CONNECT_TIMEOUT = 10.0
    REQUEST_READ_TIMEOUT = 30.0
    UPLOAD_READ_TIMEOUT = 120.0
    SUPERVISOR_INTERVAL = 2.0
    CONTRACT_LEASE_INTERVAL = 10.0
    CONTRACT_LEASE_TIMEOUT = 90
    CONTRACT_LEASE_RETRY_DEADLINE = 8.0
    POLLING_BACKOFF_MAX = 10
    RELAY_RECONNECT_BASE = 1.0
    RELAY_RECONNECT_MAX = 10.0
    RELAY_HEARTBEAT_INTERVAL = 20.0
    RELAY_SOURCE_TTL_SECONDS = 86_400
    RELAY_CHUNK_SIZE = 64 * 1024
    _automatic_built_agents: dict[str, "Agent"] = {}

    def __init__(self, broker_url: str) -> None:
        self._to_show_i_am_agent_instance = True
        self.broker_url: str = broker_url
        self._http_session_local = threading.local()
        self.access_token: str | None = None
        self.agent_funcs: dict[str, Callable[..., Any]] = {}
        self.running: bool = False
        self.interface: AgentInterface = AgentInterface()
        self.auth: str = ""
        self.runtime_instance_id = str(uuid.uuid4())
        self.polling_interval: int = 0
        self.last_heartbeat: float = 0.0
        self._contract_runs: dict[str, _ContractRunState] = {}
        self._contract_runs_lock = threading.Lock()
        self._connect_thread: threading.Thread | None = None
        self._connect_thread_lock = threading.Lock()
        self._heartbeat_thread: threading.Thread | None = None
        self._supervisor_thread: threading.Thread | None = None
        self._relay_thread: threading.Thread | None = None
        self._relay_thread_lock = threading.Lock()
        self._relay_socket: websocket.WebSocketApp | None = None
        self._relay_socket_lock = threading.Lock()
        self._relay_send_lock = threading.Lock()
        self._relay_sources: dict[
            str,
            _RelayFileSource
            | _RelayLiveSource
            | _RelayAssetTreeSource
            | _RelaySessionRuntimeSource,
        ] = {}
        self._relay_sources_lock = threading.Lock()
        self._relay_cancel_events: dict[str, threading.Event] = {}
        self._relay_cancel_events_lock = threading.Lock()
        self._relay_binary_runtimes: dict[str, _RelayBinaryRuntime] = {}
        self._relay_binary_runtimes_lock = threading.Lock()
        self._relay_session_runtimes: dict[str, _RelaySessionRuntime] = {}
        self._relay_session_runtimes_lock = threading.Lock()
        self._relay_job_local = threading.local()

    def _http_session(self) -> requests.Session:
        return _get_thread_session(self._http_session_local)

    def _set_current_relay_job_id(self, job_id: str) -> None:
        self._relay_job_local.job_id = job_id

    def _clear_current_relay_job_id(self) -> None:
        if hasattr(self._relay_job_local, "job_id"):
            delattr(self._relay_job_local, "job_id")

    def _current_relay_job_id(self) -> str | None:
        job_id = getattr(self._relay_job_local, "job_id", None)
        return job_id if isinstance(job_id, str) else None

    def _increase_polling_interval(self) -> None:
        if self.polling_interval <= 0:
            self.polling_interval = 1
        else:
            self.polling_interval = min(
                self.polling_interval * 2, self.POLLING_BACKOFF_MAX
            )

    def _reset_polling_interval(self) -> None:
        self.polling_interval = 0

    def run(self, _automatic: bool = False) -> None:
        """Start the agent loop and block until shutdown if not automatic."""
        if self.running:
            return
        self.interface.prepare(self.agent_funcs)
        self.auth = self.interface.agent_auth
        self.polling_interval = 0
        self.last_heartbeat = time.perf_counter()
        if self.register_config():
            self.running = True
            self._ensure_connect_thread()
            self._ensure_relay_thread()
            self._ensure_heartbeat_thread()
            self._ensure_supervisor_thread()
            if not _automatic:
                try:
                    if sys.stdin.isatty():
                        logger.info(
                            "Agent %s has started. Press return to quit.",
                            self.interface.name,
                        )
                        input("")
                    else:
                        # In non-interactive environments (stdin closed / redirected),
                        # `input()` would raise EOFError and immediately stop the agent.
                        logger.info(
                            "Agent %s has started (non-interactive). Press Ctrl+C to quit.",
                            self.interface.name,
                        )
                        while True:
                            time.sleep(1)
                except KeyboardInterrupt:
                    pass
                finally:
                    self.goodbye()

    @classmethod
    def start(cls) -> None:
        """Start all auto-built agents and block until interrupted."""
        agent_list = list[Agent]()
        try:
            for agent in cls._automatic_built_agents.values():
                agent.run(_automatic=True)
                agent_list.append(agent)
            if sys.stdin.isatty():
                logger.info("Press return to quit.")
                input("")
            else:
                logger.info(
                    "Agents have started (non-interactive). Press Ctrl+C to quit."
                )
                while True:
                    time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            for agent in agent_list:
                agent.goodbye()

    def goodbye(self) -> None:
        """Stop agent loops and allow threads to exit."""
        self.running = False
        with self._relay_socket_lock:
            if self._relay_socket is not None:
                try:
                    self._relay_socket.close()
                except Exception:
                    logger.debug("Ignoring relay socket close failure", exc_info=True)
                self._relay_socket = None

    def _ensure_connect_thread(self) -> None:
        with self._connect_thread_lock:
            if self._connect_thread is not None and self._connect_thread.is_alive():
                return
            self._connect_thread = threading.Thread(
                target=self.connect,
                daemon=True,
                name=f"{self.interface.name}-msgbox-poller",
            )
            self._connect_thread.start()

    def _ensure_heartbeat_thread(self) -> None:
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_thread = threading.Thread(
            target=self.heartbeat,
            daemon=True,
            name=f"{self.interface.name}-heartbeat",
        )
        self._heartbeat_thread.start()

    def _ensure_relay_thread(self) -> None:
        with self._relay_thread_lock:
            if self._relay_thread is not None and self._relay_thread.is_alive():
                return
            self._relay_thread = threading.Thread(
                target=self.relay_loop,
                daemon=True,
                name=f"{self.interface.name}-relay",
            )
            self._relay_thread.start()

    def _ensure_supervisor_thread(self) -> None:
        if self._supervisor_thread is not None and self._supervisor_thread.is_alive():
            return
        self._supervisor_thread = threading.Thread(
            target=self._supervise_contract_runs,
            daemon=True,
            name=f"{self.interface.name}-contract-supervisor",
        )
        self._supervisor_thread.start()

    def relay_loop(self) -> None:
        """Maintain the broker relay WebSocket connection."""
        attempt = 0
        while self.running:
            try:
                headers = [f"authorization: Basic {self.auth}"]
                ws = websocket.WebSocketApp(
                    _relay_socket_url(self.broker_url),
                    header=headers,
                    on_open=self._on_relay_open,
                    on_message=self._on_relay_message,
                    on_error=self._on_relay_error,
                    on_close=self._on_relay_close,
                )
                with self._relay_socket_lock:
                    self._relay_socket = ws

                ws.run_forever(
                    ping_interval=self.RELAY_HEARTBEAT_INTERVAL,
                    ping_timeout=max(self.RELAY_HEARTBEAT_INTERVAL / 2.0, 5.0),
                )
                attempt = 0
            except Exception:
                logger.exception("Relay socket loop crashed")
            finally:
                with self._relay_socket_lock:
                    self._relay_socket = None

            if not self.running:
                break

            time.sleep(
                _backoff_seconds(
                    attempt, self.RELAY_RECONNECT_BASE, self.RELAY_RECONNECT_MAX
                )
            )
            attempt += 1

    def _on_relay_open(self, ws: websocket.WebSocketApp) -> None:
        logger.info("Relay socket connected.")
        self._relay_send_text(
            {"type": "hello", "runtime_instance_id": self.runtime_instance_id}, ws=ws
        )

    def _on_relay_message(
        self, ws: websocket.WebSocketApp, message: str | bytes
    ) -> None:
        try:
            if isinstance(message, bytes):
                logger.warning("Unexpected binary control message on relay socket")
                return

            payload = json.loads(message)
            if not isinstance(payload, dict):
                logger.warning("Ignoring non-dict relay control message: %r", payload)
                return
            self._handle_relay_control_message(payload)
        except Exception:
            logger.exception("Failed to process relay control message")

    def _on_relay_error(
        self,
        _ws: websocket.WebSocketApp,
        error: Exception | str,
    ) -> None:
        logger.warning("Relay socket error: %s", error)

    def _on_relay_close(
        self,
        _ws: websocket.WebSocketApp,
        status_code: int | None,
        close_msg: str | None,
    ) -> None:
        self._cancel_all_relay_streams()
        if self.running:
            logger.warning(
                "Relay socket closed: status_code=%r close_msg=%r",
                status_code,
                close_msg,
            )

    def _handle_relay_control_message(self, payload: Mapping[str, Any]) -> None:
        msg_type = payload.get("type")
        if msg_type == "serve_file":
            stream_id = payload.get("stream_id")
            source_id = payload.get("source_id")
            start_byte = payload.get("start_byte")
            end_byte = payload.get("end_byte")
            chunk_size = payload.get("chunk_size")
            credit_bytes = payload.get("credit_bytes")

            if not (
                isinstance(stream_id, str)
                and isinstance(source_id, str)
                and isinstance(start_byte, int)
                and isinstance(end_byte, int)
                and isinstance(chunk_size, int)
                and isinstance(credit_bytes, int)
            ):
                logger.warning(
                    "Ignoring malformed serve_file relay command: %r", payload
                )
                return

            cancel_event = threading.Event()
            runtime = _RelayBinaryRuntime(
                stream_id=stream_id,
                source_id=source_id,
                cancel_event=cancel_event,
                credit_condition=threading.Condition(),
                available_credit=max(credit_bytes, 0),
            )
            with self._relay_cancel_events_lock:
                self._relay_cancel_events[stream_id] = cancel_event
            with self._relay_binary_runtimes_lock:
                self._relay_binary_runtimes[stream_id] = runtime

            threading.Thread(
                target=self._serve_relay_file,
                args=(
                    stream_id,
                    source_id,
                    start_byte,
                    end_byte,
                    chunk_size,
                    runtime,
                ),
                daemon=True,
                name=f"relay-{stream_id[:8]}",
            ).start()
            return

        if msg_type == "serve_live_stream":
            stream_id = payload.get("stream_id")
            source_id = payload.get("source_id")
            credit_bytes = payload.get("credit_bytes")

            if not (
                isinstance(stream_id, str)
                and isinstance(source_id, str)
                and isinstance(credit_bytes, int)
            ):
                logger.warning(
                    "Ignoring malformed serve_live_stream relay command: %r", payload
                )
                return

            cancel_event = threading.Event()
            runtime = _RelayBinaryRuntime(
                stream_id=stream_id,
                source_id=source_id,
                cancel_event=cancel_event,
                credit_condition=threading.Condition(),
                available_credit=max(credit_bytes, 0),
            )
            with self._relay_cancel_events_lock:
                self._relay_cancel_events[stream_id] = cancel_event
            with self._relay_binary_runtimes_lock:
                self._relay_binary_runtimes[stream_id] = runtime

            threading.Thread(
                target=self._serve_relay_live_stream,
                args=(stream_id, source_id, runtime),
                daemon=True,
                name=f"relay-live-{stream_id[:8]}",
            ).start()
            return

        if msg_type == "serve_asset":
            stream_id = payload.get("stream_id")
            source_id = payload.get("source_id")
            asset_path = payload.get("asset_path")
            credit_bytes = payload.get("credit_bytes")

            if not (
                isinstance(stream_id, str)
                and isinstance(source_id, str)
                and isinstance(asset_path, str)
                and isinstance(credit_bytes, int)
            ):
                logger.warning(
                    "Ignoring malformed serve_asset relay command: %r", payload
                )
                return

            cancel_event = threading.Event()
            runtime = _RelayBinaryRuntime(
                stream_id=stream_id,
                source_id=source_id,
                cancel_event=cancel_event,
                credit_condition=threading.Condition(),
                available_credit=max(credit_bytes, 0),
            )
            with self._relay_cancel_events_lock:
                self._relay_cancel_events[stream_id] = cancel_event
            with self._relay_binary_runtimes_lock:
                self._relay_binary_runtimes[stream_id] = runtime

            threading.Thread(
                target=self._serve_relay_asset,
                args=(stream_id, source_id, asset_path, runtime),
                daemon=True,
                name=f"relay-asset-{stream_id[:8]}",
            ).start()
            return

        if msg_type == "open_text_session":
            stream_id = payload.get("stream_id")
            source_id = payload.get("source_id")

            if not (isinstance(stream_id, str) and isinstance(source_id, str)):
                logger.warning(
                    "Ignoring malformed open_text_session relay command: %r", payload
                )
                return

            cancel_event = threading.Event()
            with self._relay_cancel_events_lock:
                self._relay_cancel_events[stream_id] = cancel_event

            threading.Thread(
                target=self._serve_relay_text_session,
                args=(stream_id, source_id, cancel_event),
                daemon=True,
                name=f"relay-session-{stream_id[:8]}",
            ).start()
            return

        if msg_type == "session_input":
            stream_id = payload.get("stream_id")
            text = payload.get("text")
            if not (isinstance(stream_id, str) and isinstance(text, str)):
                logger.warning(
                    "Ignoring malformed session_input relay command: %r", payload
                )
                return
            self._handle_relay_session_input(stream_id, text)
            return

        if msg_type == "cancel":
            stream_id = payload.get("stream_id")
            if isinstance(stream_id, str):
                with self._relay_cancel_events_lock:
                    event = self._relay_cancel_events.get(stream_id)
                if event is not None:
                    event.set()
                self._notify_relay_binary_runtime(stream_id)
                self._clear_relay_session_runtime(stream_id)
            return

        if msg_type == "credit":
            stream_id = payload.get("stream_id")
            bytes_credit = payload.get("bytes")
            if isinstance(stream_id, str) and isinstance(bytes_credit, int):
                self._add_relay_credit(stream_id, bytes_credit)
            else:
                logger.warning("Ignoring malformed relay credit command: %r", payload)
            return

        logger.warning("Unknown relay control message: %r", payload)

    def register_relay_source(
        self,
        value: RelaySourceValue,
        download_name: str | None,
        content_type_override: str | None,
    ) -> JsonDict:
        """Register a local file or live stream as a broker relay source."""
        if isinstance(value, RelaySessionSource):
            return self.register_relay_session(
                value,
                download_name,
                content_type_override,
            )
        if isinstance(value, RelayAssetSource):
            return self.register_relay_assets(
                value,
                download_name,
                content_type_override,
            )
        if isinstance(value, RelayStreamSource):
            return self.register_relay_stream(
                value,
                download_name,
                content_type_override,
            )
        return self.register_relay_file(value, download_name, content_type_override)

    def register_relay_file(
        self,
        value: PathLike[str] | str,
        download_name: str | None,
        content_type_override: str | None,
    ) -> JsonDict:
        """Register a local file as a broker relay source and return tagged metadata."""
        path = Path(value)
        stat = path.stat()
        name = download_name or path.name
        content_type = content_type_override or mimetypes.guess_type(path.name)[0]
        if content_type is None:
            content_type = "application/octet-stream"
        source_id = f"src_{uuid.uuid4()}"
        now = time.perf_counter()
        source = _RelayFileSource(
            source_id=source_id,
            job_id=self._current_relay_job_id(),
            path=path,
            name=name,
            size_bytes=stat.st_size,
            content_type=content_type,
            created_at=now,
            last_accessed_at=now,
        )
        with self._relay_sources_lock:
            self._relay_sources[source_id] = source
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": source_id,
            "runtime_instance_id": self.runtime_instance_id,
            "name": name,
            "size_bytes": stat.st_size,
            "content_type": content_type,
            "availability": "agent_online_required",
        }

    def register_relay_stream(
        self,
        value: RelayStreamSource,
        download_name: str | None,
        content_type_override: str | None,
    ) -> JsonDict:
        """Register a live byte stream as a broker relay source."""
        name = download_name or "relay-stream"
        content_type = content_type_override or "application/octet-stream"
        source_id = f"src_{uuid.uuid4()}"
        now = time.perf_counter()
        source = _RelayLiveSource(
            source_id=source_id,
            job_id=self._current_relay_job_id(),
            name=name,
            content_type=content_type,
            created_at=now,
            last_accessed_at=now,
            open_chunks=value.open_chunks,
        )
        with self._relay_sources_lock:
            self._relay_sources[source_id] = source
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": source_id,
            "runtime_instance_id": self.runtime_instance_id,
            "name": name,
            "size_bytes": 0,
            "content_type": content_type,
            "availability": "agent_online_required",
        }

    def register_relay_assets(
        self,
        value: RelayAssetSource,
        download_name: str | None,
        content_type_override: str | None,
    ) -> JsonDict:
        """Register a rooted asset tree as a broker relay source."""
        root_dir = Path(value.root_dir).expanduser().resolve()
        if not root_dir.is_dir():
            raise TypeError(
                f"a RelayAssetSource root_dir must be an existing directory but got: {root_dir}"
            )

        entry_path = self._normalize_relay_asset_path(value.entry_path)
        entry_file = root_dir / Path(*PurePosixPath(entry_path).parts)
        if not entry_file.is_file():
            raise TypeError(
                "a RelayAssetSource entry_path must point to an existing file under"
                f" root_dir but got: {value.entry_path!r}"
            )

        name = download_name or entry_file.name
        content_type = content_type_override or mimetypes.guess_type(entry_file.name)[0]
        if content_type is None:
            content_type = "application/octet-stream"
        source_id = f"src_{uuid.uuid4()}"
        now = time.perf_counter()
        source = _RelayAssetTreeSource(
            source_id=source_id,
            job_id=self._current_relay_job_id(),
            root_dir=root_dir,
            entry_path=entry_path,
            name=name,
            size_bytes=entry_file.stat().st_size,
            content_type=content_type,
            created_at=now,
            last_accessed_at=now,
        )
        with self._relay_sources_lock:
            self._relay_sources[source_id] = source
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_file",
                "transport": "broker_relay_v1",
            },
            "source_id": source_id,
            "runtime_instance_id": self.runtime_instance_id,
            "name": name,
            "size_bytes": source.size_bytes,
            "content_type": content_type,
            "availability": "agent_online_required",
            "entry_path": entry_path,
        }

    def register_relay_session(
        self,
        value: RelaySessionSource,
        download_name: str | None,
        protocol_override: str | None,
    ) -> JsonDict:
        """Register a bidirectional text session as a broker relay source."""
        name = download_name or "relay-session"
        protocol = protocol_override or "text"
        source_id = f"src_{uuid.uuid4()}"
        now = time.perf_counter()
        source = _RelaySessionRuntimeSource(
            source_id=source_id,
            job_id=self._current_relay_job_id(),
            name=name,
            protocol=protocol,
            created_at=now,
            last_accessed_at=now,
            open_session=value.open_session,
        )
        with self._relay_sources_lock:
            self._relay_sources[source_id] = source
        return {
            "$brokersystem": {
                "version": 1,
                "kind": "relay_session",
                "transport": "broker_relay_v1",
            },
            "source_id": source_id,
            "runtime_instance_id": self.runtime_instance_id,
            "name": name,
            "protocol": protocol,
            "availability": "agent_online_required",
        }

    def _prune_relay_sources(self) -> None:
        cutoff = time.perf_counter() - self.RELAY_SOURCE_TTL_SECONDS
        with self._relay_sources_lock:
            stale_ids = [
                source_id
                for source_id, source in self._relay_sources.items()
                if source.last_accessed_at < cutoff
            ]
            for source_id in stale_ids:
                self._relay_sources.pop(source_id, None)

    def _get_relay_source(
        self, source_id: str
    ) -> (
        _RelayFileSource
        | _RelayLiveSource
        | _RelayAssetTreeSource
        | _RelaySessionRuntimeSource
        | None
    ):
        with self._relay_sources_lock:
            source = self._relay_sources.get(source_id)
            if source is None:
                return None
            source.last_accessed_at = time.perf_counter()
            return source

    def _claim_relay_credit(self, runtime: _RelayBinaryRuntime, max_bytes: int) -> int:
        with runtime.credit_condition:
            while runtime.available_credit <= 0 and not runtime.cancel_event.is_set():
                runtime.credit_condition.wait(timeout=1.0)
            if runtime.cancel_event.is_set():
                return 0
            granted = min(max_bytes, runtime.available_credit)
            runtime.available_credit -= granted
            return granted

    def _add_relay_credit(self, stream_id: str, bytes_credit: int) -> None:
        with self._relay_binary_runtimes_lock:
            runtime = self._relay_binary_runtimes.get(stream_id)

        if runtime is None:
            return

        with runtime.credit_condition:
            runtime.available_credit += max(bytes_credit, 0)
            runtime.credit_condition.notify_all()

    def _notify_relay_binary_runtime(self, stream_id: str) -> None:
        with self._relay_binary_runtimes_lock:
            runtime = self._relay_binary_runtimes.get(stream_id)

        if runtime is None:
            return

        with runtime.credit_condition:
            runtime.credit_condition.notify_all()

    def _clear_relay_binary_runtime(self, stream_id: str) -> None:
        with self._relay_binary_runtimes_lock:
            self._relay_binary_runtimes.pop(stream_id, None)

    def _cancel_all_relay_streams(self) -> None:
        with self._relay_cancel_events_lock:
            events = list(self._relay_cancel_events.items())
            self._relay_cancel_events.clear()

        for stream_id, event in events:
            event.set()
            self._notify_relay_binary_runtime(stream_id)

        with self._relay_session_runtimes_lock:
            self._relay_session_runtimes.clear()

        with self._relay_binary_runtimes_lock:
            self._relay_binary_runtimes.clear()

    def _normalize_relay_asset_path(self, asset_path: str) -> str:
        posix_path = PurePosixPath(asset_path)
        if asset_path == "" or posix_path.is_absolute():
            raise ValueError(f"Invalid relay asset path: {asset_path!r}")
        parts = posix_path.parts
        if parts == () or any(part in {"", ".", ".."} for part in parts):
            raise ValueError(f"Invalid relay asset path: {asset_path!r}")
        return posix_path.as_posix()

    def _serve_relay_file(
        self,
        stream_id: str,
        source_id: str,
        start_byte: int,
        end_byte: int,
        chunk_size: int,
        runtime: _RelayBinaryRuntime,
    ) -> None:
        try:
            source = self._get_relay_source(source_id)
            if source is None:
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_missing",
                        "message": "Relay source is no longer available on the agent.",
                    }
                )
                return
            if not isinstance(source, _RelayFileSource):
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_type_mismatch",
                        "message": "Relay source is not a stored file.",
                    }
                )
                return

            current_size = source.path.stat().st_size
            if current_size != source.size_bytes:
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_changed",
                        "message": "Relay source size changed after the result was reported.",
                    }
                )
                return

            with source.path.open("rb") as handle:
                handle.seek(start_byte)
                offset = start_byte
                remaining = end_byte - start_byte + 1
                read_size = max(1, min(chunk_size, self.RELAY_CHUNK_SIZE))
                while remaining > 0 and not runtime.cancel_event.is_set():
                    allowed = self._claim_relay_credit(
                        runtime, min(read_size, remaining)
                    )
                    if allowed <= 0:
                        break
                    chunk = handle.read(allowed)
                    if chunk == b"":
                        break
                    self._relay_send_binary_chunk(stream_id, offset, chunk)
                    offset += len(chunk)
                    remaining -= len(chunk)

            if not runtime.cancel_event.is_set():
                self._relay_send_text({"type": "eof", "stream_id": stream_id})
        except FileNotFoundError:
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "source_missing",
                    "message": "Relay source file no longer exists on the agent.",
                }
            )
        except Exception as exc:
            logger.exception("Relay file serving failed; stream_id=%s", stream_id)
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "relay_failed",
                    "message": str(exc),
                }
            )
        finally:
            with self._relay_cancel_events_lock:
                self._relay_cancel_events.pop(stream_id, None)
            self._clear_relay_binary_runtime(stream_id)

    def _serve_relay_live_stream(
        self,
        stream_id: str,
        source_id: str,
        runtime: _RelayBinaryRuntime,
    ) -> None:
        offset = 0
        try:
            source = self._get_relay_source(source_id)
            if source is None:
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_missing",
                        "message": "Relay source is no longer available on the agent.",
                    }
                )
                return
            if not isinstance(source, _RelayLiveSource):
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_type_mismatch",
                        "message": "Relay source is not a live stream.",
                    }
                )
                return

            for chunk in source.open_chunks(runtime.cancel_event):
                if runtime.cancel_event.is_set():
                    break
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise TypeError(
                        "Relay live stream chunks must be bytes-like values."
                    )
                data = bytes(chunk)
                while data != b"" and not runtime.cancel_event.is_set():
                    allowed = self._claim_relay_credit(
                        runtime, min(len(data), self.RELAY_CHUNK_SIZE)
                    )
                    if allowed <= 0:
                        break
                    chunk = data[:allowed]
                    data = data[allowed:]
                    self._relay_send_binary_chunk(stream_id, offset, chunk)
                    offset += len(chunk)

            if not runtime.cancel_event.is_set():
                self._relay_send_text({"type": "eof", "stream_id": stream_id})
        except Exception as exc:
            logger.exception(
                "Relay live stream serving failed; stream_id=%s", stream_id
            )
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "relay_failed",
                    "message": str(exc),
                }
            )
        finally:
            with self._relay_cancel_events_lock:
                self._relay_cancel_events.pop(stream_id, None)
            self._clear_relay_binary_runtime(stream_id)
            self._clear_relay_session_runtime(stream_id)

    def _serve_relay_asset(
        self,
        stream_id: str,
        source_id: str,
        asset_path: str,
        runtime: _RelayBinaryRuntime,
    ) -> None:
        try:
            normalized_asset_path = self._normalize_relay_asset_path(asset_path)
            source = self._get_relay_source(source_id)
            if source is None:
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_missing",
                        "message": "Relay source is no longer available on the agent.",
                    }
                )
                return
            if not isinstance(source, _RelayAssetTreeSource):
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_type_mismatch",
                        "message": "Relay source is not an asset tree.",
                    }
                )
                return

            relative_path = Path(*PurePosixPath(normalized_asset_path).parts)
            asset_file = (source.root_dir / relative_path).resolve()
            try:
                asset_file.relative_to(source.root_dir)
            except ValueError as exc:
                raise RuntimeError("Relay asset path escaped the source root.") from exc
            if not asset_file.is_file():
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "asset_missing",
                        "message": "Relay asset file is not available on the agent.",
                    }
                )
                return

            with asset_file.open("rb") as handle:
                offset = 0
                remaining = asset_file.stat().st_size
                while remaining > 0 and not runtime.cancel_event.is_set():
                    allowed = self._claim_relay_credit(
                        runtime, min(self.RELAY_CHUNK_SIZE, remaining)
                    )
                    if allowed <= 0:
                        break
                    chunk = handle.read(allowed)
                    if chunk == b"":
                        break
                    self._relay_send_binary_chunk(stream_id, offset, chunk)
                    offset += len(chunk)
                    remaining -= len(chunk)

            if not runtime.cancel_event.is_set():
                self._relay_send_text({"type": "eof", "stream_id": stream_id})
        except FileNotFoundError:
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "asset_missing",
                    "message": "Relay asset file is not available on the agent.",
                }
            )
        except Exception as exc:
            logger.exception("Relay asset serving failed; stream_id=%s", stream_id)
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "relay_failed",
                    "message": str(exc),
                }
            )
        finally:
            with self._relay_cancel_events_lock:
                self._relay_cancel_events.pop(stream_id, None)
            self._clear_relay_binary_runtime(stream_id)

    def _serve_relay_text_session(
        self,
        stream_id: str,
        source_id: str,
        cancel_event: threading.Event,
    ) -> None:
        def emit_text(text: str) -> None:
            if cancel_event.is_set():
                return
            self._relay_send_text(
                {"type": "session_output", "stream_id": stream_id, "text": text}
            )

        def close_session() -> None:
            if cancel_event.is_set():
                return
            self._relay_send_text({"type": "eof", "stream_id": stream_id})
            cancel_event.set()
            self._clear_relay_session_runtime(stream_id)
            with self._relay_cancel_events_lock:
                self._relay_cancel_events.pop(stream_id, None)

        try:
            source = self._get_relay_source(source_id)
            if source is None:
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_missing",
                        "message": "Relay source is no longer available on the agent.",
                    }
                )
                return
            if not isinstance(source, _RelaySessionRuntimeSource):
                self._relay_send_text(
                    {
                        "type": "error",
                        "stream_id": stream_id,
                        "code": "source_type_mismatch",
                        "message": "Relay source is not a text session.",
                    }
                )
                return

            input_handler = source.open_session(cancel_event, emit_text, close_session)
            with self._relay_session_runtimes_lock:
                self._relay_session_runtimes[stream_id] = _RelaySessionRuntime(
                    stream_id=stream_id,
                    source_id=source_id,
                    cancel_event=cancel_event,
                    handle_input=input_handler,
                )
        except Exception as exc:
            logger.exception(
                "Relay text session serving failed; stream_id=%s", stream_id
            )
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "relay_failed",
                    "message": str(exc),
                }
            )
            cancel_event.set()
            self._clear_relay_session_runtime(stream_id)
            with self._relay_cancel_events_lock:
                self._relay_cancel_events.pop(stream_id, None)

    def _handle_relay_session_input(self, stream_id: str, text: str) -> None:
        with self._relay_session_runtimes_lock:
            runtime = self._relay_session_runtimes.get(stream_id)

        if runtime is None:
            logger.warning(
                "Relay session input ignored for unknown stream: %s", stream_id
            )
            return

        if runtime.cancel_event.is_set():
            return

        if runtime.handle_input is None:
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "input_not_supported",
                    "message": "This relay session does not accept input.",
                }
            )
            return

        try:
            runtime.handle_input(text)
        except Exception as exc:
            logger.exception(
                "Relay session input handler failed; stream_id=%s", stream_id
            )
            self._relay_send_text(
                {
                    "type": "error",
                    "stream_id": stream_id,
                    "code": "relay_failed",
                    "message": str(exc),
                }
            )
            runtime.cancel_event.set()
            self._clear_relay_session_runtime(stream_id)
            with self._relay_cancel_events_lock:
                self._relay_cancel_events.pop(stream_id, None)

    def _clear_relay_session_runtime(self, stream_id: str) -> None:
        with self._relay_session_runtimes_lock:
            self._relay_session_runtimes.pop(stream_id, None)

    def _relay_send_text(
        self, payload: Mapping[str, Any], *, ws: websocket.WebSocketApp | None = None
    ) -> None:
        if ws is None:
            with self._relay_socket_lock:
                ws = self._relay_socket
        if ws is None:
            raise RuntimeError("Relay socket is not connected")
        with self._relay_send_lock:
            ws.send(json.dumps(dict(payload)))

    def _relay_send_binary_chunk(
        self, stream_id: str, offset: int, data: bytes
    ) -> None:
        with self._relay_socket_lock:
            ws = self._relay_socket
        if ws is None:
            raise RuntimeError("Relay socket is not connected")
        frame = (
            bytes([1, 0]) + stream_id.encode("ascii") + offset.to_bytes(8, "big") + data
        )
        with self._relay_send_lock:
            ws.send(frame, opcode=websocket.ABNF.OPCODE_BINARY)

    def register_config(self, *, raise_on_fail: bool = True) -> bool:
        """Register agent config and obtain an access token."""
        attempts = 5
        for attempt in range(attempts):
            try:
                response = self.post(
                    "config",
                    self.interface.make_config(for_registration=True),
                    basic_auth=True,
                )
            except AgentConnectionError as exc:
                logger.warning(
                    "Cannot connect to broker (attempt %s/%s): %s",
                    attempt + 1,
                    attempts,
                    exc,
                )
                time.sleep(_backoff_seconds(attempt, 1.0, 5.0))
                continue
            except AgentHTTPError as exc:
                logger.exception("Broker returned HTTP error during config: %s", exc)
                if raise_on_fail:
                    raise RuntimeError(str(exc)) from exc
                return False
            except AgentResponseError as exc:
                logger.exception("Broker returned invalid config response: %s", exc)
                if raise_on_fail:
                    raise RuntimeError(str(exc)) from exc
                return False
            if response.get("status") == "ok":
                logger.info(
                    "Agent %s has successfully connected to the broker system!",
                    self.interface.name,
                )
                self.access_token = response["token"]
                # Never log credentials/tokens (even at DEBUG).
                logger.debug("Token issued")
                return True
            if response.get("status") == "error":
                message = response.get("error_msg") or "unknown error"
                logger.exception("Broker rejected config: %s", message)
                if raise_on_fail:
                    raise RuntimeError(message)
                return False
            logger.warning(
                "Cannot connect to the broker system (attempt %s/%s).",
                attempt + 1,
                attempts,
            )
            time.sleep(_backoff_seconds(attempt, 1.0, 5.0))
        message = "Stop try connecting to the broker system."
        logger.exception(message)
        if raise_on_fail:
            raise RuntimeError(message)
        return False

    def heartbeat(self) -> None:
        """Keep the message polling loop alive across deploys and restarts."""
        while self.running:
            if (
                self._connect_thread is None
                or not self._connect_thread.is_alive()
                or (
                    self.last_heartbeat > 0
                    and time.perf_counter() - self.last_heartbeat
                    > self.RESTART_INTERVAL_CRITERIA
                )
            ):
                logger.info("Automatic reconnection...")
                self._ensure_connect_thread()
            if self._relay_thread is None or not self._relay_thread.is_alive():
                logger.info("Automatic relay reconnection...")
                self._ensure_relay_thread()
            time.sleep(self.HEARTBEAT_INTERVAL)

    def _supervise_contract_runs(self) -> None:
        """Detect worker threads that died before reaching a terminal state."""
        while self.running:
            stale_runs: list[_ContractRunState] = []
            finished_ids: list[str] = []
            with self._contract_runs_lock:
                for negotiation_id, state in self._contract_runs.items():
                    if state.thread.is_alive():
                        if not state.finished:
                            self._maybe_renew_contract_lease(state)
                        continue
                    if state.finished or state.cleanup_reported:
                        finished_ids.append(negotiation_id)
                    else:
                        state.cleanup_reported = True
                        stale_runs.append(state)
                        finished_ids.append(negotiation_id)
                for negotiation_id in finished_ids:
                    self._contract_runs.pop(negotiation_id, None)

            for state in stale_runs:
                self._handle_unexpected_contract_death(state)

            self._prune_relay_sources()
            time.sleep(self.SUPERVISOR_INTERVAL)

    def _maybe_renew_contract_lease(self, state: _ContractRunState) -> None:
        if state.phase not in ("running",):
            return
        now = time.perf_counter()
        if now - state.last_lease_sent < self.CONTRACT_LEASE_INTERVAL:
            return
        try:
            self.renew_contract_lease(state.negotiation_id)
            state.last_lease_sent = now
        except AgentError as exc:
            logger.warning(
                "Contract lease renewal failed for %s: %s",
                state.negotiation_id,
                exc,
            )
        except Exception:
            logger.exception(
                "Unexpected error while renewing contract lease; negotiation_id=%s",
                state.negotiation_id,
            )

    def _handle_unexpected_contract_death(self, state: _ContractRunState) -> None:
        message = (
            "Job worker thread terminated unexpectedly before reaching a "
            f"terminal status (phase={state.phase!r})."
        )
        logger.error("%s negotiation_id=%s", message, state.negotiation_id)
        state.job._set_status("error")
        try:
            state.job._report_internal(
                msg=message,
                progress=-1,
                result={
                    "error_msg": message,
                    "error_type": "WorkerThreadUnexpectedTermination",
                    "negotiation_id": state.negotiation_id,
                },
            )
        except Exception:
            logger.exception(
                "Failed to report unexpected worker termination; negotiation_id=%s",
                state.negotiation_id,
            )

    def _register_contract_run(
        self, negotiation_id: str, job: Job, thread: threading.Thread
    ) -> None:
        with self._contract_runs_lock:
            self._contract_runs[negotiation_id] = _ContractRunState(
                negotiation_id=negotiation_id,
                job=job,
                thread=thread,
                started_at=time.perf_counter(),
            )

    def _mark_contract_phase(
        self, negotiation_id: str, phase: ContractRunPhase
    ) -> None:
        with self._contract_runs_lock:
            state = self._contract_runs.get(negotiation_id)
            if state is not None:
                state.phase = phase

    def _finish_contract_run(self, negotiation_id: str) -> None:
        with self._contract_runs_lock:
            state = self._contract_runs.get(negotiation_id)
            if state is not None:
                state.finished = True

    def request_job_cancellation(
        self, job_id: str, *, reason: str | None = None
    ) -> bool:
        """Set cooperative cancellation state for a running job."""
        with self._contract_runs_lock:
            state = self._contract_runs.get(job_id)
            if state is None:
                return False
            state.phase = "terminated"
            state.job._request_cancel(reason)
        self._cancel_relay_streams_for_job(job_id)
        return True

    def _cancel_relay_streams_for_job(self, job_id: str) -> None:
        with self._relay_sources_lock:
            source_ids = {
                source_id
                for source_id, source in self._relay_sources.items()
                if source.job_id == job_id
            }
        if not source_ids:
            return

        with self._relay_binary_runtimes_lock:
            binary_runtimes = [
                runtime
                for runtime in self._relay_binary_runtimes.values()
                if runtime.source_id in source_ids
            ]
        with self._relay_session_runtimes_lock:
            session_runtimes = [
                runtime
                for runtime in self._relay_session_runtimes.values()
                if runtime.source_id in source_ids
            ]

        for runtime in binary_runtimes:
            runtime.cancel_event.set()
            self._notify_relay_binary_runtime(runtime.stream_id)
        for runtime in session_runtimes:
            runtime.cancel_event.set()
            self._clear_relay_session_runtime(runtime.stream_id)

    def connect(self) -> None:
        """Poll the agent message box and dispatch handlers."""
        while self.running:
            try:
                messages = self.check_msgbox()
                self.last_heartbeat = time.perf_counter()
                self._reset_polling_interval()
                for message in messages:
                    threading.Thread(
                        target=self._process_and_ack_message,
                        args=[message],
                        daemon=True,
                    ).start()
            except AgentError as exc:
                logger.warning("Message polling failed: %s", exc)
                self._increase_polling_interval()
            except Exception:
                logger.exception("Error occured during the message processing")
                self._increase_polling_interval()
            time.sleep(self.polling_interval)

    def check_msgbox(self) -> list[Any]:
        """Fetch queued messages from the broker."""
        response = self.get("msgbox")
        messages = response.get("messages")
        if not isinstance(messages, list):
            logger.error("Response messages was not a list: %s", response)
            raise AgentResponseError(
                "Response messages was not a list", payload=response
            )
        return messages

    def _process_and_ack_message(self, message: Mapping[str, Any]) -> None:
        processed = self.process_message(message)
        if not processed:
            return

        msg_id = message.get("_message_box_id")
        claim_token = message.get("_message_box_claim_token")
        if not isinstance(msg_id, str) or not isinstance(claim_token, str):
            return

        try:
            self.ack_msgbox(msg_id, claim_token)
        except AgentError as exc:
            logger.warning("Message ack failed for %s: %s", msg_id, exc)
        except Exception:
            logger.exception("Unexpected error during message ack; %s", msg_id)

    def process_message(self, message: Mapping[str, Any]) -> bool:
        """Dispatch a broker message to the appropriate handler."""
        try:
            logger.debug(f"Message: {message}")
            if "msg_type" not in message or "body" not in message:
                logger.exception(f"Wrong message format: {message}")
                return False
            if message["msg_type"] == "negotiation_request":
                self.process_negotiation_request(message["body"])
            if message["msg_type"] == "negotiation":
                self.process_negotiation(message["body"])
            if message["msg_type"] == "contract":
                self.process_contract(message["body"])
            if message["msg_type"] == "contract_terminate":
                self.process_contract_terminate(message["body"])
            return True
        except Exception:
            logger.exception(f"Error occured during process_message; {message=}")
            return False

    def process_negotiation_request(self, body: Mapping[str, Any]) -> None:
        """Respond with config to a negotiation request."""
        negotiation_id = body["negotiation_id"]
        response = self.interface.make_config()
        self.post(
            "negotiation/response",
            {
                "msg": "need_revision",
                "negotiation_id": negotiation_id,
                "response": response,
            },
            allow_empty=True,
        )

    def process_negotiation(self, body: Mapping[str, Any]) -> None:
        """Validate input and respond to a negotiation request."""
        negotiation_id = body["negotiation_id"]
        response = self.interface.make_config()
        request = body["request"]
        request["_user_info"] = _normalize_user_info_payload(request.get("_user_info"))
        validation_msg, input_response = self.interface.validate(request)
        msg: NegotiationStatus = validation_msg
        response["input"] = input_response
        if msg == "ok" and self.interface.has_func("negotiation"):
            msg, feedback = self.interface.func_dict["negotiation"](request)
            if msg not in ["ok", "need_revision", "ng"]:
                logger.exception(
                    f"Negotiation func in {self.interface.name} returns a wrong msg: should be one of 'ok', 'need_revision' or 'ng'"
                )
            response = _attach_feedback(response, msg, feedback)
        elif msg == "need_revision":
            response = _attach_feedback(response, msg, self.interface.last_feedback)
        else:
            response = _attach_feedback(response, msg, None)

        if (
            msg == "ok"
            and self.interface.has_func("charge_func")
            and (
                not isinstance(response.get("billing"), Mapping)
                or response["billing"].get("mode") != "relay_time"
            )
        ):
            response["charge"] = round(
                self.interface.func_dict["charge_func"](input_response)
            )

        self.post(
            "negotiation/response",
            {"msg": msg, "negotiation_id": negotiation_id, "response": response},
            allow_empty=True,
        )

    def process_contract(self, msg: Mapping[str, Any]) -> None:
        """Run the job function and report progress/results."""
        negotiation_id = msg["negotiation_id"]
        request = msg["request"]
        job = Job(self, negotiation_id, request)
        self.post(
            "contract/accept",
            {"negotiation_id": negotiation_id},
            allow_empty=True,
        )
        self.renew_contract_lease(negotiation_id)
        job._set_status("running")
        worker = threading.Thread(
            target=self._run_running_contract_job,
            args=(cast(str, negotiation_id), job),
            daemon=True,
            name=f"contract-{negotiation_id[:8]}",
        )
        self._register_contract_run(negotiation_id, job, worker)
        self._mark_contract_phase(negotiation_id, "running")
        worker.start()

    def process_contract_terminate(self, msg: Mapping[str, Any]) -> None:
        """Mark a running job for cooperative broker-requested termination."""
        raw_job_id = msg.get("job_id")
        if not isinstance(raw_job_id, str):
            logger.warning("Ignoring malformed contract_terminate message: %r", msg)
            return
        reason = msg.get("reason")
        if reason is not None and not isinstance(reason, str):
            reason = None
        if not self.request_job_cancellation(raw_job_id, reason=reason):
            logger.warning("Termination requested for unknown job: %s", raw_job_id)

    def _run_contract_job(self, msg: Mapping[str, Any], job: Job) -> None:
        """Backward-compatible wrapper for tests and direct callers."""
        negotiation_id = cast(str, msg["negotiation_id"])
        self.post(
            "contract/accept",
            {"negotiation_id": negotiation_id},
            allow_empty=True,
        )
        self.renew_contract_lease(negotiation_id)
        job._set_status("running")
        self._mark_contract_phase(negotiation_id, "running")
        self._run_running_contract_job(negotiation_id, job)

    def _run_running_contract_job(self, negotiation_id: str, job: Job) -> None:
        st = time.perf_counter()
        try:
            job.raise_if_cancelled()
            job.msg(f"{self.interface.name} starts running...")

            result = self.interface.func_dict["job_func"](job)
            job.raise_if_cancelled()
            logger.debug(f"JOB RETURN VALUE: {result}")
            if result is None:
                result = {}
            job._set_status("done")
            self._mark_contract_phase(negotiation_id, "done")
            try:
                job.report(msg="Job done.", progress=1, result=result)
            except Exception:
                logger.exception("Failed to report job completion")
        except BaseException as exc:
            logger.exception(
                "Error occured during the job execution; negotiation_id=%s",
                negotiation_id,
            )
            job._set_status("error")
            self._mark_contract_phase(negotiation_id, "error")
            try:
                error_summary = _job_error_summary(exc)
                error_result = _job_error_result(
                    exc, self.interface.job_error_detail_level
                )
                job._report_internal(
                    msg=f"Job failed with {error_summary}.",
                    progress=-1,
                    result=error_result,
                )
            except Exception:
                logger.exception("Failed to report job error status")
        finally:
            if job._status not in ["done", "error"]:
                message = (
                    "Job worker exited without reporting a terminal status; "
                    f"status={job._status!r}"
                )
                logger.error("%s negotiation_id=%s", message, negotiation_id)
                job._set_status("error")
                self._mark_contract_phase(negotiation_id, "error")
                try:
                    job._report_internal(
                        msg=message,
                        progress=-1,
                        result={
                            "error_msg": message,
                            "error_type": "WorkerExitedWithoutTerminalStatus",
                            "negotiation_id": negotiation_id,
                        },
                    )
                except Exception:
                    logger.exception(
                        "Failed to report non-terminal worker exit; negotiation_id=%s",
                        negotiation_id,
                    )
            self._finish_contract_run(negotiation_id)
            logger.debug(f"{negotiation_id} took {time.perf_counter()-st:.3f} s.")

    def header(self, basic_auth: bool = False, upload: bool = False) -> JsonDict:
        """Return request headers for JSON or multipart uploads."""
        if upload:
            headers = {}
        else:
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if basic_auth:
            headers.update({"authorization": f"Basic {self.auth}"})
        elif self.access_token:
            headers.update({"authorization": f"Token {self.access_token}"})
            headers.update({"x-brokersystem-msgbox-ack": "1"})

        return headers

    def _request_json_with_retry(
        self,
        method: str,
        uri: str,
        payload: JsonDict | None = None,
        *,
        basic_auth: bool = False,
        retries: float | None = None,
        allow_empty: bool = False,
    ) -> JsonDict:
        url = f"{self.broker_url}/api/v1/agent/{uri}"
        retry_deadline = (
            self.REQUEST_RETRY_DEADLINE if retries is None else max(float(retries), 0.0)
        )
        started_at = time.perf_counter()
        attempt = 0
        while True:
            timeout = _request_timeout_tuple(
                started_at,
                retry_deadline,
                connect_timeout=self.REQUEST_CONNECT_TIMEOUT,
                read_timeout_cap=self.REQUEST_READ_TIMEOUT,
            )
            try:
                response = self._http_session().request(
                    method,
                    url,
                    json=payload,
                    headers=self.header(basic_auth),
                    timeout=timeout,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Connection error on %s request; %s (attempt %s, deadline %.1fs)",
                    method,
                    uri,
                    attempt + 1,
                    retry_deadline,
                )
                logger.debug("Connection error details: %s", exc)
                if _sleep_for_retry(
                    started_at,
                    retry_deadline,
                    attempt,
                    self.REQUEST_RETRY_BASE,
                    self.REQUEST_RETRY_MAX,
                ):
                    attempt += 1
                    continue
                logger.error(
                    "Giving up %s request after %.1fs; %s",
                    method,
                    retry_deadline,
                    uri,
                )
                raise AgentConnectionError(
                    f"Connection error on {method} request; {uri}"
                ) from exc

            if response.status_code == 401 and not basic_auth:
                if self.register_config(raise_on_fail=False):
                    continue
                logger.error("Re-auth failed; aborting %s request; %s", method, uri)
                raise AgentHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )
            if _retryable_http_status(response.status_code):
                logger.warning(
                    "Transient HTTP status on %s request; %s status=%s "
                    "(attempt %s, deadline %.1fs)",
                    method,
                    uri,
                    response.status_code,
                    attempt + 1,
                    retry_deadline,
                )
                logger.debug(
                    "Retryable HTTP response content on %s %s: %r",
                    method,
                    uri,
                    response.content,
                )
                if _sleep_for_retry(
                    started_at,
                    retry_deadline,
                    attempt,
                    self.REQUEST_RETRY_BASE,
                    self.REQUEST_RETRY_MAX,
                    status_code=response.status_code,
                    retry_after=_retry_after_seconds(response.headers),
                ):
                    attempt += 1
                    continue
                logger.error(
                    "Giving up %s request after retryable HTTP status %s and %.1fs; %s",
                    method,
                    response.status_code,
                    retry_deadline,
                    uri,
                )
                raise AgentHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )
            if response.status_code != 200:
                logger.error(
                    "Not 200: status=%s; uri=%s; response=%s",
                    response.status_code,
                    uri,
                    response.content,
                )
                raise AgentHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )
            try:
                obj = response.json()
            except ValueError as exc:
                logger.error("Non-JSON response for %s request; %s", method, uri)
                raise AgentResponseError(
                    f"{method} {uri} returned non-JSON response"
                ) from exc
            if not isinstance(obj, dict):
                logger.error("Response was not a dict: %r", obj)
                raise AgentResponseError(
                    f"{method} {uri} returned non-dict response", payload=obj
                )
            if not obj:
                if allow_empty:
                    return {}
                logger.error("Empty response for %s request; %s", method, uri)
                raise AgentResponseError(
                    f"{method} {uri} returned empty response", payload=obj
                )
            if not all(isinstance(k, str) for k in obj.keys()):
                logger.error("Some response keys were not str: %s", obj.keys())
                raise AgentResponseError(
                    f"{method} {uri} returned non-str keys", payload=obj
                )
            return obj
        raise AgentConnectionError(f"Connection error on {method} request; {uri}")

    def post(
        self,
        uri: str,
        payload: JsonDict,
        basic_auth: bool = False,
        *,
        allow_empty: bool = False,
    ) -> JsonDict:
        """POST to the agent API and return JSON response.

        Retries transient connection errors with backoff and logs failures.
        """
        return self._request_json_with_retry(
            "POST",
            uri,
            payload,
            basic_auth=basic_auth,
            allow_empty=allow_empty,
        )

    def get(self, uri: str, basic_auth: bool = False) -> JsonDict:
        """GET from the agent API and return JSON response.

        Retries transient connection errors with backoff and logs failures.
        """
        return self._request_json_with_retry("GET", uri, basic_auth=basic_auth)

    def upload(self, file_type: str, binary_data: bytes) -> JsonDict:
        """Upload a binary payload and return the broker file id.

        Retries transient connection errors with backoff and logs failures.

        Raises:
            AgentConnectionError: If the broker cannot be reached after retries.
            AgentHTTPError: If the broker responds with a non-200 HTTP status.
            AgentResponseError: If the broker responds with malformed JSON.
            AgentUploadError: If the broker rejects the upload payload, including
                storage-capacity failures.
        """
        extension = file_type.strip().lower().lstrip(".")
        file_name = f"__file_name__.{extension}" if extension else "__file_name__"
        content_type, _encoding = mimetypes.guess_type(file_name)
        if content_type is None:
            content_type = "application/octet-stream"
        files = {"file": (file_name, binary_data, content_type)}
        started_at = time.perf_counter()
        attempt = 0
        while True:
            timeout = _request_timeout_tuple(
                started_at,
                self.REQUEST_RETRY_DEADLINE,
                connect_timeout=self.REQUEST_CONNECT_TIMEOUT,
                read_timeout_cap=self.UPLOAD_READ_TIMEOUT,
            )
            try:
                response = self._http_session().post(
                    f"{self.broker_url}/api/v1/agent/upload",
                    headers=self.header(upload=True),
                    files=files,
                    timeout=timeout,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Connection error during upload (attempt %s, deadline %.1fs)",
                    attempt + 1,
                    self.REQUEST_RETRY_DEADLINE,
                )
                logger.debug("Upload connection error details: %s", exc)
                if _sleep_for_retry(
                    started_at,
                    self.REQUEST_RETRY_DEADLINE,
                    attempt,
                    self.REQUEST_RETRY_BASE,
                    self.REQUEST_RETRY_MAX,
                ):
                    attempt += 1
                    continue
                logger.error(
                    "Giving up upload after %.1fs", self.REQUEST_RETRY_DEADLINE
                )
                raise AgentConnectionError("Connection error during upload") from exc
            if response.status_code == 401:
                if self.register_config(raise_on_fail=False):
                    continue
                logger.error("Re-auth failed; aborting upload")
                raise AgentHTTPError(
                    response.status_code,
                    "upload",
                    _coerce_error_content(response.content),
                )
            if _retryable_http_status(response.status_code):
                logger.warning(
                    "Transient HTTP status during upload: status=%s "
                    "(attempt %s, deadline %.1fs)",
                    response.status_code,
                    attempt + 1,
                    self.REQUEST_RETRY_DEADLINE,
                )
                logger.debug("Retryable upload response content: %r", response.content)
                if _sleep_for_retry(
                    started_at,
                    self.REQUEST_RETRY_DEADLINE,
                    attempt,
                    self.REQUEST_RETRY_BASE,
                    self.REQUEST_RETRY_MAX,
                    status_code=response.status_code,
                    retry_after=_retry_after_seconds(response.headers),
                ):
                    attempt += 1
                    continue
                logger.error(
                    "Giving up upload after retryable HTTP status %s and %.1fs",
                    response.status_code,
                    self.REQUEST_RETRY_DEADLINE,
                )
                raise AgentHTTPError(
                    response.status_code,
                    "upload",
                    _coerce_error_content(response.content),
                )
            if response.status_code != 200:
                logger.error(
                    "Upload failed: status=%s; response=%s",
                    response.status_code,
                    response.content,
                )
                raise AgentHTTPError(
                    response.status_code,
                    "upload",
                    _coerce_error_content(response.content),
                )
            try:
                obj = response.json()
            except ValueError as exc:
                logger.error("Upload returned non-JSON response")
                raise AgentResponseError("Upload returned non-JSON response") from exc
            if not isinstance(obj, dict):
                logger.error("Upload response was not a dict: %r", obj)
                raise AgentResponseError("Upload response was not a dict", payload=obj)
            if not obj:
                logger.error("Upload returned empty response")
                raise AgentResponseError("Upload returned empty response", payload=obj)
            if obj.get("status") == "error":
                # Upload responses use the same canonical broker error envelope:
                # error_msg = detail, error = optional machine-readable code.
                code = obj.get("error")
                error_msg = obj.get("error_msg") or "Upload rejected by broker"
                logger.error("Upload rejected by broker: code=%s payload=%r", code, obj)
                raise AgentUploadError(error_msg, code=code, payload=obj)
            return obj

    def ack_msgbox(self, msg_id: str, claim_token: str) -> None:
        """Acknowledge successful processing of a claimed message."""
        self.post(
            "msgbox/ack",
            {"messages": [{"msg_id": msg_id, "claim_token": claim_token}]},
            allow_empty=True,
        )

    def renew_contract_lease(self, negotiation_id: str) -> None:
        """Refresh the running-contract lease on the broker."""
        self._request_json_with_retry(
            "POST",
            "contract/lease",
            {
                "negotiation_id": negotiation_id,
                "lease_owner": self.runtime_instance_id,
            },
            retries=self.CONTRACT_LEASE_RETRY_DEADLINE,
            allow_empty=True,
        )

    def terminate_job(
        self, job_id: str, reason: str = "agent_requested"
    ) -> StatusResponse:
        """Request broker-side termination of a job using agent auth.

        Relay jobs are keyed by `negotiation_id`; pass that value as `job_id`.
        """
        response = self.post(
            "contract/terminate",
            {"job_id": job_id, "reason": reason},
        )
        _require_key(response, "status", "terminate_job")
        return cast(StatusResponse, response)

    def register_func(self, func_name: str, func: Callable[..., Any]) -> None:
        """Register a handler function on the agent."""
        self.agent_funcs[func_name] = func

    ##### wrapper functions #####
    def config(self, func: Callable[P, None]) -> Callable[P, None]:
        """Decorator for the configuration builder.

        Optional: You may omit this if you set `agent.name` and
        `agent.agent_auth` programmatically before running the agent.
        """

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
            return func(*args, **kwargs)

        self.register_func("make_config", wrapper)
        return wrapper

    def negotiation(
        self,
        func: Callable[[JsonDict], NegotiationDecision],
    ) -> Callable[[JsonDict], NegotiationDecision]:
        """Register custom negotiation logic for broker requests.

        If this decorator is omitted, the SDK applies schema validation only.

        The callback receives the submitted request values for the current
        round. When user info disclosure is enabled, `_user_info` is included
        in the request.

        Return `ok_response(...)`, `need_revision_response(...)`, or
        `ng_response(...)`. Use `message` for a round-level note and `fields`
        for sparse per-parameter notes. Do not return raw broker transport
        envelopes such as `{"status": "error", ...}` from user code.
        """

        @functools.wraps(func)
        def wrapper(request: JsonDict) -> NegotiationDecision:
            return func(request)

        self.register_func("negotiation", wrapper)
        return wrapper

    def charge_func(self, func: Callable[[JsonDict], int]) -> Callable[[JsonDict], int]:
        """Decorator for custom charge calculation.

        Optional: If omitted, the configured `agent.charge` is used.
        """

        @functools.wraps(func)
        def wrapper(input_values: JsonDict) -> int:
            return func(input_values)

        self.register_func("charge_func", wrapper)
        return wrapper

    def job_func(
        self, func: Callable[P, JsonDict | None]
    ) -> Callable[P, JsonDict | None]:
        """Decorator for the main job function.

        The job argument (if provided) implements Mapping[str, Any] semantics.
        """

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> JsonDict | None:
            return func(*args, **kwargs)

        self.register_func("job_func", wrapper)
        return wrapper

    @classmethod
    def _automatic_run(cls) -> list["Agent"]:
        """Run auto-built agents without interactive input."""
        agent_list = list[Agent]()
        for agent in cls._automatic_built_agents.values():
            agent.run(_automatic=True)
            agent_list.append(agent)
        return agent_list

    @classmethod
    def _is_automatic_mode(cls) -> bool:
        """Return True if any auto-built agents exist."""
        return len(cls._automatic_built_agents) > 0

    @classmethod
    def make(cls, name: str, **agent_kwargs: Any) -> AgentFactory:
        """Decorator-based factory for auto-built agents."""

        def make_func(func: Callable[..., JsonDict | None]):
            # automatic_flag = "_automatic" in agent_kwargs and agent_kwargs["_automatic"]
            agent = AgentConstructor.make(name=name, job_func=func, kwargs=agent_kwargs)
            # agent.run(_automatic=automatic_flag)

            def wrapper(*args, **kwargs):
                logger.warning(
                    "Direct call of the agent job function for the start up will be disabled in future. Change it to Agent.start().",
                    stack_info=True,
                )
                cls.start()
                return agent

            cls._automatic_built_agents[name] = agent
            return wrapper

        return make_func

    @classmethod
    def add_config(
        cls,
        broker_url: str | None = None,
        agent_auth: str | None = None,
        secret_token: str | None = None,
    ) -> Callable[[Callable[P, R]], Callable[P, R]]:
        """Decorator to inject broker_url/agent_auth into a job func."""
        config_dict = dict[str, str]()
        if broker_url is not None:
            config_dict["broker_url"] = broker_url
        if (
            agent_auth is not None
            and secret_token is not None
            and agent_auth != secret_token
        ):
            raise ValueError(
                "agent_auth and secret_token must match when both are provided"
            )
        if agent_auth is not None:
            config_dict["agent_auth"] = agent_auth
        elif secret_token is not None:
            config_dict["agent_auth"] = secret_token

        def add_config_func(func: Callable[P, R]) -> Callable[P, R]:
            @functools.wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                return func(*args, **kwargs)

            wrapper._additional_config = (  # pyright: ignore[reportAttributeAccessIssue]
                config_dict
            )
            wrapper._original_keys = (  # pyright: ignore[reportAttributeAccessIssue]
                list(inspect.signature(func).parameters)
            )
            return wrapper

        return add_config_func

    ##### interface for agent interface #####
    @property
    def input(self):
        """Input schema container used during negotiation."""
        return self.interface.input

    @property
    def output(self):
        """Output schema container used to format results."""
        return self.interface.output

    @property
    def condition(self):
        """Condition schema container for optional negotiation inputs."""
        return self.interface.condition

    @property
    def agent_auth(self):
        """`agent_auth` credential string used for `/api/v1/agent/*` requests."""
        return self.interface.agent_auth

    @agent_auth.setter
    def agent_auth(self, auth: str):
        self.interface.agent_auth = auth

    @property
    def secret_token(self):
        """Deprecated alias for `agent_auth`."""
        return self.interface.agent_auth

    @secret_token.setter
    def secret_token(self, token: str):
        self.interface.agent_auth = token

    @property
    def name(self):
        """Agent name shown in broker listings."""
        return self.interface.name

    @name.setter
    def name(self, name: str):
        self.interface.name = name

    @property
    def convention(self):
        """Optional convention label included in the agent config."""
        return self.interface.convention

    @convention.setter
    def convention(self, convention: str):
        self.interface.convention = convention

    @property
    def description(self):
        """Human-readable description shown to users."""
        return self.interface.description

    @description.setter
    def description(self, description: str):
        self.interface.description = description

    @property
    def charge(self):
        """Default charge used when no `charge_func` is provided.

        Note: The broker platform fee is 20% plus 10% per requested user info
        field (deducted from the agent payout).
        """
        return self.interface.charge

    @charge.setter
    def charge(self, charge: int):
        self.interface.charge = charge

    @property
    def relay_points_per_minute(self) -> int | float | None:
        """Interactive relay price in points/minute.

        This is used for relay_file, relay_media, and relay_session outputs and
        leaves `charge` available for ordinary batch jobs.
        """
        return self.interface.relay_points_per_minute

    @relay_points_per_minute.setter
    def relay_points_per_minute(self, points_per_minute: int | float | None) -> None:
        if points_per_minute is None:
            self.interface.relay_points_per_minute = None
            return
        if not isinstance(points_per_minute, (int, float)) or isinstance(
            points_per_minute, bool
        ):
            raise TypeError("relay_points_per_minute must be a number or None")
        if points_per_minute <= 0:
            raise ValueError("relay_points_per_minute must be > 0")
        self.interface.relay_points_per_minute = points_per_minute

    @property
    def interactive_points_per_minute(self) -> int | float | None:
        """Alias for `relay_points_per_minute`."""
        return self.relay_points_per_minute

    @interactive_points_per_minute.setter
    def interactive_points_per_minute(
        self, points_per_minute: int | float | None
    ) -> None:
        self.relay_points_per_minute = points_per_minute

    @property
    def job_error_detail_level(self) -> JobErrorDetailLevel:
        """How much job exception detail the broker/UI should receive."""
        return self.interface.job_error_detail_level

    @job_error_detail_level.setter
    def job_error_detail_level(self, value: JobErrorDetailLevel) -> None:
        self.interface.job_error_detail_level = _normalize_job_error_detail_level(value)

    @property
    def user_info_request(self) -> list[UserInfoField]:
        """List of requested user info fields for negotiation consent."""
        return list(self.interface.user_info_request)

    @user_info_request.setter
    def user_info_request(self, fields: Iterable[UserInfoField]) -> None:
        self.interface.user_info_request = _normalize_user_info_fields(fields)

    @property
    def ui_preview(self) -> UiPreview | None:
        """Optional UI preview declaration included in agent config.

        The broker UI may render this declaration (e.g., a Vega spec) during
        negotiation. Treat it as public: do not embed secrets.
        """

        return self.interface.ui_preview

    @ui_preview.setter
    def ui_preview(self, value: Mapping[str, Any] | None) -> None:
        if value is None:
            self.interface.ui_preview = None
            return
        if not isinstance(value, Mapping):
            raise TypeError("agent.ui_preview must be a dict-like mapping")
        self.interface.ui_preview = _normalize_ui_preview(value)

    def request_user_info(
        self,
        *,
        user_id: bool = False,
        email: bool = False,
        name_affiliation: bool = False,
    ) -> None:
        """Request user info during negotiation.

        Approved fields are delivered under `_user_info` in negotiation requests
        and exposed as `job.user_info` during contract execution.
        """
        fields: list[UserInfoField] = []
        if user_id:
            fields.append(UserInfoField.USER_ID)
        if email:
            fields.append(UserInfoField.EMAIL)
        if name_affiliation:
            fields.append(UserInfoField.NAME_AFFILIATION)
        self.user_info_request = fields


class AgentConstructor:
    """Builds agent config interactively and wires it to an Agent instance."""

    def __init__(
        self,
        name: str,
        job_func: Callable[..., JsonDict | None],
        kwargs: Mapping[str, Any],
    ) -> None:
        self.name: str = name
        self.job_func: Callable[..., JsonDict | None] = job_func
        self.kwargs: dict[str, Any] = dict(kwargs)
        self.config: JsonDict = {}
        self.config_changed: bool = False
        self.agent: Agent | None = None

    @classmethod
    def make(
        cls,
        name: str,
        job_func: Callable[..., JsonDict | None],
        kwargs: Mapping[str, Any],
    ) -> Agent:
        """Create a configured Agent from a job function."""
        constructor = cls(name, job_func, kwargs)
        constructor.load_config()
        constructor.config["name"] = name
        constructor.fill_config()
        if constructor.config_changed:
            constructor.dump_config()
        if not ("skip_config" in kwargs and kwargs["skip_config"]):
            constructor.investigate_job_func()
            constructor.dump_config()

        constructor.agent = Agent(constructor.config["broker_url"])

        constructor.agent.register_func("make_config", constructor.make_config_func)
        constructor.agent.register_func("job_func", constructor.agent_job_func)
        return constructor.agent

    def investigate_job_func(self) -> None:
        """Run the job function once to infer input/output schemas."""
        input_params = self.fill_input_params()
        if self.config_changed:
            self.dump_config()
        if "job" in self.job_func_keys:
            input_params["job"] = DummyJob(
                input_params,
                {
                    "broker_url": self.config["broker_url"],
                    "agent_auth": self.config["agent_auth"],
                },
            )
        result = self.job_func(**input_params)
        if result is None:
            result = {}
        self.fill_output_format(result)

    def fill_input_params(self) -> JsonDict:
        """Interactively populate input schema and return defaults."""
        if "input" not in self.config:
            self.config["input"] = {"@keys": []}
        if "condition" not in self.config:
            self.config["condition"] = {"@keys": []}
        input_config = cast(dict[str, Any], self.config.get("input", {}))
        constraints = cast(dict[str, Any], input_config.get("@constraints", {}))
        container = TemplateContainer("input")
        updated = False
        input_keys = cast(list[str], input_config.get("@keys", []))
        keys_to_remove = set(input_keys)
        default_value_dict = dict[str, Any]()
        for key in self.job_func_keys:
            if key in ["job"]:
                continue
            if key in keys_to_remove:
                keys_to_remove.discard(key)
            if key not in input_keys:
                if not updated:
                    print("")
                    print("--- Input ---")
                updated = True
                setattr(container, key, self.ask_input_format(key))
                if container[key].type in ["number", "string"]:
                    default_value_dict[key] = container[key].constraint_dict["default"]
                elif container[key].type in ["choice"]:
                    default_value_dict[key] = container[key].constraint_dict["choices"][
                        0
                    ]
                self.config_changed = True
            else:
                constraint = cast(dict[str, Any], constraints.get(key, {}))
                if "default" in constraint:
                    default_value_dict[key] = constraint["default"]
                elif "choices" in constraint:
                    default_value_dict[key] = constraint["choices"][0]
        if updated:
            self.merge_config_and_container("input", container)
        self.remove_unused_keys("input", keys_to_remove)
        return default_value_dict

    def merge_config_and_container(
        self, item_type: str, container: TemplateContainer
    ) -> None:
        template_dict = container._get_template()
        for key, value in template_dict.items():
            assert key.startswith("@"), "Property keys should start with @"
            if isinstance(value, dict):
                if key not in self.config[item_type]:
                    self.config[item_type][key] = {}
                self.config[item_type][key].update(value)
            elif isinstance(value, list):
                if key not in self.config[item_type]:
                    self.config[item_type][key] = []
                s = set(self.config[item_type][key])
                self.config[item_type][key] = list(s.union(value))

    def remove_unused_keys(self, item_type: str, keys_to_remove: set[str]) -> None:
        if len(keys_to_remove) == 0:
            return
        for property_key in self.config[item_type]:
            for key in keys_to_remove:
                print(item_type, property_key, self.config[item_type][property_key])
                if isinstance(self.config[item_type][property_key], dict):
                    if key in self.config[item_type][property_key]:
                        del self.config[item_type][property_key][key]
                if isinstance(self.config[item_type][property_key], list):
                    s = set(self.config[item_type][property_key])
                    s.discard(key)
                    self.config[item_type][property_key] = list(s)

    def fill_output_format(self, result: JsonDict) -> None:
        """Infer output schema from a job function result."""
        if "output" not in self.config:
            self.config["output"] = {"@keys": []}
        updated = False
        keys_to_remove = set(self.config["output"]["@keys"])
        container = TemplateContainer("output")
        for key, value in result.items():
            if key in keys_to_remove:
                keys_to_remove.discard(key)
            if key not in self.config["output"]["@keys"] or (
                isinstance(value, pd.DataFrame)
                and any(
                    [c not in self.config["output"]["@unit"] for c in value.columns]
                )
            ):
                if not updated:
                    print("")
                    print("--- Output ---")
                updated = True
                template = ValueTemplate.guess_from_value(
                    value, unit_callback_func=self.ask_output_format, key=key
                )
                template.help = self.ask_template_help(key)
                setattr(
                    container,
                    key,
                    template,
                )
        if updated:
            self.merge_config_and_container("output", container)
        self.remove_unused_keys("output", keys_to_remove)

    def ask_input_format(self, key: str) -> ValueTemplate:
        """Prompt for input template details when missing in config."""
        while True:
            value_type = input(
                f'Select the value type of "{key}" '
                "('n':Number, 's': String, 'c': Choice, 'b': Bool):"
            )
            if value_type in ["n", "s", "c", "b"]:
                break
        unit = None
        if value_type in ["n", "c"]:
            unit = input(f'--> Unit of "{key}" (empty if none):')
            if len(unit) == 0:
                unit = None
        help_text = self.ask_template_help(key)
        if value_type == "n":
            default_value = float(input(f'--> Default value of "{key}":'))
            print(
                f"== {key}: {default_value} {'('+unit+')' if unit is not None else ''} =="
            )
            print("")
            return Number(value=default_value, unit=unit, help=help_text)
        elif value_type == "c":
            choices = input(f'--> Choices separated by a comma of "{key}":')
            choices = [c.strip() for c in choices.split(",")]
            print(f"== {key}: {choices} {'('+unit+')' if unit is not None else ''}")
            print("")
            return Choice(choices=choices, unit=unit, help=help_text)
        elif value_type == "s":
            default_value = input(f'--> Default value of "{key}":')
            print(
                f"== {key}: {default_value} {'('+unit+')' if unit is not None else ''} =="
            )
            print("")
            return String(string=default_value, help=help_text)
        elif value_type == "b":
            while True:
                default_value_raw = input(
                    f'--> Default value of "{key}" (true/false):'
                ).strip()
                normalized = default_value_raw.lower()
                if normalized in ["true", "false"]:
                    default_value = normalized == "true"
                    break
            print(f"== {key}: {default_value} ==")
            print("")
            return Bool(value=default_value, help=help_text)
        raise AssertionError("Unreachable input format branch")

    def ask_output_format(self, key: str) -> str | None:
        """Prompt for output unit metadata."""
        unit = input(f'Unit of "{key}" (empty if none) > ')
        print("")
        return unit

    def ask_template_help(self, key: str) -> str | None:
        """Prompt for optional help text metadata."""
        help_text = input(f'Help text of "{key}" (empty if none) > ').strip()
        print("")
        return help_text or None

    def load_config(self) -> None:
        """Load config JSON if it exists; otherwise start fresh."""
        self.config_changed = False
        try:
            with self.config_path.open() as f:
                self.config = json.load(f)
        except FileNotFoundError:
            self.config = {}
        except json.JSONDecodeError as exc:
            logger.exception("Invalid config JSON: %s", self.config_path)
            raise exc
        except Exception as exc:
            logger.exception("Failed to load config: %s", self.config_path)
            raise exc

    def fill_config(self) -> None:
        """Fill required top-level config values (broker_url, agent_auth, etc.)."""
        broker_url = self.fill_config_item("broker_url")
        if not broker_url.startswith("http"):
            raise ValueError("broker_url should be properly specified.")
        agent_auth = self.fill_agent_auth_item()
        if len(agent_auth) == 0:
            raise ValueError("agent_auth should be set")
        charge = self.fill_config_item("charge", int)
        if charge <= 0:
            raise ValueError("charge should be larger than 0")
        _ = self.fill_config_item("description")

    def fill_agent_auth_item(self) -> str:
        """Fill `agent_auth`."""
        if "agent_auth" in self.config:
            value = str(self.config["agent_auth"])
        elif "secret_token" in self.config:
            value = str(self.config["secret_token"])
            self.config_changed = True
        elif hasattr(self.job_func, "_additional_config") and "agent_auth" in getattr(
            self.job_func, "_additional_config"
        ):
            value = str(getattr(self.job_func, "_additional_config")["agent_auth"])
            self.config_changed = True
        elif hasattr(self.job_func, "_additional_config") and "secret_token" in getattr(
            self.job_func, "_additional_config"
        ):
            value = str(getattr(self.job_func, "_additional_config")["secret_token"])
            self.config_changed = True
        elif "agent_auth" in self.kwargs:
            value = str(self.kwargs["agent_auth"])
            self.config_changed = True
        elif "secret_token" in self.kwargs:
            value = str(self.kwargs["secret_token"])
            self.config_changed = True
        else:
            value = input("agent_auth:")
            self.config_changed = True

        self.config.pop("secret_token", None)
        self.config["agent_auth"] = value
        return value

    def fill_config_item(self, key: str, astype: type = str) -> Any:
        """Read a config item from decorator, kwargs, or user input."""
        if hasattr(self.job_func, "_additional_config") and key in getattr(
            self.job_func, "_additional_config"
        ):
            self.config[key] = getattr(self.job_func, "_additional_config")[key]
        elif key in self.kwargs:
            if key in self.config and self.config[key] == astype(self.kwargs[key]):
                return self.config[key]
            self.config[key] = astype(self.kwargs[key])
            self.config_changed = True
        else:
            user_input = input(f"{key}:")
            self.config[key] = astype(user_input)
            self.config_changed = True
        return self.config[key]

    def make_config_func(self) -> None:
        """Apply loaded config values to the Agent instance."""
        assert self.agent is not None, "Agent has not been properly prepared."
        keys = ["name", "charge", "description"]
        for key in keys:
            setattr(self.agent, key, self.config[key])
        self.agent.agent_auth = str(self.config["agent_auth"])

        item_types = ["input", "output", "condition"]
        for item_type in item_types:
            getattr(self.agent, item_type)._load(self.config[item_type])

    def agent_job_func(self, job: Job) -> JsonDict:
        """Adapter that calls the original job function."""
        assert self.agent is not None, "Agent has not been properly prepared."
        input_params = self.job_to_params(job)
        if "estimated_time" in self.kwargs:
            job.periodic_report(self.kwargs["estimated_time"])
        result = self.job_func(**input_params)
        if result is None:
            result = {}
        return result

    def job_to_params(self, job: Job) -> JsonDict:
        input_keys = self.job_func_keys
        params = {key: job[key] for key in input_keys if key in job}
        if "job" in input_keys:
            params["job"] = job
        return params

    def dump_config(self) -> None:
        """Persist config JSON to disk."""
        self.config_path.parent.mkdir(exist_ok=True)

        with self.config_path.open("w") as f:
            json.dump(self.config, f)
        self.config_changed = False

    @property
    def job_func_keys(self) -> list[str]:
        if hasattr(self.job_func, "_original_keys"):
            return getattr(self.job_func, "_original_keys")
        return list(inspect.signature(self.job_func).parameters)

    @property
    def config_path(self) -> Path:
        return Path(f"./configs/{self.name}.json")


class Broker:
    """Client for `/api/v1/client/*`.

    `/api/v1/client/board` mirrors `/api/v1/broker/board` payloads but accepts
    either `agent_auth` or user tokens.
    """

    def __init__(
        self,
        job: Job | None = None,
        broker_url: str | None = None,
        auth: str | None = None,
    ) -> None:
        """Create a client for `/api/v1/client/*`.

        Args:
            job: Existing Job wrapper; uses the agent's configured `agent_auth`.
            broker_url: Base URL used for `/api/v1/client/*`.
            auth: Credential string for the client API. Accepted values:
                - bare user token string
                - bare ephemeral token string
                - `agent_auth` string

                Do not prefix token values with `"Token "`.
        """
        if job is None:
            assert broker_url is not None and auth is not None
            self.broker_url = broker_url
            self.auth = auth
        elif isinstance(job, Job) or hasattr(job, "_to_show_i_am_job_class"):
            self.broker_url = job._agent.broker_url
            self.auth = job._agent.interface.agent_auth
        else:
            raise TypeError
        self._http_session_local = threading.local()

    def _http_session(self) -> requests.Session:
        return _get_thread_session(self._http_session_local)

    def ask(
        self,
        agent_id: str,
        request: JsonDict,
        *,
        user_info_consent: Iterable[UserInfoField] | None = None,
        max_duration_minutes: int | float | None = None,
    ) -> ResultResponse:
        """Execute negotiate -> contract -> wait for result.

        Args:
            agent_id: Target agent id.
            request: Input payload for negotiation. For `file` / `image` inputs,
                pass a broker file id string or a local `Path` / bytes value.
                Local file values are uploaded automatically before negotiation.
            user_info_consent: Optional consent fields (`user_id`, `email`,
                `name_affiliation`) to include in the request.
            max_duration_minutes: Optional maximum relay interactive duration;
                sent as `_broker.max_duration_minutes`.

        Returns:
            Result payload once the job finishes.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerUploadError: If the broker rejects an input file upload.
            BrokerResponseError: If the response payload is malformed or indicates error
                (including negotiation states other than "ok").
            TypeError: If a local file input value is not bytes or a valid path-like.
            OSError: If a local file input path cannot be read.
        """
        negotiation = self.negotiate(
            agent_id,
            request,
            user_info_consent=user_info_consent,
            max_duration_minutes=max_duration_minutes,
        )
        state = _require_key(negotiation, "state", "negotiate")
        if state != "ok":
            content = negotiation["content"]
            error_msg = content["feedback"]["message"]
            message = f"Negotiation returned {state}"
            if error_msg:
                message = f"{message}: {error_msg}"
            raise BrokerResponseError(message, payload=negotiation)
        negotiation_id = _require_key(negotiation, "negotiation_id", "negotiate")
        self.contract(cast(str, negotiation_id))
        return self.get_result(cast(str, negotiation_id))

    def execute(
        self,
        agent_id: str,
        request: JsonDict,
        *,
        user_info_consent: Iterable[UserInfoField] | None = None,
        max_duration_minutes: int | float | None = None,
    ) -> ResultResponse:
        """Alias for `ask(...)`."""
        return self.ask(
            agent_id,
            request,
            user_info_consent=user_info_consent,
            max_duration_minutes=max_duration_minutes,
        )

    def negotiate(
        self,
        agent_id: str,
        request: JsonDict,
        *,
        user_info_consent: Iterable[UserInfoField] | None = None,
        max_duration_minutes: int | float | None = None,
    ) -> NegotiationResponse:
        """Start negotiation and send the first request.

        Use `user_info_consent` to approve sharing `user_id`, `email`,
        and/or `name_affiliation` when the agent requests it.

        Args:
            agent_id: Target agent id.
            request: Input payload for negotiation. For `file` / `image` inputs,
                pass a broker file id string or a local `Path` / bytes value.
                Local file values are uploaded automatically before negotiation.
            user_info_consent: Optional consent fields to include in the request.
            max_duration_minutes: Optional maximum relay interactive duration;
                sent as `_broker.max_duration_minutes`.

        Returns:
            Negotiation response payload including `negotiation_id`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerUploadError: If the broker rejects an input file upload.
            BrokerResponseError: If the response payload is malformed or indicates error.
            TypeError: If a local file input value is not bytes or a valid path-like.
            OSError: If a local file input path cannot be read.
        """
        request_payload = self._with_broker_request_metadata(
            self._with_user_info_consent(request, user_info_consent),
            max_duration_minutes=max_duration_minutes,
        )
        if self._request_contains_local_files(request_payload):
            begin = self.begin_negotiation(agent_id)
            prepared_request = self._prepare_request_uploads(
                request_payload,
                begin["content"]["input"],
                context=f"negotiate({agent_id})",
            )
            response = self.post(
                "negotiate",
                {
                    "negotiation_id": begin["negotiation_id"],
                    "request": prepared_request,
                },
            )
        else:
            response = self.post(
                "negotiate",
                {"agent_id": agent_id, "request": request_payload},
            )
        _require_key(response, "negotiation_id", "negotiate")
        response["job_id"] = _require_str(response, "negotiation_id", "negotiate")
        content = _require_mapping(response, "content", "negotiate")
        response["content"] = _normalize_negotiation_content_feedback(
            content, context="negotiate"
        )
        return cast(NegotiationResponse, response)

    def begin_negotiation(self, agent_id: str) -> NegotiationResponse:
        """Begin negotiation to fetch input schema before sending a request.

        If the response content includes `user_info_request`, pass that list
        as `user_info_consent` when calling negotiate/ask.

        Args:
            agent_id: Target agent id.

        Returns:
            Response payload including `negotiation_id` and `content`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self.post("negotiation/begin", {"agent_id": agent_id})
        _require_key(response, "negotiation_id", "begin_negotiation")
        response["job_id"] = _require_str(
            response, "negotiation_id", "begin_negotiation"
        )
        content = _require_mapping(response, "content", "begin_negotiation")
        response["content"] = _normalize_negotiation_content_feedback(
            content, context="begin_negotiation"
        )
        return cast(NegotiationResponse, response)

    def contract(self, negotiation_id: str) -> ContractResponse:
        """Request a contract for a negotiated job.

        Args:
            negotiation_id: Negotiation id returned by begin/negotiation.

        Returns:
            Contract response payload.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self.post("contract", {"negotiation_id": negotiation_id})
        _require_key(response, "status", "contract")
        return cast(ContractResponse, response)

    def terminate_job(
        self, job_id: str, reason: str = "client_requested"
    ) -> StatusResponse:
        """Request broker-side termination of a job using client auth.

        Relay jobs are keyed by `negotiation_id`; pass that value as `job_id`.
        """
        response = self.post(
            "contract/terminate",
            {"job_id": job_id, "reason": reason},
        )
        _require_key(response, "status", "terminate_job")
        return cast(StatusResponse, response)

    def get_result(self, negotiation_id: str) -> ResultResponse:
        """Poll the broker for results until done/error.

        Args:
            negotiation_id: Negotiation id to poll.

        Returns:
            Final result payload when status reaches done.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        msg = ""
        while True:
            response = self._request_json(
                "GET", f"result/{negotiation_id}", allow_status_error=True
            )
            status = _require_key(response, "status", "get_result")
            _require_key(response, "msg", "get_result")
            _require_key(response, "progress", "get_result")
            _require_key(response, "result", "get_result")
            response["job_id"] = _require_str(response, "negotiation_id", "get_result")
            if response["msg"] != msg:
                msg = response["msg"]
                logger.debug(msg)
            if status in ["done", "error"]:
                break
            time.sleep(1)
        if response["status"] == "error":
            raise BrokerResponseError(
                f"Error response from agent: {response['msg']}", payload=response
            )
        return cast(ResultResponse, response)

    def board(self) -> BoardResponse:
        """Return the board listing (same payload as broker board).

        Returns:
            Board payload with `agents`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self.get("board")
        agents = _require_list(response, "agents", "board")
        response["agents"] = [
            _normalize_board_agent_payload(agent, context=f"board.agents[{index}]")
            for index, agent in enumerate(agents)
        ]
        return cast(BoardResponse, response)

    @property
    def header_auth(self) -> dict[str, str]:
        return {"authorization": f"Basic {self.auth}"}

    @property
    def header(self) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            **self.header_auth,
        }
        return headers

    def _with_user_info_consent(
        self, request: JsonDict, consent: Iterable[UserInfoField] | None
    ) -> JsonDict:
        """Attach user info consent to the request payload."""
        if not consent:
            return request
        payload = dict(request)
        payload["_user_info_consent"] = [
            field.value for field in _normalize_user_info_fields(consent)
        ]
        return payload

    def _with_broker_request_metadata(
        self,
        request: JsonDict,
        *,
        max_duration_minutes: int | float | None = None,
    ) -> JsonDict:
        """Attach reserved `_broker` request metadata."""
        if max_duration_minutes is None:
            return request
        if not isinstance(max_duration_minutes, (int, float)) or isinstance(
            max_duration_minutes, bool
        ):
            raise TypeError("max_duration_minutes must be a number or None")
        if max_duration_minutes <= 0:
            raise ValueError("max_duration_minutes must be > 0")

        payload = dict(request)
        raw_metadata = payload.get("_broker")
        metadata = dict(raw_metadata) if isinstance(raw_metadata, Mapping) else {}
        metadata["max_duration_minutes"] = max_duration_minutes
        payload["_broker"] = metadata
        return payload

    def post(self, uri: str, payload: JsonDict) -> JsonDict:
        """POST to the client API and return JSON.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        return self._request_json("POST", uri, payload)

    def get(self, uri: str) -> JsonDict:
        """GET from the client API and return JSON.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        return self._request_json("GET", uri)

    def parse_relay_file(
        self, value: RelayFileHandle | Mapping[str, object]
    ) -> RelayFileHandle:
        """Parse a relay-file value returned by the client API."""
        if isinstance(value, RelayFileHandle):
            return value
        return _parse_relay_file_handle(value, context="relay_file")

    def parse_relay_media(
        self, value: RelayMediaHandle | Mapping[str, object]
    ) -> RelayMediaHandle:
        """Parse a relay-media value returned by the client API."""
        if isinstance(value, RelayMediaHandle):
            return value
        return _parse_relay_media_handle(value, context="relay_media")

    def parse_relay_session(
        self, value: RelaySessionHandle | Mapping[str, object]
    ) -> RelaySessionHandle:
        """Parse a relay-session value returned by the client API."""
        if isinstance(value, RelaySessionHandle):
            return value
        return _parse_relay_session_handle(value, context="relay_session")

    def _resolve_client_download_url(self, uri: str) -> str:
        if uri.startswith("http://") or uri.startswith("https://"):
            return uri
        broker_parts = urlsplit(self.broker_url)
        broker_origin = urlunsplit(
            (broker_parts.scheme, broker_parts.netloc, "", "", "")
        )
        if uri.startswith("/"):
            return f"{broker_origin}{uri}"
        return f"{self.broker_url.rstrip('/')}/api/v1/client/{uri.lstrip('/')}"

    def _resolve_client_websocket_url(self, uri: str) -> str:
        if uri.startswith("ws://") or uri.startswith("wss://"):
            return uri
        if uri.startswith("http://") or uri.startswith("https://"):
            parsed = urlsplit(uri)
        else:
            parsed = urlsplit(self._resolve_client_download_url(uri))
        ws_scheme = "wss" if parsed.scheme == "https" else "ws"
        return urlunsplit((ws_scheme, parsed.netloc, parsed.path, parsed.query, ""))

    def _open_binary_uri(
        self,
        uri: str,
        *,
        stream: bool,
        byte_range: tuple[int, int | None] | None = None,
    ) -> requests.Response:
        started_at = time.perf_counter()
        attempt = 0
        url = self._resolve_client_download_url(uri)
        headers = dict(self.header_auth)
        if byte_range is not None:
            start, end = byte_range
            range_end = "" if end is None else str(end)
            headers["Range"] = f"bytes={start}-{range_end}"
        while True:
            timeout = _request_timeout_tuple(
                started_at,
                Agent.REQUEST_RETRY_DEADLINE,
                connect_timeout=Agent.REQUEST_CONNECT_TIMEOUT,
                read_timeout_cap=Agent.UPLOAD_READ_TIMEOUT,
            )
            try:
                response = self._http_session().get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    stream=stream,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Connection error on get request; %s (attempt %s, deadline %.1fs)",
                    uri,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                logger.debug("Broker binary request connection error details: %s", exc)
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                ):
                    attempt += 1
                    continue
                raise BrokerConnectionError(
                    f"Connection error on get request; {uri=}"
                ) from exc

            if _retryable_http_status(response.status_code):
                logger.warning(
                    "Transient HTTP status on get request; %s status=%s "
                    "(attempt %s, deadline %.1fs)",
                    uri,
                    response.status_code,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                    status_code=response.status_code,
                    retry_after=_retry_after_seconds(response.headers),
                ):
                    attempt += 1
                    continue

            if response.status_code not in (200, 206):
                logger.exception(
                    "Unexpected status on binary get: response.status_code=%s; uri=%s; response.content=%r",
                    response.status_code,
                    uri,
                    response.content,
                )
                raise BrokerHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )
            return response

    def get_file(self, uri: str) -> requests.Response:
        """Open a binary file response from the client API.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
        """
        return self._open_binary_uri(uri, stream=False)

    def open_file(
        self,
        relay: RelayFileHandle | Mapping[str, object],
        *,
        stream: bool = True,
        byte_range: tuple[int, int | None] | None = None,
    ) -> requests.Response:
        """Open a relay-file download response."""
        handle = self.parse_relay_file(relay)
        return self._open_binary_uri(handle.uri, stream=stream, byte_range=byte_range)

    def open_media(
        self,
        relay: RelayMediaHandle | Mapping[str, object],
        *,
        purpose: Literal["playback", "download"] = "playback",
        profile: str | None = None,
        stream: bool = True,
        byte_range: tuple[int, int | None] | None = None,
    ) -> requests.Response:
        """Open a relay-media response for playback or download.

        `profile` selects one named playback manifest exposed by
        `RelayMediaHandle.profiles`. It is available only for playback because
        download uses the media handle's dedicated `download_uri`.
        """
        handle = self.parse_relay_media(relay)
        uri = handle.playback_uri
        if purpose == "download":
            if profile is not None:
                raise BrokerResponseError(
                    "profile selection is only available for playback",
                    payload=relay,
                )
            if handle.download_uri is None:
                raise BrokerResponseError(
                    "relay_media is missing download_uri",
                    payload=relay,
                )
            uri = handle.download_uri
        elif profile is not None:
            selected_profile = handle.get_profile(profile)
            if selected_profile is None:
                raise BrokerResponseError(
                    f"relay_media is missing playback profile {profile!r}",
                    payload=relay,
                )
            uri = selected_profile.playback_uri
        return self._open_binary_uri(uri, stream=stream, byte_range=byte_range)

    def download_file(
        self,
        relay: RelayFileHandle | Mapping[str, object],
        destination: PathLike[str] | str,
    ) -> Path:
        """Download a relay-file handle to a local path."""
        target = Path(destination).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        response = self.open_file(relay, stream=True)
        try:
            with target.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        handle.write(chunk)
        finally:
            response.close()
        return target

    def download_media(
        self,
        relay: RelayMediaHandle | Mapping[str, object],
        destination: PathLike[str] | str,
        *,
        purpose: Literal["playback", "download"] = "download",
    ) -> Path:
        """Download a relay-media handle to a local path."""
        target = Path(destination).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        response = self.open_media(relay, purpose=purpose, stream=True)
        try:
            with target.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=64 * 1024):
                    if chunk:
                        handle.write(chunk)
        finally:
            response.close()
        return target

    def open_session(
        self,
        relay: RelaySessionHandle | Mapping[str, object],
        *,
        timeout: float = Agent.REQUEST_READ_TIMEOUT,
    ) -> RelaySessionConnection:
        """Open a relay-session websocket and wait until it becomes ready."""
        handle = self.parse_relay_session(relay)
        url = self._resolve_client_websocket_url(handle.session_uri)
        try:
            ws = websocket.create_connection(
                url,
                header=[f"authorization: Basic {self.auth}"],
                timeout=timeout,
            )
        except Exception as exc:
            raise BrokerConnectionError(
                f"Connection error while opening relay_session; {url=}"
            ) from exc

        connection = RelaySessionConnection(ws)
        first_event = connection.recv_event(timeout=timeout)
        if first_event.type == "ready":
            return connection
        connection.close()
        if first_event.type == "error":
            raise BrokerResponseError(
                f"Failed to open relay_session [{first_event.code or 'relay_error'}]: {first_event.message or 'Unknown error.'}",
                payload=relay,
            )
        raise BrokerResponseError(
            f"Relay session did not become ready; first event was {first_event.type!r}",
            payload=relay,
        )

    def upload(
        self,
        file_type: str,
        value: UploadValue,
    ) -> str:
        """Upload a client-side input file and return the broker file id.

        Args:
            file_type: File extension/type declared by the agent input schema.
            value: Bytes-like content or a local file path.

        Returns:
            Broker file id string suitable for `file` / `image` request fields.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerUploadError: If the broker rejects the upload payload.
            BrokerResponseError: If the broker returns malformed JSON.
            TypeError: If `value` is neither bytes-like nor a path-like object.
            OSError: If the local file path cannot be read.
        """

        file_id, _format = File(file_type).format_for_output(
            value, self._upload_binary_data
        )
        if not isinstance(file_id, str):
            raise BrokerResponseError(
                f"Malformed broker response in upload: missing file id for {file_type!r}",
                payload=file_id,
            )
        return file_id

    def upload_parallel(
        self,
        files: Mapping[str, tuple[str, UploadValue]],
        *,
        max_upload_workers: int | None = None,
    ) -> dict[str, str]:
        """Upload multiple client-side input files concurrently."""

        def upload_one(_key: str, item: tuple[str, UploadValue]) -> str:
            file_type, value = item
            return self.upload(file_type, value)

        return cast(
            dict[str, str],
            _upload_parallel_map(
                files,
                max_upload_workers=max_upload_workers,
                upload_one=upload_one,
            ),
        )

    def _request_json(
        self,
        method: str,
        uri: str,
        payload: JsonDict | None = None,
        *,
        allow_status_error: bool = False,
    ) -> JsonDict:
        url = f"{self.broker_url}/api/v1/client/{uri}"
        started_at = time.perf_counter()
        attempt = 0
        while True:
            timeout = _request_timeout_tuple(
                started_at,
                Agent.REQUEST_RETRY_DEADLINE,
                connect_timeout=Agent.REQUEST_CONNECT_TIMEOUT,
                read_timeout_cap=Agent.REQUEST_READ_TIMEOUT,
            )
            try:
                response = self._http_session().request(
                    method,
                    url,
                    json=payload,
                    headers=self.header,
                    timeout=timeout,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Connection error on %s request; %s (attempt %s, deadline %.1fs)",
                    method,
                    uri,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                logger.debug("Broker request connection error details: %s", exc)
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                ):
                    attempt += 1
                    continue
                raise BrokerConnectionError(
                    f"Connection error on {method} request; {uri=}"
                ) from exc

            if _retryable_http_status(response.status_code):
                logger.warning(
                    "Transient HTTP status on %s request; %s status=%s "
                    "(attempt %s, deadline %.1fs)",
                    method,
                    uri,
                    response.status_code,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                logger.debug(
                    "Retryable broker response content on %s %s: %r",
                    method,
                    uri,
                    response.content,
                )
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                    status_code=response.status_code,
                    retry_after=_retry_after_seconds(response.headers),
                ):
                    attempt += 1
                    continue

            if response.status_code != 200:
                logger.exception(
                    f"Not 200: {response.status_code=}; {uri=}, {response.content=}"
                )
                raise BrokerHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )
            break

        try:
            obj = response.json()
        except ValueError as exc:
            raise BrokerResponseError(
                f"{method} {uri} returned non-JSON response"
            ) from exc
        obj = _ensure_response_dict(
            obj, f"{method} {uri}", allow_status_error=allow_status_error
        )
        assert all(
            isinstance(k, str) for k in obj.keys()
        ), f"Some of response keys were not str: {obj.keys()}"
        return obj

    def _request_contains_local_files(self, request: Mapping[str, object]) -> bool:
        return any(
            key not in ("_user_info_consent", "_broker") and _is_local_file_value(value)
            for key, value in request.items()
        )

    def _prepare_request_uploads(
        self,
        request: Mapping[str, object],
        input_schema: TemplateSchema,
        *,
        context: str,
    ) -> JsonDict:
        types = input_schema.get("@type")
        constraints = input_schema.get("@constraints")
        if not isinstance(types, Mapping):
            _raise_malformed_response(context, "missing input.@type", input_schema)
        if not isinstance(constraints, Mapping):
            _raise_malformed_response(
                context, "missing input.@constraints", input_schema
            )

        prepared = dict(request)
        for key, value in request.items():
            if key in ("_user_info_consent", "_broker") or not _is_local_file_value(
                value
            ):
                continue

            declared_type = types.get(key)
            if declared_type not in ("file", "image"):
                raise BrokerResponseError(
                    f"{context} failed: local file data was provided for '{key}', "
                    f"but the input schema declares {declared_type!r}",
                    payload=input_schema,
                )

            constraint = constraints.get(key)
            if not isinstance(constraint, Mapping):
                _raise_malformed_response(
                    context, f"missing constraints for file input '{key}'", input_schema
                )
            file_type = constraint.get("file_type")
            if not isinstance(file_type, str) or file_type == "":
                _raise_malformed_response(
                    context, f"missing file_type for file input '{key}'", input_schema
                )

            prepared[key] = self.upload(
                file_type,
                cast(BinaryData | PathLike[str] | str, value),
            )

        return prepared

    def _upload_binary_data(self, file_type: str, binary_data: bytes) -> JsonDict:
        extension = file_type.strip().lower().lstrip(".")
        file_name = f"__file_name__.{extension}" if extension else "__file_name__"
        content_type, _encoding = mimetypes.guess_type(file_name)
        if content_type is None:
            content_type = "application/octet-stream"

        files = {"file": (file_name, binary_data, content_type)}
        uri = "upload"

        started_at = time.perf_counter()
        attempt = 0
        while True:
            timeout = _request_timeout_tuple(
                started_at,
                Agent.REQUEST_RETRY_DEADLINE,
                connect_timeout=Agent.REQUEST_CONNECT_TIMEOUT,
                read_timeout_cap=Agent.UPLOAD_READ_TIMEOUT,
            )
            try:
                response = self._http_session().post(
                    f"{self.broker_url}/api/v1/client/{uri}",
                    headers=self.header_auth,
                    files=files,
                    timeout=timeout,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Connection error on upload request; %s (attempt %s, deadline %.1fs)",
                    uri,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                logger.debug("Broker upload connection error details: %s", exc)
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                ):
                    attempt += 1
                    continue
                raise BrokerConnectionError(
                    f"Connection error on upload request; {uri=}"
                ) from exc

            if _retryable_http_status(response.status_code):
                logger.warning(
                    "Transient HTTP status on upload request; %s status=%s "
                    "(attempt %s, deadline %.1fs)",
                    uri,
                    response.status_code,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                logger.debug(
                    "Retryable broker upload response content: %r", response.content
                )
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                    status_code=response.status_code,
                    retry_after=_retry_after_seconds(response.headers),
                ):
                    attempt += 1
                    continue

            if response.status_code != 200:
                logger.exception(
                    "Not 200: response.status_code=%s; uri=%s, response.content=%r",
                    response.status_code,
                    uri,
                    response.content,
                )
                raise BrokerHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )
            break

        try:
            payload = response.json()
        except ValueError as exc:
            raise BrokerResponseError(
                "Malformed broker response in upload: expected JSON object",
                payload=response.text,
            ) from exc

        if not isinstance(payload, dict):
            raise BrokerResponseError(
                "Malformed broker response in upload: expected JSON object",
                payload=payload,
            )

        if payload.get("status") == "error":
            message = (
                payload.get("error_msg") or payload.get("error") or "unknown error"
            )
            raise BrokerUploadError(
                str(message),
                code=(
                    payload.get("error")
                    if isinstance(payload.get("error"), str)
                    else None
                ),
                payload=payload,
            )

        file_id = _require_str(payload, "file_id", "upload")
        return {"file_id": file_id}


class BrokerAdmin:
    """Client for `/api/v1/broker/*`.

    These endpoints accept user tokens only. `agent_auth` strings and
    ephemeral tokens are rejected.
    """

    def __init__(self, broker_url: str, token: str) -> None:
        """Create a client for `/api/v1/broker/*`.

        Args:
            broker_url: Base URL used for `/api/v1/broker/*`.
            token: Bare user token string. Do not include `"Token "`.
        """
        self.broker_url = broker_url
        self.token = token
        self._http_session_local = threading.local()

    def _http_session(self) -> requests.Session:
        return _get_thread_session(self._http_session_local)

    @property
    def header(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "authorization": f"Token {self.token}",
        }

    @property
    def header_auth(self) -> dict[str, str]:
        return {"authorization": f"Token {self.token}"}

    def _request_json(
        self, method: str, uri: str, payload: JsonDict | None = None
    ) -> JsonDict:
        started_at = time.perf_counter()
        attempt = 0
        while True:
            timeout = _request_timeout_tuple(
                started_at,
                Agent.REQUEST_RETRY_DEADLINE,
                connect_timeout=Agent.REQUEST_CONNECT_TIMEOUT,
                read_timeout_cap=Agent.REQUEST_READ_TIMEOUT,
            )
            try:
                response = self._http_session().request(
                    method,
                    f"{self.broker_url}/api/v1/broker/{uri}",
                    json=payload,
                    headers=self.header,
                    timeout=timeout,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Connection error on broker %s request; %s "
                    "(attempt %s, deadline %.1fs)",
                    method,
                    uri,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                logger.debug("BrokerAdmin request connection error details: %s", exc)
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                ):
                    attempt += 1
                    continue
                raise BrokerConnectionError(
                    f"Connection error on {method} request; {uri=}"
                ) from exc

            if _retryable_http_status(response.status_code):
                logger.warning(
                    "Transient HTTP status on broker %s request; %s status=%s "
                    "(attempt %s, deadline %.1fs)",
                    method,
                    uri,
                    response.status_code,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                    status_code=response.status_code,
                    retry_after=_retry_after_seconds(response.headers),
                ):
                    attempt += 1
                    continue

            if response.status_code != 200:
                logger.exception(
                    f"Not 200: {response.status_code=}; {uri=}, {response.content=}"
                )
                raise BrokerHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )
            break

        try:
            obj = response.json()
        except ValueError as exc:
            raise BrokerResponseError(
                f"{method} {uri} returned non-JSON response"
            ) from exc
        obj = _ensure_response_dict(obj, f"{method} {uri}")
        assert all(
            isinstance(k, str) for k in obj.keys()
        ), f"Some of response keys were not str: {obj.keys()}"
        return obj

    def _request_raw(self, uri: str) -> requests.Response:
        started_at = time.perf_counter()
        attempt = 0
        while True:
            timeout = _request_timeout_tuple(
                started_at,
                Agent.REQUEST_RETRY_DEADLINE,
                connect_timeout=Agent.REQUEST_CONNECT_TIMEOUT,
                read_timeout_cap=Agent.UPLOAD_READ_TIMEOUT,
            )
            try:
                response = self._http_session().get(
                    f"{self.broker_url}/api/v1/broker/{uri}",
                    headers=self.header_auth,
                    timeout=timeout,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "Connection error on broker get request; %s "
                    "(attempt %s, deadline %.1fs)",
                    uri,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                logger.debug(
                    "BrokerAdmin raw request connection error details: %s", exc
                )
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                ):
                    attempt += 1
                    continue
                raise BrokerConnectionError(
                    f"Connection error on get request; {uri=}"
                ) from exc

            if _retryable_http_status(response.status_code):
                logger.warning(
                    "Transient HTTP status on broker get request; %s status=%s "
                    "(attempt %s, deadline %.1fs)",
                    uri,
                    response.status_code,
                    attempt + 1,
                    Agent.REQUEST_RETRY_DEADLINE,
                )
                if _sleep_for_retry(
                    started_at,
                    Agent.REQUEST_RETRY_DEADLINE,
                    attempt,
                    Agent.REQUEST_RETRY_BASE,
                    Agent.REQUEST_RETRY_MAX,
                    status_code=response.status_code,
                    retry_after=_retry_after_seconds(response.headers),
                ):
                    attempt += 1
                    continue

            if response.status_code != 200:
                logger.exception(
                    f"Not 200: {response.status_code=}; {uri=}, {response.content=}"
                )
                raise BrokerHTTPError(
                    response.status_code, uri, _coerce_error_content(response.content)
                )

            return response

    # Board + results
    def board(self) -> BoardResponse:
        """Return the board listing (agents + active flag).

        Requires a user token and returns the same payload as /api/v1/client/board.

        Returns:
            Board payload with `agents`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", "board")
        agents = _require_list(response, "agents", "board")
        response["agents"] = [
            _normalize_board_agent_payload(agent, context=f"board.agents[{index}]")
            for index, agent in enumerate(agents)
        ]
        return cast(BoardResponse, response)

    def list_results(self) -> ResultsResponse:
        """Return the result list visible to the current user.

        Non-admin users receive only their own contracts. Admin users receive
        all visible contracts; each summary includes `client` metadata and a
        `mine` boolean so callers can filter client-side if needed.

        Returns:
            Results payload with `contracts`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", "results")
        _require_list(response, "contracts", "list_results")
        return cast(ResultsResponse, response)

    def terminate_job(
        self, job_id: str, reason: str = "client_requested"
    ) -> StatusResponse:
        """Request broker-side termination of a job using a user token.

        Relay jobs are keyed by `negotiation_id`; pass that value as `job_id`.
        """
        response = self._request_json(
            "POST",
            f"jobs/{job_id}/terminate",
            {"reason": reason},
        )
        _require_key(response, "status", "terminate_job")
        return cast(StatusResponse, response)

    # Agents
    def list_agents(self) -> AgentsResponse:
        """Return agents visible to the current user.

        Returns:
            Payload with `agents`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", "agents")
        agents = _require_list(response, "agents", "list_agents")
        response["agents"] = [
            _normalize_agent_summary_payload(
                agent, context=f"list_agents.agents[{index}]"
            )
            for index, agent in enumerate(agents)
        ]
        return cast(AgentsResponse, response)

    def get_agent(self, agent_id: str) -> AgentResponse:
        """Return details for a specific agent.

        Args:
            agent_id: Target agent id.

        Returns:
            Payload with `agent`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", f"agents/{agent_id}")
        response["agent"] = _normalize_agent_detail_payload(
            _require_mapping(response, "agent", "get_agent"),
            context="get_agent.agent",
        )
        return cast(AgentResponse, response)

    def create_agent(
        self,
        name: str,
        agent_type: str,
        category: str,
        is_public: bool = True,
    ) -> AgentResponse:
        """Create a new agent owned by the current user (defaults to public).

        Args:
            name: Agent display name.
            agent_type: Agent type string.
            category: Agent category.
            is_public: Whether to publish the agent.

        Returns:
            Payload with `agent`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        payload: JsonDict = {
            "servicer_agent": {
                "name": name,
                "type": agent_type,
                "category": category,
                "is_public": is_public,
            }
        }
        response = self._request_json("POST", "agents", payload)
        response["agent"] = _normalize_agent_detail_payload(
            _require_mapping(response, "agent", "create_agent"),
            context="create_agent.agent",
        )
        return cast(AgentResponse, response)

    def update_agent(self, agent_id: str, **fields: Any) -> AgentResponse:
        """Update an existing agent (name/type/category/is_public).

        Args:
            agent_id: Target agent id.
            **fields: Fields to update.

        Returns:
            Payload with `agent`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json(
            "PATCH", f"agents/{agent_id}", {"servicer_agent": fields}
        )
        response["agent"] = _normalize_agent_detail_payload(
            _require_mapping(response, "agent", "update_agent"),
            context="update_agent.agent",
        )
        return cast(AgentResponse, response)

    def delete_agent(self, agent_id: str) -> StatusResponse:
        """Delete an agent owned by the current user.

        Args:
            agent_id: Target agent id.

        Returns:
            Payload with `status`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("DELETE", f"agents/{agent_id}")
        _require_key(response, "status", "delete_agent")
        return cast(StatusResponse, response)

    def download_agent_template(self, agent_id: str, simple: bool = False) -> str:
        """Download the agent Python template as text.

        Args:
            agent_id: Target agent id.
            simple: Whether to fetch the simplified template.

        Returns:
            Python source code template.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
        """
        path = (
            f"agents/{agent_id}/template_simple"
            if simple
            else f"agents/{agent_id}/template"
        )
        response = self._request_raw(path)
        return response.text

    # Access tokens
    def list_access_tokens(self) -> TokensResponse:
        """List broker access tokens for the current user.

        Returns:
            Payload with `tokens`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", "access_tokens")
        _require_list(response, "tokens", "list_access_tokens")
        return cast(TokensResponse, response)

    def issue_access_token(self, label: str) -> AccessTokenResponse:
        """Issue a new access token with a label.

        Args:
            label: Token label.

        Returns:
            Payload with `token`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("POST", "access_tokens", {"label": label})
        _require_key(response, "token", "issue_access_token")
        return cast(AccessTokenResponse, response)

    def revoke_access_token(self, token: str) -> StatusResponse:
        """Revoke an existing access token.

        Args:
            token: Token string.

        Returns:
            Payload with `status`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("DELETE", f"access_tokens/{token}")
        _require_key(response, "status", "revoke_access_token")
        return cast(StatusResponse, response)

    # User profile
    def get_user(self) -> UserResponse:
        """Return the current user's profile.

        Returns:
            Payload with `user`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", "user")
        _require_mapping(response, "user", "get_user")
        return cast(UserResponseWithStatus, response)

    def create_user(
        self, name: str, affiliation: str | None = None
    ) -> UserResponseWithStatus:
        """Create a user record for the current auth id if missing.

        Args:
            name: User display name.
            affiliation: Optional affiliation.

        Returns:
            Payload with `user`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        payload = {"user": {"name": name, "affiliation": affiliation}}
        response = self._request_json("POST", "user", payload)
        _require_mapping(response, "user", "create_user")
        return cast(UserResponseWithStatus, response)

    def update_user(
        self, name: str | None = None, affiliation: str | None = None
    ) -> UserResponse:
        """Update the current user's profile fields.

        Args:
            name: Updated name (optional).
            affiliation: Updated affiliation (optional).

        Returns:
            Payload with `user`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        payload: JsonDict = {"user": {}}
        if name is not None:
            payload["user"]["name"] = name
        if affiliation is not None:
            payload["user"]["affiliation"] = affiliation
        response = self._request_json("PATCH", "user", payload)
        _require_mapping(response, "user", "update_user")
        return cast(UserResponse, response)

    # Admin/self endpoints
    def list_users(self) -> UsersResponse:
        """List users (admin sees all; non-admin sees self).

        Returns:
            Payload with `users`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", "users")
        _require_list(response, "users", "list_users")
        return cast(UsersResponse, response)

    def get_user_by_id(self, user_id: str) -> UserResponse:
        """Fetch a specific user by id.

        Args:
            user_id: Target user id.

        Returns:
            Payload with `user`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("GET", f"users/{user_id}")
        _require_mapping(response, "user", "get_user_by_id")
        return cast(UserResponse, response)

    def update_user_by_id(self, user_id: str, **fields: Any) -> UserResponse:
        """Update a user by id (self-only unless admin rules change).

        Args:
            user_id: Target user id.
            **fields: Fields to update.

        Returns:
            Payload with `user`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("PATCH", f"users/{user_id}", {"user": fields})
        _require_mapping(response, "user", "update_user_by_id")
        return cast(UserResponse, response)

    def delete_user(self, user_id: str) -> StatusResponse:
        """Delete a user by id (self-only).

        Args:
            user_id: Target user id.

        Returns:
            Payload with `status`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("DELETE", f"users/{user_id}")
        _require_key(response, "status", "delete_user")
        return cast(StatusResponse, response)

    def deposit_user(self, user_id: str) -> UserResponse:
        """Deposit points to a user (self-only in current policy).

        Args:
            user_id: Target user id.

        Returns:
            Payload with `user`.

        Raises:
            BrokerConnectionError: If the broker URL cannot be reached.
            BrokerHTTPError: If the server returns a non-200 response.
            BrokerResponseError: If the response payload is malformed or indicates error.
        """
        response = self._request_json("POST", f"users/{user_id}/deposit")
        _require_mapping(response, "user", "deposit_user")
        return cast(UserResponse, response)


class AgentManager:
    """Watches a directory and auto-runs agent modules."""

    WATCHING_INTERVAL = 5

    def __init__(self, dir_path: PathLike[str] | str = Path(".")) -> None:
        self.dir_path: Path = Path(dir_path)
        self.agents: dict[Path, dict[str, Any]] = {}
        self.start_watching_loop()

    def start_watching_loop(self) -> None:
        """Start the file watch loop and manage agent lifecycles."""
        while True:
            try:
                self.dog_watching()
                time.sleep(self.WATCHING_INTERVAL)
            except KeyboardInterrupt:
                logger.info("Automatic agent runner closing...")
                break
        for k in self.agents.keys():
            self.stop(k)

    def dog_watching(self) -> None:
        """Scan directory changes and start/stop agents accordingly."""
        exisiting_files = set(self.dir_path.glob("*.py"))
        current_files = set(self.agents.keys())
        new_files = exisiting_files - current_files
        removed_files = current_files - exisiting_files
        ongoing_files = current_files & exisiting_files
        for file in new_files:
            self.load(file)
        for file in ongoing_files:
            self.check_for_update(file)
        for file in removed_files:
            self.stop(file)
            self.agents.pop(file)

    def run_module(self, module: ModuleType) -> Agent | list[Agent] | None:
        """Run an Agent instance or automatic Agent class from a module."""

        def is_agent(obj):
            return hasattr(obj, "_to_show_i_am_agent_instance")

        def is_automatic_agent(obj):
            return (
                hasattr(obj, "_is_automatic_mode")
                and getattr(obj, "_is_automatic_mode")()
            )

        for key in module.__dir__():
            if is_agent(getattr(module, key)):
                agent = getattr(module, key)
                assert isinstance(
                    agent, Agent
                ), f"Not an Agent instance; {module=}; {key=}; {agent=}"
                agent.run(_automatic=True)
                return agent
            elif is_automatic_agent(getattr(module, key)):
                agent_class = getattr(module, key)
                return getattr(agent_class, "_automatic_run")()
        return None

    def load(self, file: Path) -> None:
        """Load an agent module and start it if possible."""
        self.agents[file] = {"mtime": file.stat().st_mtime}
        try:
            module_name = file.as_posix().replace(".py", "").replace("/", ".")
            module = importlib.import_module(module_name)
            self.agents[file]["module"] = module
            agent = self.run_module(module)
            if agent is not None:
                self.agents[file]["agent"] = agent
                if isinstance(agent, list):
                    logger.info(
                        f"Agent {[x.interface.name for x in agent]} has started!"
                    )
                else:
                    logger.info(f"Agent {agent.interface.name} has started!")
        except:
            logger.exception(f"load failed: {file=}")

    def check_for_update(self, file: Path) -> None:
        """Reload and restart an agent when its file changes."""
        try:
            mtime = file.stat().st_mtime
            if mtime == self.agents[file]["mtime"]:
                return
            self.agents[file]["mtime"] = mtime
            if "module" not in self.agents[file]:
                self.load(file)
                return
            if "agent" in self.agents[file]:
                self.stop(file)
                time.sleep(Agent.HEARTBEAT_INTERVAL)

            module = importlib.reload(self.agents[file]["module"])
            self.agents[file]["module"] = module
            agent = self.run_module(module)
            if agent is not None:
                self.agents[file]["agent"] = agent
                if isinstance(agent, list):
                    logger.info(
                        f"Agent {[x.interface.name for x in agent]} has started!"
                    )
                else:
                    logger.info(f"Agent {agent.interface.name} has started!")
        except:
            logger.exception(f"check_for_update failed: {file=}")

    def stop(self, file: Path) -> None:
        """Stop a running agent for the given file."""
        try:
            if "agent" in self.agents[file]:
                if isinstance(self.agents[file]["agent"], list):
                    for agent in self.agents[file]["agent"]:
                        agent.goodbye()
                else:
                    self.agents[file]["agent"].goodbye()
        except:
            logger.exception(f"stop failed: {file=}")


def batch_run() -> None:
    """CLI entrypoint for auto-running agents in a directory."""
    AgentManager()


def main() -> None:
    """Console entrypoint for the brokersystem package."""
    from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

    parser = ArgumentParser(
        prog="brokersystem",
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("command", choices=["batch_run"], help="Command to run")
    parser.add_argument(
        "-v", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    args = parser.parse_args()
    if args.command == "batch_run":
        batch_run()


if __name__ == "__main__":
    main()
