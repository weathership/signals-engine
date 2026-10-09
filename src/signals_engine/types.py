"""Host-facing types. Wire messages stay inside the servicers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import grpc


class LatticeBindError(RuntimeError):
    """The listen port is already held. gRPC reuseport is off, so this is fatal."""


class HandlerError(Exception):
    """A host handler refuses the call with a gRPC status."""

    def __init__(self, code: grpc.StatusCode, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


@dataclass(frozen=True)
class PeerIdentity:
    """Who this process is on the lattice."""

    project: str
    port: int = 0
    bind_host: str = "0.0.0.0"
    # Explicit advertise host. Empty asks the resolver, which never returns loopback.
    # An explicit loopback value is kept so a single-host test can dial itself.
    advertise_host: str = ""
    primary_ui: str = ""
    health_url: str = ""
    repo_root: Path | None = None
    version: str = "0.1.0"


@dataclass(frozen=True)
class EndpointSpec:
    capability: str
    model: str = ""
    healthy: bool = False
    gpu_ids: tuple[int, ...] = ()
    detail: str = ""


@dataclass(frozen=True)
class SurfaceSpec:
    kind: str
    url: str
    healthy: bool = True


@dataclass(frozen=True)
class ToolCall:
    name: str
    arguments_json: str = ""
    id: str = ""


@dataclass(frozen=True)
class CompletionRequest:
    """One capability completion. Both Engine/Complete and OIP ModelInfer build this."""

    capability: str
    prompt: str
    system_prompt: str = ""
    max_tokens: int = 2048
    temperature: float = 0.7
    json_schema: str = ""
    tools_json: str = ""
    messages_json: str = ""
    tool_choice: str = ""
    capabilities: tuple[str, ...] = ()
    timezone: str = ""
    clock_json: str = ""


@dataclass(frozen=True)
class Completion:
    text: str
    model: str = ""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    latency_ms: float = 0.0
    reasoning_content: str = ""
    finish_reason: str = "stop"
    tool_calls: tuple[ToolCall, ...] = ()
    fulfilled_by: str = ""


@dataclass(frozen=True)
class YieldOutcome:
    ok: bool = True
    process_ended: bool = False
    restore_started: bool = False
    message: str = ""


@dataclass(frozen=True)
class MigrationSpec:
    source: str
    current: str = ""
    unapplied: tuple[str, ...] = ()


@dataclass(frozen=True)
class NativeService:
    """A project servicer on the same port as the lattice face."""

    add_to_server: Callable[[object, grpc.Server], None]
    servicer: object
    full_name: str


CompleteHandler = Callable[[CompletionRequest], Completion]
YieldHandler = Callable[[str, int, str, str], YieldOutcome]
QueryHandler = Callable[[object, object], None]
RpcHandler = Callable[..., object]
EndpointProbe = Callable[[], list[EndpointSpec]]
SurfaceProbe = Callable[[], list[SurfaceSpec]]
MigrationProbe = Callable[[], list[MigrationSpec]]
ModelReadyProbe = Callable[[str], bool]


@dataclass
class EngineHooks:
    endpoints: EndpointProbe | None = None
    surfaces: SurfaceProbe | None = None
    complete: CompleteHandler | None = None
    yield_workload: YieldHandler | None = None
    # ServerQuery kinds this host fills. Key is ServerQueryKind int.
    query: dict[int, QueryHandler] = field(default_factory=dict)
    # Product RPCs the library does not model: "Remediate", "PutAgendaItem", "WatchWorkload".
    rpc: dict[str, RpcHandler] = field(default_factory=dict)
    migrations: MigrationProbe | None = None
    model_ready: ModelReadyProbe | None = None
    native: tuple[NativeService, ...] = ()
