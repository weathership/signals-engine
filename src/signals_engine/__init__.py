"""Shared lattice engine for signals peers.

A host supplies its project, probes, and — when it serves a model — one
completion handler. This package registers ``zndx.engine.v1.Engine`` and
KServe OIP on the same port, with reflection and an exclusive bind.

The protocol version a peer imports is this package: ``PROTOCOL_SHA``,
``proto_root``, and ``signals_engine.wire``.
"""

from signals_engine.client import announce, complete, model_infer, model_stream, server_query, status
from signals_engine.protocol import PROTOCOL_SHA, proto_root
from signals_engine.server import LatticeEngine
from signals_engine.types import (
    Completion,
    CompletionRequest,
    EndpointSpec,
    EngineHooks,
    HandlerError,
    LatticeBindError,
    MigrationSpec,
    NativeService,
    PeerIdentity,
    SurfaceSpec,
    ToolCall,
    YieldOutcome,
)

__all__ = [
    "Completion",
    "CompletionRequest",
    "EndpointSpec",
    "EngineHooks",
    "HandlerError",
    "LatticeBindError",
    "LatticeEngine",
    "MigrationSpec",
    "NativeService",
    "PROTOCOL_SHA",
    "PeerIdentity",
    "SurfaceSpec",
    "ToolCall",
    "YieldOutcome",
    "announce",
    "complete",
    "model_infer",
    "model_stream",
    "proto_root",
    "server_query",
    "status",
]

__version__ = "0.2.0"
