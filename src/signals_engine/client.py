"""Generated-stub client for a lattice engine.

First-party callers use these stubs. Reflection is for bare grpcurl.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import grpc

from signals_engine import oip
from signals_engine.types import Completion, CompletionRequest, ToolCall
from signals_engine.wire.inference import open_inference_grpc_pb2 as oip_pb
from signals_engine.wire.inference import open_inference_grpc_pb2_grpc as oip_grpc
from signals_engine.wire.zndx.engine.v1 import engine_pb2
from signals_engine.wire.zndx.engine.v1 import engine_pb2_grpc

log = logging.getLogger("signals_engine.client")


_MAX_MESSAGE = 64 * 1024 * 1024


def channel(target: str) -> grpc.Channel:
    return grpc.insecure_channel(
        target.replace("grpc://", "").strip(),
        options=(
            ("grpc.max_send_message_length", _MAX_MESSAGE),
            ("grpc.max_receive_message_length", _MAX_MESSAGE),
        ),
    )


def status(target: str, timeout: float = 4.0) -> engine_pb2.StatusResponse:
    with channel(target) as ch:
        return engine_pb2_grpc.EngineStub(ch).Status(engine_pb2.StatusRequest(), timeout=timeout)


def server_query(
    target: str,
    kind: int,
    *,
    note_id: str = "",
    query: str = "",
    timeout: float = 4.0,
) -> engine_pb2.ServerQueryResponse:
    request = engine_pb2.ServerQueryRequest(kind=kind, note_id=note_id, query=query)
    with channel(target) as ch:
        return engine_pb2_grpc.EngineStub(ch).ServerQuery(request, timeout=timeout)


def complete(target: str, request: CompletionRequest, timeout: float = 30.0) -> Completion:
    message = engine_pb2.CompleteRequest(
        capability=request.capability,
        prompt=request.prompt,
        system_prompt=request.system_prompt,
        max_tokens=request.max_tokens,
        temperature=request.temperature,
        json_schema=request.json_schema,
        tools_json=request.tools_json,
        messages_json=request.messages_json,
        tool_choice=request.tool_choice,
        capabilities=list(request.capabilities),
        timezone=request.timezone,
        clock_json=request.clock_json,
    )
    with channel(target) as ch:
        response = engine_pb2_grpc.EngineStub(ch).Complete(message, timeout=timeout)
    return Completion(
        text=response.text,
        model=response.model,
        prompt_tokens=response.prompt_tokens,
        completion_tokens=response.completion_tokens,
        latency_ms=response.latency_ms,
        reasoning_content=response.reasoning_content,
        finish_reason=response.finish_reason or "stop",
        tool_calls=tuple(
            ToolCall(name=call.name, arguments_json=call.arguments_json, id=call.id)
            for call in response.tool_calls
        ),
        fulfilled_by=response.fulfilled_by,
    )


def announce(
    target: str,
    message: engine_pb2.PeerAnnounce,
    timeout: float = 4.0,
) -> engine_pb2.AnnounceAck | None:
    """``None`` when the peer is not a directory (``UNIMPLEMENTED``) or is down.

    A refused hint (empty project, self-announce) is an ack with ``accepted``
    false, not ``None``.
    """
    addr = target.replace("grpc://", "").strip()
    try:
        with channel(addr) as ch:
            return engine_pb2_grpc.EngineStub(ch).Announce(message, timeout=timeout)
    except grpc.RpcError as exc:
        if exc.code() == grpc.StatusCode.UNIMPLEMENTED:
            log.info("Announce UNIMPLEMENTED at %s — not a directory", addr)
            return None
        log.warning("Announce failed at %s: %s %s", addr, exc.code(), exc.details())
        return None


def model_ready(target: str, name: str, timeout: float = 4.0) -> bool:
    request = oip_pb.ModelReadyRequest(name=name)
    try:
        with channel(target) as ch:
            response = oip_grpc.GRPCInferenceServiceStub(ch).ModelReady(request, timeout=timeout)
        return bool(response.ready)
    except grpc.RpcError:
        return False


def model_infer(target: str, request: CompletionRequest, timeout: float = 30.0) -> Completion:
    message = oip.build_infer_request(request)
    with channel(target) as ch:
        response = oip_grpc.GRPCInferenceServiceStub(ch).ModelInfer(message, timeout=timeout)
    return oip.completion_from_oip(response)


def model_stream(
    target: str,
    request: CompletionRequest,
    timeout: float = 30.0,
) -> Iterator[Completion]:
    """Stream OIP frames. A unary-only peer (``UNIMPLEMENTED``) is one ModelInfer."""
    message = oip.build_infer_request(request)

    def _once():
        yield message

    try:
        with channel(target) as ch:
            call = oip_grpc.GRPCInferenceServiceStub(ch).ModelStreamInfer(_once(), timeout=timeout)
            for response in call:
                yield oip.completion_from_oip(response)
    except grpc.RpcError as exc:
        if exc.code() != grpc.StatusCode.UNIMPLEMENTED:
            raise
        log.info("ModelStreamInfer UNIMPLEMENTED at %s — unary ModelInfer", target)
        yield model_infer(target, request, timeout=timeout)
