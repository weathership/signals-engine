"""zndx.engine.v1.Engine and inference.GRPCInferenceService on one port."""

from __future__ import annotations

import queue
import threading
from collections.abc import Iterator
from pathlib import Path

import grpc

from signals_engine import oip
from signals_engine.directory import Directory
from signals_engine.lineage import lineage_url, record_lineage
from signals_engine.query import fill_server_query
from signals_engine.types import (
    Completion,
    CompletionRequest,
    EndpointSpec,
    EngineHooks,
    HandlerError,
    PeerIdentity,
    StreamFrame,
    YieldOutcome,
)
from signals_engine.wire.inference import open_inference_grpc_pb2 as oip_pb
from signals_engine.wire.inference import open_inference_grpc_pb2_grpc as oip_grpc
from signals_engine.wire.zndx.engine.v1 import engine_pb2
from signals_engine.wire.zndx.engine.v1 import engine_pb2_grpc

_NO_MODEL = (
    "this engine does not serve a model; Complete and OIP ModelInfer "
    "stay unimplemented until a host registers a completion handler"
)
_NOT_DIRECTORY = "this engine is not a peer directory (Announce is UNIMPLEMENTED)"


class EngineState:
    def __init__(
        self,
        identity: PeerIdentity,
        hooks: EngineHooks,
        *,
        directory: Directory | None,
        repo_root: Path | None,
        running_sha: str,
        contract: Path | None,
        lineage: str | None,
    ) -> None:
        self.identity = identity
        self.hooks = hooks
        self.directory = directory
        self.repo_root = repo_root
        self.running_sha = running_sha
        self.contract = contract
        self.lineage = lineage_url(lineage)

    @property
    def serves_model(self) -> bool:
        return self.hooks.complete is not None or self.hooks.stream is not None

    @property
    def accepts_inference(self) -> bool:
        return self.serves_model or self.hooks.route is not None


def _hosts(state: EngineState, capability: str) -> bool:
    if state.hooks.endpoints is None:
        return False
    return any(endpoint.capability == capability for endpoint in state.hooks.endpoints())


def _call_complete(state: EngineState, request, context):
    if state.hooks.complete is None:
        context.abort(grpc.StatusCode.UNIMPLEMENTED, _NO_MODEL)
    try:
        return state.hooks.complete(request)
    except HandlerError as exc:
        context.abort(exc.code, exc.detail)


class LatticeEngineServicer(engine_pb2_grpc.EngineServicer):
    def __init__(self, state: EngineState) -> None:
        self.state = state

    def Status(self, request, context):
        identity = self.state.identity
        try:
            endpoints = (
                list(self.state.hooks.endpoints())
                if self.state.hooks.endpoints is not None
                else []
            )
        except HandlerError as exc:
            context.abort(exc.code, exc.detail)
        message = engine_pb2.StatusResponse(project=identity.project, total_gpus=_gpu_total(endpoints))
        for endpoint in endpoints:
            message.endpoints.add(
                capability=endpoint.capability,
                model=endpoint.model,
                healthy=endpoint.healthy,
                gpu_ids=list(endpoint.gpu_ids),
                detail=endpoint.detail,
            )
        for surface in _status_surfaces(self.state):
            message.surfaces.add(kind=surface.kind, url=surface.url, healthy=surface.healthy)
        return message

    def ServerQuery(self, request, context):
        response = engine_pb2.ServerQueryResponse(project=self.state.identity.project)
        try:
            fill_server_query(
                response,
                request,
                identity=self.state.identity,
                hooks=self.state.hooks,
                directory=self.state.directory,
                repo_root=self.state.repo_root,
                running_sha=self.state.running_sha,
                contract=self.state.contract,
            )
        except HandlerError as exc:
            context.abort(exc.code, exc.detail)
        return response

    def Complete(self, request, context):
        result = _call_complete(self.state, oip.completion_request_from_complete(request), context)
        message = engine_pb2.CompleteResponse(
            text=result.text,
            model=result.model,
            prompt_tokens=int(result.prompt_tokens),
            completion_tokens=int(result.completion_tokens),
            latency_ms=float(result.latency_ms),
            reasoning_content=result.reasoning_content,
            finish_reason=result.finish_reason or "stop",
            fulfilled_by=result.fulfilled_by,
        )
        for call in result.tool_calls:
            message.tool_calls.add(id=call.id, name=call.name, arguments_json=call.arguments_json)
        return message

    def Yield(self, request, context):
        workload_id = request.workload_id or ""
        if self.state.hooks.yield_workload is None:
            return engine_pb2.YieldResponse(
                ok=True,
                process_ended=False,
                restore_started=False,
                message=f"no workload {workload_id or '(empty)'}",
            )
        try:
            outcome: YieldOutcome = self.state.hooks.yield_workload(
                workload_id,
                int(request.reason or 0),
                request.sentinel_id or "",
                request.detail or "",
            )
        except HandlerError as exc:
            context.abort(exc.code, exc.detail)
        return engine_pb2.YieldResponse(
            ok=outcome.ok,
            process_ended=outcome.process_ended,
            restore_started=outcome.restore_started,
            message=outcome.message,
        )

    def RecordLineage(self, request, context):
        return record_lineage(request, context, self.state.lineage)

    def Announce(self, request, context):
        if self.state.directory is None:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, _NOT_DIRECTORY)
        ok, ttl, err = self.state.directory.remember(
            request.project,
            request.engine_target,
            int(request.ttl_seconds or 0),
        )
        return engine_pb2.AnnounceAck(accepted=ok, ttl_seconds=ttl, error=err)

    def Remediate(self, request, context):
        return _delegate(self.state, "Remediate", request, context)

    def PutAgendaItem(self, request, context):
        return _delegate(
            self.state,
            "PutAgendaItem",
            request,
            context,
            unimplemented="PutAgendaItem is served by the agenda holder",
        )

    def WatchWorkload(self, request, context):
        handler = self.state.hooks.rpc.get("WatchWorkload")
        if handler is None:
            context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                "WatchWorkload is not registered on this engine",
            )
        yield from handler(request, context)


def _delegate(state, name, request, context, unimplemented: str | None = None):
    handler = state.hooks.rpc.get(name)
    if handler is None:
        context.abort(
            grpc.StatusCode.UNIMPLEMENTED,
            unimplemented or f"{name} is not registered on this engine",
        )
    try:
        return handler(request, context)
    except HandlerError as exc:
        context.abort(exc.code, exc.detail)


def _gpu_total(endpoints: list[EndpointSpec]) -> int:
    ids = {gpu for endpoint in endpoints for gpu in endpoint.gpu_ids}
    return len(ids)


def _status_surfaces(state: EngineState):
    from signals_engine.query import _surfaces

    return _surfaces(state.identity, state.hooks)


class OipServicer(oip_grpc.GRPCInferenceServiceServicer):
    def __init__(self, state: EngineState) -> None:
        self.state = state

    def ServerLive(self, request, context):
        return oip_pb.ServerLiveResponse(live=True)

    def ServerReady(self, request, context):
        # The process can accept inference only when a host registered a model handler.
        # Per-capability residency is ModelReady.
        return oip_pb.ServerReadyResponse(ready=self.state.accepts_inference)

    def ModelReady(self, request, context):
        name = (request.name or "").strip()
        if not name or not self.state.serves_model:
            return oip_pb.ModelReadyResponse(ready=False)
        if self.state.hooks.model_ready is not None:
            try:
                ready = bool(self.state.hooks.model_ready(name))
            except HandlerError as exc:
                context.abort(exc.code, exc.detail)
            return oip_pb.ModelReadyResponse(ready=ready)
        ready = False
        if self.state.hooks.endpoints is not None:
            for endpoint in self.state.hooks.endpoints():
                if endpoint.healthy and name in (endpoint.capability, endpoint.model):
                    ready = True
                    break
        return oip_pb.ModelReadyResponse(ready=ready)

    def ServerMetadata(self, request, context):
        return oip_pb.ServerMetadataResponse(
            name=self.state.identity.project,
            version=self.state.identity.version or "0.2.0",
            extensions=oip.metadata_extensions(tools=self.state.serves_model),
        )

    def ModelMetadata(self, request, context):
        name = (request.name or "").strip()
        tools = self.state.serves_model
        message = oip_pb.ModelMetadataResponse(
            name=name,
            versions=["v1"],
            platform="signals-engine",
        )
        for tensor_name in oip.input_names(tools=tools):
            tensor = message.inputs.add()
            tensor.name = tensor_name
            tensor.datatype = "BYTES"
            tensor.shape.extend([1])
        for tensor_name in oip.output_names(tools=tools):
            tensor = message.outputs.add()
            tensor.name = tensor_name
            tensor.datatype = "BYTES"
            tensor.shape.extend([1])
        return message

    def ModelInfer(self, request, context):
        incoming = oip.completion_request_from_oip(request)
        if not incoming.prompt and not incoming.messages_json:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'no "prompt" input tensor provided')
        if self.state.hooks.complete is not None:
            result = _call_complete(self.state, incoming, context)
        elif self.state.hooks.stream is not None:
            result = _fold_stream(self.state, incoming, context)
        else:
            context.abort(grpc.StatusCode.UNIMPLEMENTED, _NO_MODEL)
        response = oip_pb.ModelInferResponse()
        oip.apply_completion(
            response,
            result,
            request_id=request.id or "",
            model_name=request.model_name or incoming.capability,
        )
        return response

    def ModelStreamInfer(self, request_iterator, context):
        # Bi-directional when a host streams or routes. Otherwise one unary
        # completion frame per request, for engines that only register complete.
        if self.state.hooks.stream is not None or self.state.hooks.route is not None:
            yield from _bidi_stream(self.state, request_iterator, context)
            return
        for request in request_iterator:
            incoming = oip.completion_request_from_oip(request)
            if not incoming.prompt and not incoming.messages_json:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    'no "prompt" input tensor provided',
                )
            result = _call_complete(self.state, incoming, context)
            response = oip_pb.ModelInferResponse()
            oip.apply_completion(
                response,
                result,
                request_id=request.id or "",
                model_name=request.model_name or incoming.capability,
            )
            yield response


def _fold_stream(state: EngineState, request: CompletionRequest, context) -> Completion:
    text: list[str] = []
    reasoning: list[str] = []
    final: StreamFrame | None = None
    assert state.hooks.stream is not None
    try:
        for frame in state.hooks.stream(request, threading.Event()):
            if frame.kind == "text_delta":
                text.append(frame.text)
            elif frame.kind == "reasoning_delta":
                reasoning.append(frame.text)
            elif frame.kind == "final":
                final = frame
    except HandlerError as exc:
        context.abort(exc.code, exc.detail)
    if final is not None:
        return Completion(
            text=final.text or "".join(text),
            model=final.model,
            prompt_tokens=final.prompt_tokens,
            completion_tokens=final.completion_tokens,
            latency_ms=final.latency_ms,
            reasoning_content=final.reasoning or "".join(reasoning),
            finish_reason=final.finish_reason or "stop",
            fulfilled_by=final.fulfilled_by,
        )
    return Completion(
        text="".join(text),
        reasoning_content="".join(reasoning),
        finish_reason="stop",
    )


def _next_request(request_iterator):
    try:
        return next(request_iterator)
    except (StopIteration, grpc.RpcError):
        return None


def _bidi_stream(state: EngineState, request_iterator, context) -> Iterator[object]:
    first = _next_request(request_iterator)
    if first is None:
        return
    if oip.is_cancel(first):
        yield oip.frame_to_response(
            StreamFrame(kind="final", finish_reason="cancelled"),
            request_id=first.id or "",
            capability="",
        )
        return
    incoming = oip.completion_request_from_oip(first)
    if not incoming.prompt and not incoming.messages_json:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'no "prompt" input tensor provided')
    capability = incoming.capability
    if not capability:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "capability is required")
    try:
        hosted = _hosts(state, capability)
    except HandlerError as exc:
        context.abort(exc.code, exc.detail)
    if hosted and state.hooks.stream is not None:
        yield from _host_session(state, first, request_iterator, context)
        return
    if state.hooks.route is not None:
        yield from _relay_session(state, first, request_iterator, context, capability)
        return
    if state.hooks.stream is not None:
        yield from _host_session(state, first, request_iterator, context)
        return
    context.abort(grpc.StatusCode.UNIMPLEMENTED, _NO_MODEL)


def _host_session(state: EngineState, first, request_iterator, context) -> Iterator[object]:
    """Run generations. A later request cancels the one in flight."""
    assert state.hooks.stream is not None
    pending = first
    while pending is not None:
        if oip.is_cancel(pending):
            yield oip.frame_to_response(
                StreamFrame(kind="final", finish_reason="cancelled"),
                request_id=pending.id or "",
                capability="",
            )
            pending = _next_request(request_iterator)
            continue
        incoming = oip.completion_request_from_oip(pending)
        if not incoming.prompt and not incoming.messages_json:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'no "prompt" input tensor provided')
        cancel = threading.Event()
        box: dict[str, object] = {}

        def watch(box: dict[str, object] = box, cancel: threading.Event = cancel) -> None:
            box["req"] = _next_request(request_iterator)
            cancel.set()

        watcher = threading.Thread(target=watch, name="signals-stream-watch", daemon=True)
        watcher.start()
        saw_final = False
        text: list[str] = []
        reasoning: list[str] = []
        try:
            for frame in state.hooks.stream(incoming, cancel):
                if frame.kind == "text_delta":
                    text.append(frame.text)
                elif frame.kind == "reasoning_delta":
                    reasoning.append(frame.text)
                elif frame.kind == "final":
                    saw_final = True
                yield oip.frame_to_response(
                    frame,
                    request_id=pending.id or "",
                    capability=incoming.capability,
                )
                if cancel.is_set() and frame.kind != "final":
                    break
        except HandlerError as exc:
            context.abort(exc.code, exc.detail)
        if cancel.is_set():
            watcher.join(timeout=2.0)
            nxt = box.get("req")
            if not saw_final:
                reason = "superseded" if nxt is not None and not oip.is_cancel(nxt) else "cancelled"
                yield oip.frame_to_response(
                    StreamFrame(
                        kind="final",
                        text="".join(text),
                        reasoning="".join(reasoning),
                        finish_reason=reason,
                    ),
                    request_id=pending.id or "",
                    capability=incoming.capability,
                )
        else:
            if not saw_final:
                yield oip.frame_to_response(
                    StreamFrame(
                        kind="final",
                        text="".join(text),
                        reasoning="".join(reasoning),
                        finish_reason="stop",
                    ),
                    request_id=pending.id or "",
                    capability=incoming.capability,
                )
            watcher.join()
            nxt = box.get("req")
        if nxt is not None and oip.is_cancel(nxt):
            pending = _next_request(request_iterator)
        else:
            pending = nxt


def _relay_session(state: EngineState, first, request_iterator, context, capability: str) -> Iterator[object]:
    """One routing progress frame, then the upstream ModelInferResponses unchanged."""
    from signals_engine.route import relay_model_stream

    assert state.hooks.route is not None
    try:
        target = state.hooks.route(capability)
    except HandlerError as exc:
        context.abort(exc.code, exc.detail)
    if not target:
        context.abort(
            grpc.StatusCode.FAILED_PRECONDITION,
            f"no peer hosts {capability}",
        )
    yield oip.frame_to_response(
        StreamFrame(kind="progress", phase="routing", detail=target),
        request_id=first.id or "",
        capability=capability,
    )
    bridge: queue.Queue = queue.Queue()

    def pump() -> None:
        try:
            for request in request_iterator:
                bridge.put(request)
        except grpc.RpcError:
            return
        finally:
            bridge.put(None)

    threading.Thread(target=pump, name="signals-relay-pump", daemon=True).start()

    def upstream():
        yield first
        while True:
            item = bridge.get()
            if item is None:
                return
            yield item

    try:
        yield from relay_model_stream(target, upstream())
    except grpc.RpcError as exc:
        context.abort(exc.code(), exc.details() or exc.code().name)
    except HandlerError as exc:
        context.abort(exc.code, exc.detail)
