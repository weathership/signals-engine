"""Two engines, one port each: Status, reflection, S2S, OIP, Announce, lineage."""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import grpc
import pytest
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc

from signals_engine import (
    Completion,
    CompletionRequest,
    EndpointSpec,
    EngineHooks,
    LatticeBindError,
    LatticeEngine,
    PeerIdentity,
    SurfaceSpec,
    ToolCall,
    YieldOutcome,
    complete,
    model_infer,
    model_stream,
    server_query,
    status,
)
from signals_engine.client import channel
from signals_engine.wire.inference import open_inference_grpc_pb2 as oip_pb
from signals_engine.wire.inference import open_inference_grpc_pb2_grpc as oip_grpc
from signals_engine.wire.zndx.engine.v1 import engine_pb2
from signals_engine.wire.zndx.engine.v1 import engine_pb2_grpc

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _isolate_lab(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SIGNALS_ENGINE_TARGET", raising=False)
    monkeypatch.delenv("SIGNALS_PEER_CONTRACT", raising=False)
    monkeypatch.delenv("SIGNALS_ATLAS_OL_URL", raising=False)
    monkeypatch.delenv("SIGNALS_ADVERTISE_HOST", raising=False)


@pytest.fixture
def start():
    running: list[LatticeEngine] = []

    def _start(
        project: str,
        *,
        port: int = 0,
        hooks: EngineHooks | None = None,
        directory: bool = False,
        contract_path: Path | None = None,
        directory_targets: tuple[str, ...] = (),
        lineage_url: str | None = None,
        primary_ui: str = "",
    ) -> LatticeEngine:
        engine = LatticeEngine(
            PeerIdentity(
                project=project,
                port=port,
                bind_host="127.0.0.1",
                advertise_host="127.0.0.1",
                primary_ui=primary_ui,
                repo_root=ROOT,
            ),
            hooks,
            directory=directory,
            contract_path=contract_path,
            directory_targets=directory_targets,
            lineage_url=lineage_url,
        )
        engine.serve(block=False)
        running.append(engine)
        return engine

    yield _start
    for engine in running:
        engine.stop()


def _target(engine: LatticeEngine) -> str:
    return f"127.0.0.1:{engine.bound_port}"


def _services(target: str) -> set[str]:
    with channel(target) as ch:
        stub = reflection_pb2_grpc.ServerReflectionStub(ch)
        req = reflection_pb2.ServerReflectionRequest(list_services="")
        replies = list(stub.ServerReflectionInfo(iter([req])))
    return {svc.name for reply in replies for svc in reply.list_services_response.service}


def test_status_reflection_and_honest_unimplemented(start) -> None:
    engine = start(
        "semantica",
        primary_ui="http://127.0.0.1:8000/",
        hooks=EngineHooks(
            endpoints=lambda: [
                EndpointSpec(capability="context", model="", healthy=False, detail="graph down")
            ],
            surfaces=lambda: [SurfaceSpec(kind="primary", url="http://127.0.0.1:8000/", healthy=False)],
        ),
    )
    target = _target(engine)
    names = _services(target)
    assert "zndx.engine.v1.Engine" in names
    assert "inference.GRPCInferenceService" in names
    assert "grpc.reflection.v1alpha.ServerReflection" in names

    body = status(target)
    assert body.project == "semantica"
    assert body.endpoints[0].capability == "context"
    assert body.endpoints[0].healthy is False
    assert body.surfaces[0].kind == "primary"
    assert body.surfaces[0].healthy is False

    with channel(target) as ch:
        stub = engine_pb2_grpc.EngineStub(ch)
        with pytest.raises(grpc.RpcError) as complete_err:
            stub.Complete(engine_pb2.CompleteRequest(prompt="hi"), timeout=3)
        assert complete_err.value.code() == grpc.StatusCode.UNIMPLEMENTED
        with pytest.raises(grpc.RpcError) as announce_err:
            stub.Announce(
                engine_pb2.PeerAnnounce(project="other", engine_target="127.0.0.1:1"),
                timeout=3,
            )
        assert announce_err.value.code() == grpc.StatusCode.UNIMPLEMENTED
        yielded = stub.Yield(engine_pb2.YieldRequest(workload_id="missing"), timeout=3)
    assert yielded.ok is True
    assert yielded.process_ended is False

    with channel(target) as ch:
        meta = oip_grpc.GRPCInferenceServiceStub(ch).ServerMetadata(
            oip_pb.ServerMetadataRequest(),
            timeout=3,
        )
    assert "zndx.engine.v1.Engine" in meta.extensions
    assert "llm_tools_v1" not in meta.extensions


def test_model_handler_serves_complete_and_oip(start) -> None:
    seen: dict[str, str] = {}

    def _complete(request: CompletionRequest) -> Completion:
        seen["prompt"] = request.prompt
        seen["tools"] = request.tools_json
        seen["capability"] = request.capability
        return Completion(
            text=f"echo:{request.prompt}",
            model="semantica-test",
            reasoning_content="trace",
            finish_reason="stop",
            tool_calls=(ToolCall(name="lookup", arguments_json="{}", id="c1"),),
            prompt_tokens=3,
            completion_tokens=2,
        )

    engine = start(
        "semantica",
        hooks=EngineHooks(
            complete=_complete,
            endpoints=lambda: [EndpointSpec(capability="context", model="semantica-test", healthy=True)],
        ),
    )
    target = _target(engine)
    answered = complete(target, CompletionRequest(capability="context", prompt="ping"))
    assert answered.text == "echo:ping"
    assert answered.model == "semantica-test"
    assert answered.tool_calls[0].name == "lookup"

    inferred = model_infer(
        target,
        CompletionRequest(capability="context", prompt="pong", tools_json='[{"name":"lookup"}]'),
    )
    assert seen["prompt"] == "pong"
    assert seen["tools"] == '[{"name":"lookup"}]'
    assert seen["capability"] == "context"
    assert inferred.text == "echo:pong"
    assert inferred.reasoning_content == "trace"
    assert inferred.tool_calls[0].id == "c1"

    frames = list(model_stream(target, CompletionRequest(capability="context", prompt="stream")))
    assert [frame.text for frame in frames] == ["echo:stream"]

    with channel(target) as ch:
        oip = oip_grpc.GRPCInferenceServiceStub(ch)
        ready = oip.ModelReady(oip_pb.ModelReadyRequest(name="context"), timeout=3)
        assert ready.ready is True
        meta = oip.ServerMetadata(oip_pb.ServerMetadataRequest(), timeout=3)
        assert "llm_tools_v1" in meta.extensions
        live = oip.ServerReady(oip_pb.ServerReadyRequest(), timeout=3)
        assert live.ready is True


def test_oip_without_prompt_is_invalid(start) -> None:
    engine = start("semantica", hooks=EngineHooks(complete=lambda _request: Completion(text="x")))
    with pytest.raises(grpc.RpcError) as err:
        model_infer(_target(engine), CompletionRequest(capability="context", prompt=""))
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_unary_oip_peer_falls_back_from_stream() -> None:
    class _Unary(oip_grpc.GRPCInferenceServiceServicer):
        def ModelInfer(self, request, context):
            response = oip_pb.ModelInferResponse(model_name="cis", id=request.id)
            out = response.outputs.add()
            out.name = "completion"
            out.datatype = "BYTES"
            out.shape.extend([1])
            out.contents.bytes_contents.append(b"unary")
            return response

        def ModelStreamInfer(self, request_iterator, context):
            context.abort(grpc.StatusCode.UNIMPLEMENTED, "unary only")
            yield oip_pb.ModelInferResponse()

    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    oip_grpc.add_GRPCInferenceServiceServicer_to_server(_Unary(), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    try:
        frames = list(model_stream(f"127.0.0.1:{port}", CompletionRequest(capability="cis", prompt="x")))
    finally:
        server.stop(0)
    assert [frame.text for frame in frames] == ["unary"]


def test_directory_announce_and_peers(start, tmp_path: Path) -> None:
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"engine_grpc_lattice": {}}))
    hub = start("hub", directory=True, contract_path=empty)
    contract = tmp_path / "peer-contract.json"
    contract.write_text(
        json.dumps({"engine_grpc_lattice": {"hub": hub.bound_port, "service": "zndx.engine.v1.Engine"}})
    )
    peer = start(
        "semantica",
        contract_path=contract,
        directory_targets=("127.0.0.1:1",),
        hooks=EngineHooks(
            surfaces=lambda: [SurfaceSpec(kind="primary", url="http://127.0.0.1:8000/", healthy=True)]
        ),
    )
    acks = peer.announce_once()
    by_target = {target: ack for target, ack in acks}
    assert _target(hub) in by_target
    assert "127.0.0.1:1" in by_target
    hub_ack = by_target[_target(hub)]
    assert hub_ack is not None and hub_ack.accepted is True
    assert by_target["127.0.0.1:1"] is None

    peers = server_query(_target(hub), engine_pb2.SERVER_QUERY_KIND_PEERS)
    found = {(row.project, row.target) for row in peers.peers}
    assert found == {("semantica", _target(peer))}

    with channel(_target(hub)) as ch:
        refused = engine_pb2_grpc.EngineStub(ch).Announce(
            engine_pb2.PeerAnnounce(project="hub", engine_target=_target(hub)),
            timeout=3,
        )
    assert refused.accepted is False
    assert refused.error == "self-announce"


def test_configured_peers_skip_self(start, tmp_path: Path) -> None:
    path = tmp_path / "peer-contract.json"
    path.write_text(
        json.dumps(
            {
                "engine_grpc_lattice": {
                    "semantica": 50751,
                    "gaius": 50051,
                    "service": "zndx.engine.v1.Engine",
                    "proto": "ignored",
                }
            }
        )
    )
    engine = start("semantica", contract_path=path)
    body = server_query(_target(engine), engine_pb2.SERVER_QUERY_KIND_PEERS)
    assert [(row.project, row.target) for row in body.peers] == [("gaius", "127.0.0.1:50051")]


def test_remotes_and_source_posture(start) -> None:
    engine = start("semantica")
    target = _target(engine)
    remotes = server_query(target, engine_pb2.SERVER_QUERY_KIND_REMOTES)
    assert any(row.name == "origin" and "signals-engine" in row.url for row in remotes.remotes)
    assert len(remotes.head) == 40
    posture = server_query(target, engine_pb2.SERVER_QUERY_KIND_SOURCE_POSTURE)
    assert posture.posture.project == "semantica"
    assert posture.posture.head == remotes.head
    assert posture.posture.running_sha == remotes.head
    assert any(row.path.endswith("signals-protocol") for row in posture.posture.submodules)


def test_exclusive_bind(start) -> None:
    first = start("semantica")
    with pytest.raises(LatticeBindError):
        start("other", port=first.bound_port)
    assert status(_target(first)).project == "semantica"


def test_record_lineage_posts_to_atlas() -> None:
    bodies: list[tuple[str, bytes]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            size = int(self.headers.get("Content-Length") or 0)
            bodies.append((self.path, self.rfile.read(size)))
            self.send_response(204)
            self.end_headers()

        def log_message(self, fmt: str, *args) -> None:
            return

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    port = httpd.server_address[1]
    engine = LatticeEngine(
        PeerIdentity(project="semantica", bind_host="127.0.0.1", advertise_host="127.0.0.1", repo_root=ROOT),
        lineage_url=f"http://127.0.0.1:{port}/api/v1/lineage",
    )
    engine.serve(block=False)
    try:
        event = {"eventType": "START", "eventTime": "2026-10-09T00:00:00.000Z"}
        with channel(_target(engine)) as ch:
            stub = engine_pb2_grpc.EngineStub(ch)
            ok = stub.RecordLineage(
                engine_pb2.LineageRequest(event_json=json.dumps(event), event_type="START"),
                timeout=3,
            )
            assert ok.accepted is True
            with pytest.raises(grpc.RpcError) as empty:
                stub.RecordLineage(engine_pb2.LineageRequest(event_json=""), timeout=3)
            assert empty.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    finally:
        engine.stop()
        httpd.shutdown()
    assert bodies
    assert bodies[0][0].endswith("/lineage")
    assert json.loads(bodies[0][1])["eventType"] == "START"


def test_yield_handler(start) -> None:
    def _yield(workload_id: str, reason: int, sentinel_id: str, detail: str) -> YieldOutcome:
        assert workload_id == "job-1"
        assert sentinel_id == "mini"
        return YieldOutcome(ok=True, process_ended=True, message=detail or "ended")

    engine = start("semantica", hooks=EngineHooks(yield_workload=_yield))
    with channel(_target(engine)) as ch:
        response = engine_pb2_grpc.EngineStub(ch).Yield(
            engine_pb2.YieldRequest(
                workload_id="job-1",
                reason=engine_pb2.YIELD_REASON_COMPLETED,
                sentinel_id="mini",
                detail="done",
            ),
            timeout=3,
        )
    assert response.process_ended is True
    assert response.message == "done"
