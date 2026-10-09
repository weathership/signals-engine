"""The engine publishes one protocol snapshot for peers to import."""

from __future__ import annotations

import hashlib
import subprocess
from pathlib import Path

from signals_engine import PROTOCOL_SHA, proto_root
from signals_engine.protocol import OIP_SHA256, include_dirs

ROOT = Path(__file__).resolve().parents[1]
PROTOCOL = ROOT / "components" / "signals-protocol"
OIP = PROTOCOL / "proto" / "inference" / "open_inference_grpc.proto"


def test_protocol_sha_matches_submodule_head() -> None:
    head = subprocess.check_output(
        ["git", "-C", str(PROTOCOL), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    assert PROTOCOL_SHA == head
    assert (proto_root() / "SNAPSHOT.txt").read_text().splitlines()[1] == f"signals-protocol {head}"


def test_packaged_protos_match_sources() -> None:
    packaged = proto_root()
    sources = list((PROTOCOL / "proto").rglob("*.proto"))
    assert sources
    for src in sources:
        rel = src.relative_to(PROTOCOL / "proto")
        assert (packaged / rel).read_bytes() == src.read_bytes()
    assert (packaged / "inference" / "open_inference_grpc.proto").read_bytes() == OIP.read_bytes()
    assert OIP_SHA256 == hashlib.sha256(OIP.read_bytes()).hexdigest()
    assert include_dirs() == [str(packaged)]


def test_cross_package_stubs_import_engine_messages() -> None:
    from signals_engine.wire.zndx.agent.v1 import agent_pb2, agent_pb2_grpc
    from signals_engine.wire.zndx.engine.v1 import engine_pb2
    from signals_engine.wire.zndx.scheduler.v1 import scheduler_pb2, scheduler_pb2_grpc
    from signals_engine.wire.zndx.supervision.v1 import supervision_pb2, supervision_pb2_grpc

    assert engine_pb2.DESCRIPTOR.package == "zndx.engine.v1"
    assert scheduler_pb2.DESCRIPTOR.package == "zndx.scheduler.v1"
    assert agent_pb2.DESCRIPTOR.package == "zndx.agent.v1"
    assert supervision_pb2.DESCRIPTOR.package == "zndx.supervision.v1"
    assert agent_pb2_grpc.AgentsStub is not None
    assert scheduler_pb2_grpc.SchedulerStub is not None
    assert supervision_pb2_grpc.NautilusStub is not None
    # scheduler.proto and agent.proto import engine.proto. Importing the
    # generated modules resolves that through signals_engine.wire.
    for module in (scheduler_pb2, agent_pb2):
        assert any(dep.package == "zndx.engine.v1" for dep in module.DESCRIPTOR.dependencies)
