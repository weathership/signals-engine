"""Listen on one port: Engine, OIP, reflection, and any native servicers.

``grpc.so_reuseport`` is off. A second process on the same port is
``LatticeBindError``, not a kernel lottery between two Status answers.
"""

from __future__ import annotations

import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
from grpc_reflection.v1alpha import reflection

from signals_engine.advertise import advertise_host
from signals_engine.client import announce
from signals_engine.contract import configured_peers
from signals_engine.directory import DEFAULT_ANNOUNCE_TTL_S, Directory, parse_engine_target
from signals_engine.posture import head_sha
from signals_engine.query import _surfaces
from signals_engine.servicer import EngineState, LatticeEngineServicer, OipServicer
from signals_engine.types import EngineHooks, LatticeBindError, PeerIdentity
from signals_engine.wire.inference import open_inference_grpc_pb2 as oip_pb
from signals_engine.wire.inference import open_inference_grpc_pb2_grpc as oip_grpc
from signals_engine.wire.zndx.engine.v1 import engine_pb2
from signals_engine.wire.zndx.engine.v1 import engine_pb2_grpc

log = logging.getLogger("signals_engine.server")

_MAX_MESSAGE = 64 * 1024 * 1024


class LatticeEngine:
    def __init__(
        self,
        identity: PeerIdentity,
        hooks: EngineHooks | None = None,
        *,
        directory: bool = False,
        announce_to_directory: bool = False,
        announce_interval_s: float | None = None,
        directory_targets: tuple[str, ...] | list[str] = (),
        contract_path: Path | None = None,
        lineage_url: str | None = None,
    ) -> None:
        if not identity.project.strip():
            raise ValueError("project is required")
        self.identity = identity
        self.hooks = hooks or EngineHooks()
        self._is_directory = directory
        self._announce = announce_to_directory
        self._announce_interval = (
            announce_interval_s
            if announce_interval_s is not None
            else max(15.0, DEFAULT_ANNOUNCE_TTL_S / 3)
        )
        self._directory_targets = tuple(directory_targets)
        self._contract_path = contract_path
        self._lineage_url = lineage_url
        self.bound_port = 0
        self._server: grpc.Server | None = None
        self._stop = threading.Event()
        self._announce_thread: threading.Thread | None = None
        self._state: EngineState | None = None

    def engine_target(self) -> str:
        host = advertise_host(self.identity)
        if not host or not self.bound_port:
            return ""
        return f"{host}:{self.bound_port}"

    def seeds(self) -> list[str]:
        raws = list(self._directory_targets)
        env = (os.environ.get("SIGNALS_ENGINE_TARGET") or "").strip()
        if env:
            raws.append(env)
        raws.extend(target for _project, target in configured_peers(self.identity, self._contract_path))
        mine = self.engine_target()
        seen: set[str] = set()
        out: list[str] = []
        for raw in raws:
            addr = parse_engine_target(raw)
            if not addr or addr in seen or addr == mine:
                continue
            seen.add(addr)
            out.append(addr)
        return out

    def announce_once(self) -> list[tuple[str, engine_pb2.AnnounceAck | None]]:
        target = self.engine_target()
        if not target:
            log.info("Announce skipped — no advertise host for %s", self.identity.project)
            return []
        assert self._state is not None
        surfaces = [
            engine_pb2.Surface(kind=row.kind, url=row.url, healthy=row.healthy)
            for row in _surfaces(self.identity, self.hooks)
        ]
        message = engine_pb2.PeerAnnounce(
            project=self.identity.project,
            engine_target=target,
            surfaces=surfaces,
            ttl_seconds=DEFAULT_ANNOUNCE_TTL_S,
        )
        results: list[tuple[str, engine_pb2.AnnounceAck | None]] = []
        for seed in self.seeds():
            ack = announce(seed, message)
            results.append((seed, ack))
            if ack is not None and ack.accepted:
                log.info(
                    "Announced %s at %s to %s (ttl=%ss)",
                    self.identity.project,
                    target,
                    seed,
                    ack.ttl_seconds,
                )
        return results

    def serve(self, *, block: bool = True) -> None:
        if self._server is not None:
            raise RuntimeError("serve() already called")
        root = self.identity.repo_root
        if root is None and (Path.cwd() / ".git").exists():
            root = Path.cwd()
        running = head_sha(root)
        state = EngineState(
            self.identity,
            self.hooks,
            directory=Directory(self.identity.project) if self._is_directory else None,
            repo_root=root,
            running_sha=running,
            contract=self._contract_path,
            lineage=self._lineage_url,
        )
        self._state = state
        server = grpc.server(
            ThreadPoolExecutor(max_workers=16),
            options=[
                ("grpc.max_send_message_length", _MAX_MESSAGE),
                ("grpc.max_receive_message_length", _MAX_MESSAGE),
                ("grpc.so_reuseport", 0),
            ],
        )
        engine_pb2_grpc.add_EngineServicer_to_server(LatticeEngineServicer(state), server)
        oip_grpc.add_GRPCInferenceServiceServicer_to_server(OipServicer(state), server)
        names = [
            engine_pb2.DESCRIPTOR.services_by_name["Engine"].full_name,
            oip_pb.DESCRIPTOR.services_by_name["GRPCInferenceService"].full_name,
        ]
        for native in self.hooks.native:
            native.add_to_server(native.servicer, server)
            names.append(native.full_name)
        names.append(reflection.SERVICE_NAME)
        reflection.enable_server_reflection(names, server)

        listen = f"{self.identity.bind_host}:{self.identity.port}"
        try:
            bound = server.add_insecure_port(listen)
        except RuntimeError as exc:
            # grpcio raises instead of returning 0 when the port is held.
            server.stop(0)
            raise LatticeBindError(
                f"could not bind {listen} (port held; grpc.so_reuseport=0): {exc}"
            ) from exc
        if bound == 0:
            server.stop(0)
            raise LatticeBindError(
                f"could not bind {listen} (port held; grpc.so_reuseport=0)"
            )
        self.bound_port = bound
        server.start()
        self._server = server
        log.info(
            "%s listening on %s:%s (Engine + OIP + reflection, directory=%s, model=%s)",
            self.identity.project,
            self.identity.bind_host,
            bound,
            self._is_directory,
            state.serves_model,
        )
        if self._announce:
            self._announce_thread = threading.Thread(
                target=self._announce_loop,
                name=f"announce-{self.identity.project}",
                daemon=True,
            )
            self._announce_thread.start()
        if block:
            try:
                server.wait_for_termination()
            except KeyboardInterrupt:
                self.stop()

    def _announce_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.announce_once()
            except Exception:
                log.exception("Announce heartbeat failed for %s", self.identity.project)
            if self._stop.wait(self._announce_interval):
                return

    def stop(self, grace: float = 0) -> None:
        self._stop.set()
        server = self._server
        if server is not None:
            server.stop(grace)
            self._server = None


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    project = os.environ.get("SIGNALS_ENGINE_PROJECT", "").strip()
    if not project:
        raise SystemExit("SIGNALS_ENGINE_PROJECT is required")
    port = int(os.environ.get("SIGNALS_ENGINE_PORT", "0"))
    bind = os.environ.get("SIGNALS_ENGINE_BIND", "0.0.0.0")
    LatticeEngine(
        PeerIdentity(project=project, port=port, bind_host=bind),
        directory=os.environ.get("SIGNALS_ENGINE_DIRECTORY", "").strip() in {"1", "true", "yes"},
        announce_to_directory=True,
    ).serve()


if __name__ == "__main__":
    main()
