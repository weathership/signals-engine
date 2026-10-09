"""Sample engine and sample client for the multi-hop stream.

The client speaks only to the sample engine. The sample engine does not host
``thinking`` and does not dial vLLM. It routes by Status and ModelReady, emits
one ``progress`` frame (``phase=routing``), and relays ``ModelStreamInfer``.

    SIGNALS_UPSTREAM=127.0.0.1:<reference port> python -m signals_engine.sample
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import grpc

from signals_engine import oip
from signals_engine.client import channel
from signals_engine.route import select_peer
from signals_engine.server import LatticeEngine
from signals_engine.types import CompletionRequest, EngineHooks, PeerIdentity, StreamFrame
from signals_engine.wire.inference import open_inference_grpc_pb2 as oip_pb
from signals_engine.wire.inference import open_inference_grpc_pb2_grpc as oip_grpc

log = logging.getLogger("signals_engine.sample")

_NO_CONTRACT = Path("/signals-engine-no-peer-contract.json")


def serve_sample(
    upstream: tuple[str, ...] | list[str],
    *,
    project: str = "sample",
    port: int = 0,
    bind_host: str = "127.0.0.1",
    advertise_host: str = "127.0.0.1",
) -> LatticeEngine:
    """Bind a forwarder. ``upstream`` is the pairwise seed list, not a gossip set."""
    seeds = tuple(item.strip() for item in upstream if item and item.strip())

    def route(capability: str) -> str:
        return select_peer(list(seeds), capability)

    engine = LatticeEngine(
        PeerIdentity(
            project=project,
            port=port,
            bind_host=bind_host,
            advertise_host=advertise_host,
        ),
        EngineHooks(route=route),
        contract_path=_NO_CONTRACT,
    )
    engine.serve(block=False)
    log.info("sample engine %s routes via %s", engine.engine_target(), ",".join(seeds) or "none")
    return engine


class SampleSession:
    """One bi-directional ``ModelStreamInfer`` on the sample engine.

    ``next_frame`` reads while ``send`` and ``cancel`` write. The call stays
    open across generations until ``close``.
    """

    def __init__(self, ch: grpc.Channel, outbound: queue.Queue, frames: queue.Queue) -> None:
        self._ch = ch
        self._outbound = outbound
        self._frames = frames
        self._closed = False

    def next_frame(self, timeout: float = 10.0) -> StreamFrame:
        item = self._frames.get(timeout=timeout)
        if item is None:
            raise EOFError("stream closed")
        if isinstance(item, BaseException):
            raise item
        return item

    def send(self, request: CompletionRequest) -> None:
        self._outbound.put(oip.build_infer_request(request, request_id=str(uuid.uuid4())))

    def cancel(self) -> None:
        message = oip_pb.ModelInferRequest(id=str(uuid.uuid4()))
        message.parameters["control"].string_param = oip.CONTROL_CANCEL
        self._outbound.put(message)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._outbound.put(None)
        self._ch.close()


class SampleClient:
    """Generic client. Point it at the sample engine, not at vLLM and not at the reference."""

    def __init__(self, target: str) -> None:
        self.target = target.replace("grpc://", "").strip()

    def open(self, request: CompletionRequest) -> SampleSession:
        outbound: queue.Queue = queue.Queue()
        frames: queue.Queue = queue.Queue()
        outbound.put(oip.build_infer_request(request, request_id=str(uuid.uuid4())))
        ch = channel(self.target)

        def requests() -> Iterator[object]:
            while True:
                item = outbound.get()
                if item is None:
                    return
                yield item

        call = oip_grpc.GRPCInferenceServiceStub(ch).ModelStreamInfer(requests())

        def read() -> None:
            try:
                for response in call:
                    frames.put(oip.frame_from_response(response))
            except grpc.RpcError as exc:
                frames.put(exc)
            except Exception as exc:
                frames.put(exc)
            finally:
                frames.put(None)

        threading.Thread(target=read, name="signals-sample-read", daemon=True).start()
        return SampleSession(ch, outbound, frames)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raw = (os.environ.get("SIGNALS_UPSTREAM") or "").strip()
    if not raw:
        raise SystemExit("SIGNALS_UPSTREAM is required (host:port of the reference engine)")
    upstream = tuple(part.strip() for part in raw.split(",") if part.strip())
    port = int(os.environ.get("SIGNALS_ENGINE_PORT", "0"))
    bind = os.environ.get("SIGNALS_ENGINE_BIND", "127.0.0.1")
    engine = serve_sample(upstream, port=port, bind_host=bind, advertise_host=bind)
    print(engine.engine_target(), flush=True)
    try:
        engine._stop.wait()
    except KeyboardInterrupt:
        engine.stop()


if __name__ == "__main__":
    main()
