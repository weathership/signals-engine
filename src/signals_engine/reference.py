"""Reference engine: hosts one model capability and dials vLLM in private.

Run against the lab endpoint without Gaius::

    SIGNALS_VLLM_BASE_URL=http://127.0.0.1:8081/v1 python -m signals_engine.reference

``Status`` advertises the capability and the model id. It does not advertise
the vLLM URL. ``ModelStreamInfer`` emits OIP frames from the token stream.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import grpc

from signals_engine.server import LatticeEngine
from signals_engine.types import EndpointSpec, EngineHooks, HandlerError, PeerIdentity
from signals_engine.vllm import VllmEndpoint, from_env, iter_chat, ready, resolved_model

log = logging.getLogger("signals_engine.reference")

_NO_CONTRACT = Path("/signals-engine-no-peer-contract.json")


def serve_reference(
    endpoint: VllmEndpoint,
    *,
    capability: str = "thinking",
    project: str = "reference",
    port: int = 0,
    bind_host: str = "127.0.0.1",
    advertise_host: str = "127.0.0.1",
) -> LatticeEngine:
    """Bind an engine that streams ``capability`` from ``endpoint``."""

    def endpoints() -> list[EndpointSpec]:
        model = resolved_model(endpoint)
        return [
            EndpointSpec(
                capability=capability,
                model=model,
                healthy=ready(endpoint),
                detail="",
            )
        ]

    def model_ready(name: str) -> bool:
        return name == capability and ready(endpoint)

    def stream(request, cancel):
        if request.capability and request.capability != capability:
            raise HandlerError(
                grpc.StatusCode.FAILED_PRECONDITION,
                f"this engine hosts {capability}",
            )
        yield from iter_chat(endpoint, request, cancel, capability=capability)

    engine = LatticeEngine(
        PeerIdentity(
            project=project,
            port=port,
            bind_host=bind_host,
            advertise_host=advertise_host,
        ),
        EngineHooks(endpoints=endpoints, model_ready=model_ready, stream=stream),
        contract_path=_NO_CONTRACT,
    )
    engine.serve(block=False)
    log.info("reference engine %s hosts %s", engine.engine_target(), capability)
    return engine


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    endpoint = from_env()
    if endpoint is None:
        raise SystemExit("SIGNALS_VLLM_BASE_URL is required (for example http://127.0.0.1:8081/v1)")
    port = int(os.environ.get("SIGNALS_ENGINE_PORT", "0"))
    bind = os.environ.get("SIGNALS_ENGINE_BIND", "127.0.0.1")
    capability = (os.environ.get("SIGNALS_VLLM_CAPABILITY") or "thinking").strip() or "thinking"
    engine = serve_reference(endpoint, capability=capability, port=port, bind_host=bind, advertise_host=bind)
    print(engine.engine_target(), flush=True)
    try:
        engine._stop.wait()
    except KeyboardInterrupt:
        engine.stop()


if __name__ == "__main__":
    main()
