"""Capability routing and an untouched ModelStreamInfer relay.

Seeds are pairwise. Status names the capability. ModelReady confirms it.
The target host:port is dialed as advertised.
"""

from __future__ import annotations

from collections.abc import Iterator

import grpc

from signals_engine.client import channel, model_ready, status
from signals_engine.types import HandlerError
from signals_engine.wire.inference import open_inference_grpc_pb2_grpc as oip_grpc


def select_peer(targets: list[str], capability: str, *, timeout: float = 3.0) -> str:
    """First seed that hosts ``capability`` and answers ModelReady.

    Raises ``HandlerError(FAILED_PRECONDITION)`` naming every target asked.
    """
    asked: list[str] = []
    for raw in targets:
        target = raw.replace("grpc://", "").strip()
        if not target or target in asked:
            continue
        asked.append(target)
        try:
            snapshot = status(target, timeout=timeout)
        except grpc.RpcError:
            continue
        hosted = any(
            endpoint.capability == capability and endpoint.healthy
            for endpoint in snapshot.endpoints
        )
        if not hosted:
            continue
        if not model_ready(target, capability, timeout=timeout):
            continue
        return target
    names = ", ".join(asked) or "none"
    raise HandlerError(
        grpc.StatusCode.FAILED_PRECONDITION,
        f"no peer hosts {capability} (asked {names})",
    )


def relay_model_stream(target: str, requests: Iterator[object]) -> Iterator[object]:
    """Copy ModelStreamInfer to ``target``. Frames are not rewritten."""
    ch = channel(target)
    try:
        call = oip_grpc.GRPCInferenceServiceStub(ch).ModelStreamInfer(requests)
        for response in call:
            yield response
    finally:
        ch.close()
