"""OpenLineage RunEvents go to Signals Atlas. No private catalog."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

import grpc

from signals_engine.wire.zndx.engine.v1 import engine_pb2

GURU_NOATLAS = "#LN.00000001.NOATLAS"
_DEFAULT_URL = "http://127.0.0.1:21010/api/v1/lineage"


def lineage_url(override: str | None = None) -> str:
    raw = (override if override is not None else os.environ.get("SIGNALS_ATLAS_OL_URL") or _DEFAULT_URL)
    url = raw.strip().rstrip("/")
    if not url.endswith("/lineage"):
        url = url + "/lineage"
    return url


def record_lineage(request: engine_pb2.LineageRequest, context: grpc.ServicerContext, url: str):
    raw = (request.event_json or "").strip()
    if not raw:
        context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            f"{GURU_NOATLAS} LineageRequest.event_json is empty.",
        )
    try:
        body = json.loads(raw)
    except json.JSONDecodeError as exc:
        context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            f"{GURU_NOATLAS} event_json is not JSON: {exc}",
        )
    want = (request.event_type or "").strip().upper()
    got = str(body.get("eventType") or "").upper()
    if want and got and want != got:
        context.abort(
            grpc.StatusCode.INVALID_ARGUMENT,
            f"{GURU_NOATLAS} event_type={want!r} != event_json.eventType={got!r}",
        )
    payload = json.dumps(body).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            if resp.status >= 400:
                snippet = resp.read()[:300].decode("utf-8", errors="replace")
                return engine_pb2.LineageResponse(
                    accepted=False,
                    error=f"{GURU_NOATLAS} Atlas HTTP {resp.status}: {snippet}",
                )
    except urllib.error.HTTPError as exc:
        snippet = exc.read()[:300].decode("utf-8", errors="replace")
        return engine_pb2.LineageResponse(
            accepted=False,
            error=f"{GURU_NOATLAS} Atlas HTTP {exc.code}: {snippet}",
        )
    except Exception as exc:
        return engine_pb2.LineageResponse(
            accepted=False,
            error=f"{GURU_NOATLAS} Atlas POST {url} failed: {exc}",
        )
    return engine_pb2.LineageResponse(accepted=True, error="")
