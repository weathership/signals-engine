"""Private vLLM chat stream. Only the hosting engine imports this.

The lattice never sees the URL. Callers get OIP stream frames: a progress
frame, reasoning and answer deltas, then one final frame. ``thinking`` aligns
to effort ``xhigh``; ``instruct`` stays thinking-on at effort ``low``.
"""

from __future__ import annotations

import json
import os
import socket
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from http.client import HTTPConnection, HTTPException, HTTPSConnection, HTTPResponse
from urllib.parse import urlparse

from signals_engine.types import CompletionRequest, HandlerError, StreamFrame

import grpc

_STALL_GURU = "#VLLM.00000005.STALLED"
_DEFAULT_MODEL = "Qwen/Qwen3.8-27B"

# capabilities.md operating profiles. The hosting engine applies these.
# The forwarder does not.
_PROFILES: dict[str, dict[str, object]] = {
    "thinking": {
        "enable_thinking": True,
        "preserve_thinking": True,
        "reasoning_effort": "xhigh",
    },
    "instruct": {
        "enable_thinking": True,
        "preserve_thinking": True,
        "reasoning_effort": "low",
    },
}


@dataclass(frozen=True)
class VllmEndpoint:
    """OpenAI-compatible base, for example ``http://127.0.0.1:8081/v1``."""

    base_url: str
    model: str = ""
    api_key: str = ""
    stall_s: float = 120.0


def from_env() -> VllmEndpoint | None:
    """``SIGNALS_VLLM_BASE_URL`` plus optional model, key, and stall seconds."""
    base = (os.environ.get("SIGNALS_VLLM_BASE_URL") or "").strip()
    if not base:
        return None
    stall_raw = (os.environ.get("SIGNALS_VLLM_STALL_S") or "120").strip()
    try:
        stall = float(stall_raw)
    except ValueError:
        stall = 120.0
    return VllmEndpoint(
        base_url=base,
        model=(os.environ.get("SIGNALS_VLLM_MODEL") or "").strip(),
        api_key=(os.environ.get("SIGNALS_VLLM_API_KEY") or "").strip(),
        stall_s=stall,
    )


def _split(base_url: str) -> tuple[str, int, str, bool]:
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise HandlerError(
            grpc.StatusCode.INVALID_ARGUMENT,
            "vLLM base URL must be http(s)://host:port/v1",
        )
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path.rstrip("/")
    if not path.endswith("/v1"):
        path = f"{path}/v1"
    return parsed.hostname, port, path, parsed.scheme == "https"


def _connect(endpoint: VllmEndpoint, timeout: float) -> HTTPConnection:
    host, port, _path, tls = _split(endpoint.base_url)
    if tls:
        return HTTPSConnection(host, port, timeout=timeout)
    return HTTPConnection(host, port, timeout=timeout)


def _headers(endpoint: VllmEndpoint) -> dict[str, str]:
    headers = {"Content-Type": "application/json", "Accept": "text/event-stream"}
    if endpoint.api_key:
        headers["Authorization"] = f"Bearer {endpoint.api_key}"
    return headers


def _exchange(
    endpoint: VllmEndpoint,
    suffix: str,
    *,
    method: str = "GET",
    body: bytes | None = None,
    timeout: float = 5.0,
) -> tuple[int, bytes]:
    _host, _port, path, _tls = _split(endpoint.base_url)
    conn = _connect(endpoint, timeout)
    try:
        conn.request(method, f"{path}{suffix}", body=body, headers=_headers(endpoint))
        response = conn.getresponse()
        return response.status, response.read()
    finally:
        conn.close()


def ready(endpoint: VllmEndpoint, *, timeout: float = 2.0) -> bool:
    """True when ``GET /v1/models`` answers. The URL stays in this process."""
    try:
        status, _body = _exchange(endpoint, "/models", timeout=timeout)
        return status < 400
    except (OSError, HTTPException, TimeoutError):
        return False


def resolved_model(endpoint: VllmEndpoint) -> str:
    if endpoint.model:
        return endpoint.model
    try:
        status, raw = _exchange(endpoint, "/models", timeout=2.0)
        if status < 400:
            doc = json.loads(raw.decode("utf-8") or "{}")
            data = doc.get("data") if isinstance(doc, dict) else None
            if isinstance(data, list) and data and isinstance(data[0], dict):
                name = str(data[0].get("id") or "")
                if name:
                    return name
    except (OSError, HTTPException, TimeoutError, json.JSONDecodeError, UnicodeError):
        pass
    return _DEFAULT_MODEL


def _messages(request: CompletionRequest) -> list[dict[str, str]]:
    if request.messages_json:
        try:
            parsed = json.loads(request.messages_json)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
    messages: list[dict[str, str]] = []
    if request.system_prompt:
        messages.append({"role": "system", "content": request.system_prompt})
    messages.append({"role": "user", "content": request.prompt})
    return messages


def _profile(capability: str, *, json_schema: str) -> dict[str, object]:
    profile = dict(_PROFILES.get(capability, _PROFILES["thinking"]))
    if json_schema:
        profile["enable_thinking"] = False
    return profile


def iter_chat(
    endpoint: VllmEndpoint,
    request: CompletionRequest,
    cancel: threading.Event,
    *,
    capability: str,
) -> Iterator[StreamFrame]:
    """Stream one completion. Return without a final frame when ``cancel`` is set."""
    model = resolved_model(endpoint)
    profile = _profile(capability or "thinking", json_schema=request.json_schema)
    yield StreamFrame(kind="progress", phase="generating", detail=model, model=model)
    payload = {
        "model": model,
        "messages": _messages(request),
        "temperature": request.temperature,
        "max_tokens": request.max_tokens,
        "stream": True,
        "stream_options": {"include_usage": True},
        "chat_template_kwargs": {
            "enable_thinking": profile["enable_thinking"],
            "preserve_thinking": profile["preserve_thinking"],
        },
        "reasoning_effort": profile["reasoning_effort"],
    }
    body = json.dumps(payload).encode("utf-8")
    conn = _connect(endpoint, endpoint.stall_s)
    done = threading.Event()

    def _close_on_cancel() -> None:
        while not done.wait(0.05):
            if cancel.is_set():
                try:
                    conn.close()
                except OSError:
                    pass
                return

    threading.Thread(target=_close_on_cancel, name="signals-vllm-cancel", daemon=True).start()
    text: list[str] = []
    reasoning: list[str] = []
    finish = ""
    prompt_tokens = 0
    completion_tokens = 0
    try:
        _host, _port, path, _tls = _split(endpoint.base_url)
        conn.request("POST", f"{path}/chat/completions", body=body, headers=_headers(endpoint))
        response = conn.getresponse()
        if response.status >= 400:
            err = response.read(800).decode("utf-8", errors="replace")
            raise HandlerError(
                grpc.StatusCode.UNAVAILABLE,
                f"vLLM HTTP {response.status}: {err}",
            )
        for line in _lines(response):
            if cancel.is_set():
                return
            piece = line.strip()
            if not piece.startswith("data:"):
                continue
            data = piece[len("data:") :].strip()
            if not data or data == "[DONE]":
                continue
            try:
                chunk = json.loads(data)
            except json.JSONDecodeError:
                continue
            usage = chunk.get("usage") if isinstance(chunk, dict) else None
            if isinstance(usage, dict):
                prompt_tokens = int(usage.get("prompt_tokens") or prompt_tokens)
                completion_tokens = int(usage.get("completion_tokens") or completion_tokens)
            for choice in chunk.get("choices") or []:
                if choice.get("finish_reason"):
                    finish = str(choice["finish_reason"])
                delta = choice.get("delta") or {}
                thought = delta.get("reasoning_content") or delta.get("reasoning") or ""
                answer = delta.get("content") or ""
                if thought:
                    reasoning.append(thought)
                    yield StreamFrame(kind="reasoning_delta", text=thought, model=model)
                if answer:
                    text.append(answer)
                    yield StreamFrame(kind="text_delta", text=answer, model=model)
                if cancel.is_set():
                    return
        if cancel.is_set():
            return
        yield StreamFrame(
            kind="final",
            text="".join(text),
            reasoning="".join(reasoning),
            finish_reason=finish or "stop",
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            fulfilled_by=model,
        )
    except socket.timeout as exc:
        if cancel.is_set():
            return
        raise HandlerError(
            grpc.StatusCode.UNAVAILABLE,
            f"Generation STALLED: no tokens for {endpoint.stall_s:.0f}s. Guru: {_STALL_GURU}",
        ) from exc
    except HandlerError:
        raise
    except (OSError, HTTPException, AttributeError):
        # Closing the socket under a chunked readline nulls the response
        # buffer. That is the cancel path, not a failed generation.
        if cancel.is_set():
            return
        raise
    finally:
        done.set()
        try:
            conn.close()
        except OSError:
            pass


def _lines(response: HTTPResponse) -> Iterator[str]:
    while True:
        raw = response.readline()
        if not raw:
            return
        yield raw.decode("utf-8", errors="replace")
