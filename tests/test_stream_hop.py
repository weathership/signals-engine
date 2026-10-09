"""Sample client → sample engine → reference engine → fixture vLLM.

The fixture speaks the OpenAI chunked stream. No lab GPU process is dialed.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
import pytest

from signals_engine.client import status
from signals_engine.reference import serve_reference
from signals_engine.sample import SampleClient, serve_sample
from signals_engine.types import CompletionRequest
from signals_engine.vllm import VllmEndpoint


class _Vllm(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args) -> None:
        return

    def _json(self, doc: dict) -> None:
        body = json.dumps(doc).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path.rsplit("?", 1)[0].endswith("/models"):
            self._json({"data": [{"id": "fixture-model"}]})
            return
        self.send_error(404)

    def _chunk(self, payload: bytes) -> None:
        self.wfile.write(f"{len(payload):X}\r\n".encode() + payload + b"\r\n")
        self.wfile.flush()
        self.server.chunks += 1

    def _event(self, doc: dict) -> None:
        self._chunk(b"data: " + json.dumps(doc).encode() + b"\n\n")

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        doc = json.loads(self.rfile.read(length).decode() or "{}")
        self.server.payloads.append(doc)
        prompt = ""
        for message in doc.get("messages") or []:
            if message.get("role") == "user":
                prompt = str(message.get("content") or "")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        try:
            if "SLOW" in prompt:
                for index in range(30):
                    time.sleep(0.2)
                    self._event({"model": "fixture-model", "choices": [{"delta": {"reasoning": f"r{index}"}}]})
                self._event({"model": "fixture-model", "choices": [{"delta": {}, "finish_reason": "stop"}]})
            elif prompt == "two":
                self._event({"model": "fixture-model", "choices": [{"delta": {"content": "beta"}, "finish_reason": "stop"}]})
                self._event({"model": "fixture-model", "usage": {"prompt_tokens": 1, "completion_tokens": 1}, "choices": []})
            else:
                self._event({"model": "fixture-model", "choices": [{"delta": {"reasoning": "Think"}}]})
                self._event({"model": "fixture-model", "choices": [{"delta": {"reasoning": " more"}}]})
                self._event({"model": "fixture-model", "choices": [{"delta": {"content": "Answer"}}]})
                self._event(
                    {
                        "model": "fixture-model",
                        "choices": [{"delta": {"content": " here"}, "finish_reason": "stop"}],
                        "usage": {"prompt_tokens": 3, "completion_tokens": 4},
                    }
                )
            self._chunk(b"data: [DONE]\n\n")
            self.wfile.write(b"0\r\n\r\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            return


@pytest.fixture
def hop(monkeypatch):
    for name in (
        "SIGNALS_ENGINE_TARGET",
        "SIGNALS_PEER_CONTRACT",
        "SIGNALS_ATLAS_OL_URL",
        "SIGNALS_ADVERTISE_HOST",
        "SIGNALS_VLLM_BASE_URL",
        "SIGNALS_UPSTREAM",
    ):
        monkeypatch.delenv(name, raising=False)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Vllm)
    httpd.payloads = []
    httpd.chunks = 0
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    port = httpd.server_address[1]
    endpoint = VllmEndpoint(
        base_url=f"http://127.0.0.1:{port}/v1",
        model="fixture-model",
        stall_s=5,
    )
    reference = serve_reference(endpoint)
    sample = serve_sample((reference.engine_target(),))
    try:
        yield httpd, reference, sample
    finally:
        sample.stop()
        reference.stop()
        httpd.shutdown()
        httpd.server_close()


def _until_final(session, timeout: float = 5.0) -> list:
    frames = []
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(f"no final frame in {frames}")
        frame = session.next_frame(timeout=remaining)
        frames.append(frame)
        if frame.kind == "final":
            return frames


def test_sample_client_streams_thinking_through_the_reference(hop) -> None:
    httpd, reference, sample = hop
    reference_status = status(reference.engine_target())
    sample_status = status(sample.engine_target())
    assert [endpoint.capability for endpoint in reference_status.endpoints] == ["thinking"]
    assert reference_status.endpoints[0].healthy
    assert reference_status.endpoints[0].model == "fixture-model"
    assert reference_status.endpoints[0].detail == ""
    assert "http" not in reference_status.endpoints[0].detail
    assert [endpoint.capability for endpoint in sample_status.endpoints] == []

    session = SampleClient(sample.engine_target()).open(
        CompletionRequest(capability="thinking", prompt="one", max_tokens=32, temperature=0.7)
    )
    try:
        frames = _until_final(session)
    finally:
        session.close()

    assert [frame.kind for frame in frames] == [
        "progress",
        "progress",
        "reasoning_delta",
        "reasoning_delta",
        "text_delta",
        "text_delta",
        "final",
    ]
    assert frames[0].phase == "routing"
    assert frames[0].detail == reference.engine_target()
    assert frames[1].phase == "generating"
    assert frames[1].detail == "fixture-model"
    assert "".join(frame.text for frame in frames if frame.kind == "reasoning_delta") == "Think more"
    assert frames[-1].text == "Answer here"
    assert frames[-1].reasoning == "Think more"
    assert frames[-1].finish_reason == "stop"
    assert frames[-1].prompt_tokens == 3
    assert frames[-1].completion_tokens == 4
    payload = httpd.payloads[-1]
    assert payload["model"] == "fixture-model"
    assert payload["stream"] is True
    assert payload["reasoning_effort"] == "xhigh"
    assert payload["chat_template_kwargs"]["enable_thinking"] is True
    assert "127.0.0.1:" + str(httpd.server_address[1]) not in frames[0].detail


def test_follow_up_on_the_same_stream_starts_a_new_generation(hop) -> None:
    _httpd, _reference, sample = hop
    session = SampleClient(sample.engine_target()).open(
        CompletionRequest(capability="thinking", prompt="one", max_tokens=16)
    )
    try:
        first = _until_final(session)
        assert first[-1].text == "Answer here"
        session.send(CompletionRequest(capability="thinking", prompt="two", max_tokens=16))
        second = _until_final(session)
    finally:
        session.close()
    assert second[-1].text == "beta"
    assert second[-1].finish_reason == "stop"
    assert second[0].phase == "generating"


def test_cancel_stops_the_reference_stream(hop) -> None:
    httpd, _reference, sample = hop
    session = SampleClient(sample.engine_target()).open(
        CompletionRequest(capability="thinking", prompt="SLOW", max_tokens=16)
    )
    started = time.monotonic()
    try:
        while True:
            frame = session.next_frame(timeout=5)
            if frame.kind == "reasoning_delta":
                break
        session.cancel()
        rest = [frame]
        rest.extend(_until_final(session, timeout=3))
    finally:
        session.close()
    assert rest[-1].kind == "final"
    assert rest[-1].finish_reason == "cancelled"
    assert time.monotonic() - started < 3
    assert httpd.chunks < 30


def test_a_new_prompt_supersedes_the_in_flight_generation(hop) -> None:
    _httpd, _reference, sample = hop
    session = SampleClient(sample.engine_target()).open(
        CompletionRequest(capability="thinking", prompt="SLOW", max_tokens=16)
    )
    try:
        while True:
            frame = session.next_frame(timeout=5)
            if frame.kind == "reasoning_delta":
                break
        session.send(CompletionRequest(capability="thinking", prompt="two", max_tokens=16))
        stopped = _until_final(session, timeout=3)
        followed = _until_final(session, timeout=5)
    finally:
        session.close()
    assert stopped[-1].finish_reason == "superseded"
    assert followed[-1].text == "beta"
    assert followed[-1].finish_reason == "stop"


def test_missing_capability_names_the_peers_it_asked(hop) -> None:
    _httpd, reference, sample = hop
    session = SampleClient(sample.engine_target()).open(
        CompletionRequest(capability="instruct", prompt="one", max_tokens=16)
    )
    try:
        with pytest.raises(grpc.RpcError) as caught:
            session.next_frame(timeout=5)
    finally:
        session.close()
    assert caught.value.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "instruct" in caught.value.details()
    assert reference.engine_target() in caught.value.details()
