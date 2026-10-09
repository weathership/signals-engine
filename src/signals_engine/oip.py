"""KServe Open Inference Protocol on the engine port.

The tensor messages are OIP (``inference.ModelInferRequest`` /
``ModelInferResponse``). ``zndx.engine.v1.Complete`` is the capability
convenience over the same host handler. A project that serves a model
registers that handler once; both wires call it.

``ModelStreamInfer`` is the federation full-duplex RPC over those same
messages (mbengine). A unary KServe or CIS peer remains callable: the client
below falls back to ``ModelInfer`` when the stream is ``UNIMPLEMENTED``.

Tensor names shared with the engines already on the lattice:

- inputs ``prompt``, ``system_prompt``, and (llm_tools_v1) ``tools``, ``messages``
- outputs ``completion``, ``reasoning_content``, and ``tool_calls`` when present
- parameters ``max_tokens``, ``temperature``, ``json_schema``, ``capability``
"""

from __future__ import annotations

import json
from typing import Any

from signals_engine.types import Completion, CompletionRequest, StreamFrame, ToolCall
from signals_engine.wire.inference import open_inference_grpc_pb2 as oip_pb

T_PROMPT = "prompt"
T_SYSTEM = "system_prompt"
T_TOOLS = "tools"
T_MESSAGES = "messages"
T_COMPLETION = "completion"
T_REASONING = "reasoning_content"
T_TOOL_CALLS = "tool_calls"
T_TEXT_DELTA = "text_delta"
T_REASONING_DELTA = "reasoning_delta"
T_PROGRESS = "progress"
CONTROL_CANCEL = "cancel"

EXTENSION_ENGINE = "zndx.engine.v1.Engine"
EXTENSION_STREAM = "model_stream_infer"
EXTENSION_TOOLS = "llm_tools_v1"

_UNSET_MAX_TOKENS = 2048
_UNSET_TEMPERATURE = 0.7


def _bytes_at(request: Any, index: int, tensor: Any) -> str:
    if tensor.contents.bytes_contents:
        return tensor.contents.bytes_contents[0].decode("utf-8")
    raws = request.raw_input_contents
    if index >= len(raws):
        return ""
    raw = raws[index]
    if len(raw) >= 4:
        size = int.from_bytes(raw[:4], "little")
        if 4 + size <= len(raw):
            return raw[4 : 4 + size].decode("utf-8")
    return raw.decode("utf-8", errors="replace")


def _param(request: Any, key: str) -> tuple[str, Any] | None:
    if key not in request.parameters:
        return None
    value = request.parameters[key]
    which = value.WhichOneof("parameter_choice")
    if not which:
        return None
    return which, getattr(value, which)


def completion_request_from_oip(request: Any) -> CompletionRequest:
    fields: dict[str, str] = {}
    for index, tensor in enumerate(request.inputs):
        fields[tensor.name] = _bytes_at(request, index, tensor)
    max_tokens = _UNSET_MAX_TOKENS
    temperature = _UNSET_TEMPERATURE
    json_schema = ""
    capability = ""
    got = _param(request, "max_tokens")
    if got and got[0] == "int64_param":
        max_tokens = int(got[1]) or _UNSET_MAX_TOKENS
    got = _param(request, "temperature")
    if got and got[0] == "double_param":
        temperature = float(got[1])
    elif got and got[0] == "string_param":
        try:
            temperature = float(got[1])
        except ValueError:
            temperature = _UNSET_TEMPERATURE
    got = _param(request, "json_schema")
    if got and got[0] == "string_param":
        json_schema = str(got[1])
    got = _param(request, "capability")
    if got and got[0] == "string_param":
        capability = str(got[1])
    model_name = (request.model_name or "").strip()
    if not capability:
        capability = model_name
    return CompletionRequest(
        capability=capability,
        prompt=fields.get(T_PROMPT, ""),
        system_prompt=fields.get(T_SYSTEM, ""),
        max_tokens=max_tokens,
        temperature=temperature,
        json_schema=json_schema,
        tools_json=fields.get(T_TOOLS, ""),
        messages_json=fields.get(T_MESSAGES, ""),
    )


def completion_request_from_complete(request: Any) -> CompletionRequest:
    caps = tuple(item for item in request.capabilities if item)
    return CompletionRequest(
        capability=request.capability or (caps[0] if caps else ""),
        prompt=request.prompt or "",
        system_prompt=request.system_prompt or "",
        max_tokens=int(request.max_tokens or 0) or _UNSET_MAX_TOKENS,
        temperature=float(request.temperature or 0) or _UNSET_TEMPERATURE,
        json_schema=request.json_schema or "",
        tools_json=request.tools_json or "",
        messages_json=request.messages_json or "",
        tool_choice=request.tool_choice or "",
        capabilities=caps,
        timezone=request.timezone or "",
        clock_json=request.clock_json or "",
    )


def _tool_json(calls: tuple[ToolCall, ...]) -> str:
    return json.dumps(
        [
            {"id": call.id, "name": call.name, "arguments_json": call.arguments_json}
            for call in calls
        ]
    )


def apply_completion(response: Any, result: Completion, *, request_id: str, model_name: str) -> None:
    """Fill a ModelInferResponse from a host Completion."""
    response.model_name = result.model or model_name
    response.model_version = "v1"
    response.id = request_id or ""

    def _add(name: str, text: str) -> None:
        out = response.outputs.add()
        out.name = name
        out.datatype = "BYTES"
        out.shape.extend([1])
        out.contents.bytes_contents.append(text.encode("utf-8"))

    _add(T_COMPLETION, result.text or "")
    if result.reasoning_content:
        _add(T_REASONING, result.reasoning_content)
    if result.tool_calls:
        _add(T_TOOL_CALLS, _tool_json(result.tool_calls))
    response.parameters["tokens_used"].int64_param = int(result.completion_tokens)
    response.parameters["prompt_tokens"].int64_param = int(result.prompt_tokens)
    response.parameters["latency_ms"].double_param = float(result.latency_ms)
    if result.model:
        response.parameters["model"].string_param = result.model
    response.parameters["finish_reason"].string_param = result.finish_reason or "stop"
    if result.fulfilled_by:
        response.parameters["backend"].string_param = result.fulfilled_by


def completion_from_oip(response: Any) -> Completion:
    text = ""
    reasoning = ""
    calls: tuple[ToolCall, ...] = ()
    for out in response.outputs:
        raw = out.contents.bytes_contents[0] if out.contents.bytes_contents else b""
        value = raw.decode("utf-8") if raw else ""
        if out.name == T_COMPLETION:
            text = value
        elif out.name in (T_REASONING, "reasoning"):
            reasoning = value
        elif out.name == T_TOOL_CALLS and value:
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                parsed = []
            if isinstance(parsed, list):
                calls = tuple(
                    ToolCall(
                        name=str(item.get("name") or ""),
                        arguments_json=str(item.get("arguments_json") or ""),
                        id=str(item.get("id") or ""),
                    )
                    for item in parsed
                    if isinstance(item, dict)
                )
    model = ""
    finish = "stop"
    prompt_tokens = completion_tokens = 0
    latency = 0.0
    fulfilled = ""
    got = _param(response, "model")
    if got and got[0] == "string_param":
        model = str(got[1])
    got = _param(response, "finish_reason")
    if got and got[0] == "string_param":
        finish = str(got[1]) or "stop"
    got = _param(response, "prompt_tokens")
    if got and got[0] == "int64_param":
        prompt_tokens = int(got[1])
    got = _param(response, "tokens_used")
    if got and got[0] == "int64_param":
        completion_tokens = int(got[1])
    got = _param(response, "latency_ms")
    if got and got[0] == "double_param":
        latency = float(got[1])
    got = _param(response, "backend")
    if got and got[0] == "string_param":
        fulfilled = str(got[1])
    return Completion(
        text=text,
        model=model or response.model_name or "",
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        latency_ms=latency,
        reasoning_content=reasoning,
        finish_reason=finish,
        tool_calls=calls,
        fulfilled_by=fulfilled,
    )


def build_infer_request(req: CompletionRequest, *, request_id: str = "") -> Any:
    message = oip_pb.ModelInferRequest(
        model_name=req.capability or "complete",
        id=request_id,
    )

    def _add(name: str, text: str) -> None:
        tensor = message.inputs.add()
        tensor.name = name
        tensor.datatype = "BYTES"
        tensor.shape.extend([1])
        tensor.contents.bytes_contents.append(text.encode("utf-8"))

    _add(T_PROMPT, req.prompt)
    if req.system_prompt:
        _add(T_SYSTEM, req.system_prompt)
    if req.tools_json:
        _add(T_TOOLS, req.tools_json)
    if req.messages_json:
        _add(T_MESSAGES, req.messages_json)
    message.parameters["max_tokens"].int64_param = int(req.max_tokens)
    message.parameters["temperature"].double_param = float(req.temperature)
    if req.json_schema:
        message.parameters["json_schema"].string_param = req.json_schema
    if req.capability:
        message.parameters["capability"].string_param = req.capability
    return message


def metadata_extensions(*, tools: bool) -> list[str]:
    names = [EXTENSION_ENGINE, EXTENSION_STREAM]
    if tools:
        names.append(EXTENSION_TOOLS)
    return names


def input_names(*, tools: bool) -> tuple[str, ...]:
    names = [T_PROMPT, T_SYSTEM]
    if tools:
        names.extend((T_TOOLS, T_MESSAGES))
    return tuple(names)


def output_names(*, tools: bool) -> tuple[str, ...]:
    names = [T_COMPLETION, T_REASONING, T_TEXT_DELTA, T_REASONING_DELTA, T_PROGRESS]
    if tools:
        names.append(T_TOOL_CALLS)
    return tuple(names)


def is_cancel(request: Any) -> bool:
    got = _param(request, "control")
    return bool(got and got[0] == "string_param" and str(got[1]) == CONTROL_CANCEL)


def progress_json(phase: str, detail: str) -> str:
    return json.dumps({"phase": phase, "detail": detail}, separators=(",", ":"))


def _tensor_text(tensor: Any) -> str:
    raw = tensor.contents.bytes_contents[0] if tensor.contents.bytes_contents else b""
    return raw.decode("utf-8") if raw else ""


def _add_bytes(response: Any, name: str, text: str) -> None:
    out = response.outputs.add()
    out.name = name
    out.datatype = "BYTES"
    out.shape.extend([1])
    out.contents.bytes_contents.append((text or "").encode("utf-8"))


def frame_to_response(frame: StreamFrame, *, request_id: str, capability: str) -> Any:
    """One ModelInferResponse for a stream frame. One tensor, except the final frame."""
    response = oip_pb.ModelInferResponse(
        model_name=frame.model or capability,
        model_version="v1",
        id=request_id or "",
    )
    if frame.kind == "progress":
        _add_bytes(response, T_PROGRESS, progress_json(frame.phase, frame.detail))
    elif frame.kind == "reasoning_delta":
        _add_bytes(response, T_REASONING_DELTA, frame.text)
    elif frame.kind == "text_delta":
        _add_bytes(response, T_TEXT_DELTA, frame.text)
    else:
        _add_bytes(response, T_COMPLETION, frame.text)
        if frame.reasoning:
            _add_bytes(response, T_REASONING, frame.reasoning)
        response.parameters["finish_reason"].string_param = frame.finish_reason or "stop"
        if frame.prompt_tokens:
            response.parameters["prompt_tokens"].int64_param = int(frame.prompt_tokens)
        if frame.completion_tokens:
            response.parameters["tokens_used"].int64_param = int(frame.completion_tokens)
        if frame.latency_ms:
            response.parameters["latency_ms"].double_param = float(frame.latency_ms)
    if capability:
        response.parameters["capability"].string_param = capability
    if frame.model:
        response.parameters["model"].string_param = frame.model
    if frame.fulfilled_by:
        response.parameters["backend"].string_param = frame.fulfilled_by
    return response


def frame_from_response(response: Any) -> StreamFrame:
    """Read one relayed ModelInferResponse. Unknown tensors are left in place by callers."""
    fields: dict[str, str] = {out.name: _tensor_text(out) for out in response.outputs}
    model = ""
    finish = ""
    prompt_tokens = completion_tokens = 0
    latency = 0.0
    fulfilled = ""
    got = _param(response, "model")
    if got and got[0] == "string_param":
        model = str(got[1])
    got = _param(response, "finish_reason")
    if got and got[0] == "string_param":
        finish = str(got[1])
    got = _param(response, "prompt_tokens")
    if got and got[0] == "int64_param":
        prompt_tokens = int(got[1])
    got = _param(response, "tokens_used")
    if got and got[0] == "int64_param":
        completion_tokens = int(got[1])
    got = _param(response, "latency_ms")
    if got and got[0] == "double_param":
        latency = float(got[1])
    got = _param(response, "backend")
    if got and got[0] == "string_param":
        fulfilled = str(got[1])
    model = model or response.model_name or ""
    if T_PROGRESS in fields:
        phase = ""
        detail = fields[T_PROGRESS]
        try:
            doc = json.loads(fields[T_PROGRESS])
        except json.JSONDecodeError:
            doc = None
        if isinstance(doc, dict):
            phase = str(doc.get("phase") or "")
            detail = str(doc.get("detail") or "")
        return StreamFrame(kind="progress", phase=phase, detail=detail, model=model, fulfilled_by=fulfilled)
    if T_REASONING_DELTA in fields:
        return StreamFrame(kind="reasoning_delta", text=fields[T_REASONING_DELTA], model=model)
    if T_TEXT_DELTA in fields:
        return StreamFrame(kind="text_delta", text=fields[T_TEXT_DELTA], model=model)
    return StreamFrame(
        kind="final",
        text=fields.get(T_COMPLETION, ""),
        reasoning=fields.get(T_REASONING) or fields.get("reasoning") or "",
        finish_reason=finish or "stop",
        model=model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        latency_ms=latency,
        fulfilled_by=fulfilled,
    )


