# signals-engine

Apache-2.0 lattice library. A peer imports this package and registers its own
product servicers beside one shared face:

- `zndx.engine.v1.Engine` from the
  [`signals-protocol`](components/signals-protocol) commit this package pins
- KServe Open Inference Protocol messages (`inference.GRPCInferenceService`)

The platform scheduler on `:50551` stays in
[weathership/signals](https://github.com/weathership/signals). This repository
is what a new project depends on, and what an existing project (Semantica
first, then Metabase's `mbengine`) uses to join the peer lattice. Those
projects import the protocol version this package publishes. They do not
vendor a second `signals-protocol` checkout.

## Protocol version

`signals-protocol` stays the only text of the `zndx` protos. This repo drives
that pin alongside the engine and ships the snapshot:

- `signals_engine.PROTOCOL_SHA` — submodule commit the bytes were copied from
- `signals_engine.proto_root()` — `protoc -I` directory (`zndx/` plus
  `inference/open_inference_grpc.proto`)
- `signals_engine.wire` — Python for `zndx.engine.v1`, `zndx.scheduler.v1`,
  `zndx.agent.v1`, `zndx.supervision.v1`, and `inference`

This process registers Engine and OIP. Scheduler, agent, and supervision
stubs are on the snapshot for a host that implements those services itself.
The Signals scheduler remains the process on `:50551`.

Wire changes are additive inside a version. A break is a new package.

1. Edit `components/signals-protocol` and commit it on `trunk`.
2. Point this repo's gitlink at that commit.
3. Run `just gen`. That refreshes the packaged protos, the stubs, and
   `PROTOCOL_SHA` together.
4. Commit the gitlink and the generated tree in this repo, and push them in
   the same slice as the protocol trunk push. This pin tracks trunk. It does
   not lag on a side branch.

The OIP proto is `inference/open_inference_grpc.proto` inside that same
commit. `OIP_SHA256` names its bytes. The stream hop — capability routing,
deltas, progress, cancel — is `specification/protocol/oip_stream.md` in the
pin. Specs stay in `components/signals-protocol/specification` at
`PROTOCOL_SHA`.

## What a host supplies

`LatticeEngine` needs a `PeerIdentity` (`project`, listen port, advertise
host) and optional hooks:

- endpoint and surface probes for `Status` (healthy only when the backend is up)
- one completion handler, if this project serves a model
- a yield handler for sentinel workloads
- extra `ServerQuery` kinds and product RPCs (`Remediate`, `PutAgendaItem`,
  `WatchWorkload`)
- native gRPC servicers on the same port

The library always enables server reflection, sets `grpc.so_reuseport=0`, and
treats a failed bind as fatal. `Complete`, `ModelInfer`, and `ModelStreamInfer`
are `UNIMPLEMENTED` until a completion handler is registered. `RecordLineage`
posts OpenLineage RunEvents to Signals Atlas. `Announce` is `UNIMPLEMENTED`
unless this process is a directory (`directory=True`). A joining peer sets
`announce_to_directory=True` and heartbeats `PeerAnnounce` at the directory
seeds (`directory_targets`, `SIGNALS_ENGINE_TARGET`, then the peer contract).

## OIP

Model projects speak signals-protocol and use OIP message types inside this
engine. The host handler sees one `CompletionRequest`. `Engine/Complete` and
`ModelInfer` both call it. `ModelStreamInfer` carries the same
`ModelInferRequest` / `ModelInferResponse` messages; a unary KServe or CIS
peer is still callable — `model_stream` falls back to `ModelInfer` when the
stream is `UNIMPLEMENTED`.

The OIP proto in this snapshot is the KServe OIP v2 message set peers already share, plus
the federation `ModelStreamInfer` RPC (no new tensor schema). Input tensors
are `prompt`, `system_prompt`, and, when a model handler is registered,
`tools` and `messages` (`llm_tools_v1`). Outputs are `completion`,
`reasoning_content`, and `tool_calls`.

## Server-to-server

`ServerQuery` follows Matrix federation Queries: one request, one response,
between a pair of servers, not gossip and not a room DAG
([Server-Server API](https://spec.matrix.org/latest/server-server-api/)).
`Announce` is a join hint with a TTL. It is process-local and is not written
to Atlas — the ephemeral half of that model, not a Matrix `/send` transaction.
This process is not a Matrix homeserver.

`PEERS` is the contract's same-box `advertise-host:port` rows plus the
directory's live announces. A `PeerHint.target` from another host is stored
as given.

## Local stream hop

Gaius still carries its own protocol pin, so local work talks to vLLM
directly. The reference engine hosts `thinking` and dials
`SIGNALS_VLLM_BASE_URL` (lab: `http://127.0.0.1:8081/v1`). The sample engine
does not advertise `thinking`. It chooses the reference with `Status` and
`ModelReady`, then relays `ModelStreamInfer`. `SampleClient` speaks only to
the sample engine.

```bash
SIGNALS_VLLM_BASE_URL=http://127.0.0.1:8081/v1 python -m signals_engine.reference
SIGNALS_UPSTREAM=127.0.0.1:<reference port> python -m signals_engine.sample
```

The test suite runs that hop against a fixture vLLM, not the lab GPU process.

## Install

Semantica and later peers pin the release tag. The wheel carries the proto
snapshot and the generated stubs, so the import does not need a second
`signals-protocol` checkout.

```bash
uv add "signals-engine @ git+https://github.com/weathership/signals-engine.git@v0.2.0"
```

v0.2.0 matches Semantica's runtime: `grpcio` and `grpcio-reflection` at
`>=1.84.0`, `protobuf` at `>=7.35.1,<8`. That is the floor
`grpcio-reflection` 1.84 declares. v0.1.0 stays the grpcio 1.81 /
protobuf 6 tag for peers that have not moved yet.

## Develop

```bash
uv sync --group dev
just test
just gen    # after a signals-protocol commit and a gitlink bump
```

`devenv shell` provides `gh` for the GitHub release. `direnv` picks that up
from `.envrc`.

Regenerate the stubs with `just gen` after a `grpcio-tools` bump. The
generated modules record that tools version, and the protobuf modules record
the compiler version. A peer on an older grpcio or on protobuf 6 stays on
the v0.1.0 tag.
