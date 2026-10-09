"""Lab peer-contract ports.

The contract stores a port per project, not a host. Same-box peers are
``<advertise-host>:<port>``. Announced peers keep the host:port they sent.
``PeerHint.target`` is opaque — callers do not rewrite a foreign host onto
this machine (signals-protocol surfaces.md).
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from signals_engine.advertise import advertise_host
from signals_engine.types import PeerIdentity

_SKIP = frozenset({"service", "proto", "server_reflection", "first_party_clients"})
_DEFAULT = Path.home() / "local/src/wxs/signals/config/platform/peer-contract.json"


def resolve_contract(path: Path | None) -> Path | None:
    """Explicit path wins, including a missing file (then there is no contract)."""
    if path is not None:
        return path if path.is_file() else None
    env = (os.environ.get("SIGNALS_PEER_CONTRACT") or "").strip()
    if env:
        candidate = Path(env)
        return candidate if candidate.is_file() else None
    return _DEFAULT if _DEFAULT.is_file() else None


def configured_peers(
    identity: PeerIdentity,
    contract: Path | None,
) -> list[tuple[str, str]]:
    path = resolve_contract(contract)
    host = advertise_host(identity)
    if path is None or not host:
        return []
    try:
        doc = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return []
    lattice = doc.get("engine_grpc_lattice")
    if not isinstance(lattice, dict):
        return []
    out: list[tuple[str, str]] = []
    for name, port in lattice.items():
        if name in _SKIP or name == identity.project:
            continue
        if isinstance(port, bool) or not isinstance(port, int):
            continue
        out.append((str(name), f"{host}:{port}"))
    return out
