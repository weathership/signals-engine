"""TTL'd peer directory.

Matrix homeservers push persistent PDUs and ephemeral EDUs. This roster is
the EDU idea only: a pairwise join hint, process-local, dropped when the TTL
elapses, never written to Atlas. It is not a Matrix ``/send`` transaction
and not room state. Launchers stay pull-only: they ``ServerQuery PEERS``
and then ``Status`` the targets (signals-protocol surfaces.md).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

DEFAULT_ANNOUNCE_TTL_S = 90
MIN_ANNOUNCE_TTL_S = 15
MAX_ANNOUNCE_TTL_S = 600


@dataclass(frozen=True)
class AnnouncedPeer:
    project: str
    target: str
    expires_at: float


def parse_engine_target(raw: str) -> str:
    """``host:port`` with a numeric port. Empty when the string is not a target."""
    addr = (raw or "").replace("grpc://", "").strip()
    if not addr:
        return ""
    host, sep, port = addr.rpartition(":")
    if not sep or not host or not port.isdigit():
        return ""
    return addr


def clamp_announce_ttl(raw: int) -> int:
    if raw <= 0:
        return DEFAULT_ANNOUNCE_TTL_S
    return max(MIN_ANNOUNCE_TTL_S, min(MAX_ANNOUNCE_TTL_S, int(raw)))


class Directory:
    """One engine's Announce table. Not process-global — two engines in one
    interpreter (tests, or a sidecar beside a product) do not share a roster."""

    def __init__(self, project: str) -> None:
        self.project = project
        self._lock = threading.Lock()
        self._rows: dict[str, AnnouncedPeer] = {}

    def remember(self, project: str, engine_target: str, ttl_seconds: int) -> tuple[bool, int, str]:
        name = (project or "").strip()
        if not name:
            return False, 0, "empty project"
        if name.lower() == self.project.lower():
            return False, 0, "self-announce"
        target = parse_engine_target(engine_target)
        if not target:
            return False, 0, "engine_target must be host:port"
        ttl = clamp_announce_ttl(ttl_seconds)
        with self._lock:
            self._rows[name.lower()] = AnnouncedPeer(
                project=name,
                target=target,
                expires_at=time.monotonic() + ttl,
            )
        return True, ttl, ""

    def peers(self) -> list[tuple[str, str]]:
        now = time.monotonic()
        with self._lock:
            expired = [key for key, row in self._rows.items() if row.expires_at <= now]
            for key in expired:
                del self._rows[key]
            return [(row.project, row.target) for row in self._rows.values()]
