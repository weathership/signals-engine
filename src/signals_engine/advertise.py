"""Advertise host and public URL rewrite.

Loopback is never discovered. A host that sets ``PeerIdentity.advertise_host``
explicitly, including to a loopback name, is obeyed — that is how a single
machine test dials its own engine.
"""

from __future__ import annotations

import os
import socket
from urllib.parse import urlparse, urlunparse

from signals_engine.types import PeerIdentity, SurfaceSpec

_LOOPBACK = frozenset(
    {"", "localhost", "127.0.0.1", "0.0.0.0", "::1", "::", "[::1]", "[::]"}
)
_ENV_KEYS = (
    "SIGNALS_ADVERTISE_HOST",
    "SIGNALS_LATTICE_HOST",
    "SIGNALS_KRB_HOST",
)


def is_loopback_host(host: str) -> bool:
    text = (host or "").strip().strip("[]")
    if not text:
        return True
    if text.lower() in _LOOPBACK or text.startswith("127."):
        return True
    return False


def resolved_advertise_host() -> str:
    """LAN hostname. Empty when every candidate is loopback."""
    for key in _ENV_KEYS:
        raw = (os.environ.get(key) or "").strip()
        if not raw:
            continue
        host = raw.split("/")[-1].split(":")[0].strip("[]")
        if host and not is_loopback_host(host):
            return host
    try:
        fqdn = (socket.getfqdn() or "").strip()
        if fqdn and not is_loopback_host(fqdn) and "." in fqdn:
            return fqdn
        hostname = (socket.gethostname() or "").strip()
        if hostname and not is_loopback_host(hostname) and "." in hostname:
            return hostname
        if hostname and not is_loopback_host(hostname):
            zt = f"{hostname}.dev.vista.zndx.org"
            try:
                socket.getaddrinfo(zt, None)
                return zt
            except OSError:
                return hostname
        for info in socket.getaddrinfo(hostname or "localhost", None, socket.AF_INET):
            ip = info[4][0]
            if ip and not is_loopback_host(ip):
                return ip
    except OSError:
        return ""
    return ""


def advertise_host(identity: PeerIdentity) -> str:
    explicit = (identity.advertise_host or "").strip()
    if explicit:
        return explicit.strip("[]")
    return resolved_advertise_host()


def rewrite_public_url(url: str, identity: PeerIdentity) -> str:
    """Replace a loopback URL host with the advertise host. Other hosts stay."""
    host = advertise_host(identity)
    if not url or not host or is_loopback_host(host):
        return url
    parsed = urlparse(url)
    if not parsed.hostname or not is_loopback_host(parsed.hostname):
        return url
    netloc = f"{host}:{parsed.port}" if parsed.port else host
    return urlunparse(
        (
            parsed.scheme or "http",
            netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )


def default_surfaces(identity: PeerIdentity) -> list[SurfaceSpec]:
    out: list[SurfaceSpec] = []
    if identity.primary_ui:
        out.append(
            SurfaceSpec(
                kind="primary",
                url=rewrite_public_url(identity.primary_ui, identity),
                healthy=True,
            )
        )
    if identity.health_url:
        out.append(
            SurfaceSpec(
                kind="health",
                url=rewrite_public_url(identity.health_url, identity),
                healthy=True,
            )
        )
    return out
