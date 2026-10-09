"""Engine/ServerQuery.

Matrix federation Queries are one request and one response between a pair
of servers (``GET /_matrix/federation/v1/query/{queryType}`` — see
https://spec.matrix.org/latest/server-server-api/). They are not gossip and
not a persisted room DAG. ``ServerQuery`` is that pattern on gRPC: the kind
is the query type, an empty payload is an honest miss, and nothing here is
written to a lineage store.

We are not a Matrix homeserver. There is no federation HTTPS, no event
authorization, and no ``/send`` transaction.
"""

from __future__ import annotations

from signals_engine.advertise import default_surfaces
from signals_engine.contract import configured_peers
from signals_engine.directory import Directory
from signals_engine.posture import build_source_posture, head_sha, named_remotes
from signals_engine.types import EngineHooks, PeerIdentity, SurfaceSpec
from signals_engine.wire.zndx.engine.v1 import engine_pb2

_pb = engine_pb2


def _surfaces(identity: PeerIdentity, hooks: EngineHooks) -> list[SurfaceSpec]:
    if hooks.surfaces is not None:
        return list(hooks.surfaces())
    return default_surfaces(identity)


def fill_server_query(
    response: engine_pb2.ServerQueryResponse,
    request: engine_pb2.ServerQueryRequest,
    *,
    identity: PeerIdentity,
    hooks: EngineHooks,
    directory: Directory | None,
    repo_root,
    running_sha: str,
    contract,
) -> None:
    kind = int(request.kind or 0)
    response.project = identity.project
    if kind == _pb.SERVER_QUERY_KIND_REMOTES:
        response.head = head_sha(repo_root)
        for name, url in named_remotes(repo_root):
            response.remotes.add(name=name, url=url)
    elif kind == _pb.SERVER_QUERY_KIND_SURFACES:
        for surface in _surfaces(identity, hooks):
            response.surfaces.add(kind=surface.kind, url=surface.url, healthy=surface.healthy)
    elif kind == _pb.SERVER_QUERY_KIND_PEERS:
        seen: set[tuple[str, str]] = set()
        rows = list(configured_peers(identity, contract))
        if directory is not None:
            rows.extend(directory.peers())
        for project, target in rows:
            key = (project, target)
            if key in seen:
                continue
            seen.add(key)
            response.peers.add(project=project, target=target)
    elif kind == _pb.SERVER_QUERY_KIND_SOURCE_POSTURE:
        migrations = None
        if hooks.migrations is not None:
            migrations = [
                (row.source, row.current, tuple(row.unapplied)) for row in hooks.migrations()
            ]
        response.posture.CopyFrom(
            build_source_posture(
                repo_root,
                project=identity.project,
                running_sha=running_sha,
                migrations=migrations,
            )
        )
    extra = hooks.query.get(kind)
    if extra is not None:
        extra(request, response)
