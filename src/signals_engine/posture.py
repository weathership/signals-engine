"""Source posture: what code this process is running.

``head`` is read live. ``running_sha`` is stamped once at serve(), so a
long-lived engine shows when the checkout moved under it. Untracked files
do not count as dirty. A value we cannot read stays empty — not invented.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from signals_engine.wire.zndx.engine.v1 import engine_pb2


def git_text(root: Path, *args: str) -> str:
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), *args],
            check=False,
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return ""
    if proc.returncode != 0:
        return ""
    return (proc.stdout or "").strip()


def _parse_submodule_line(line: str) -> tuple[str, str, str] | None:
    raw = line.rstrip("\n")
    if not raw.strip():
        return None
    if raw[0] in "+-U":
        prefix, body = raw[0], raw[1:].strip()
    else:
        prefix, body = " ", raw.strip()
    parts = body.split()
    if len(parts) < 2 or any(c not in "0123456789abcdef" for c in parts[0]):
        return None
    return prefix, parts[0], parts[1]


def _submodule_names(root: Path) -> dict[str, str]:
    """Map checkout path → submodule name from ``.gitmodules``."""
    text = git_text(root, "config", "-f", ".gitmodules", "--get-regexp", r"submodule\..*\.path")
    out: dict[str, str] = {}
    for line in text.splitlines():
        # submodule.components/signals-protocol.path components/signals-protocol
        bits = line.split(None, 1)
        if len(bits) != 2 or not bits[0].startswith("submodule.") or not bits[0].endswith(".path"):
            continue
        name = bits[0][len("submodule.") : -len(".path")]
        out[bits[1].strip()] = name
    return out


def build_source_posture(
    root: Path | None,
    *,
    project: str,
    running_sha: str,
    migrations: list[tuple[str, str, tuple[str, ...]]] | None = None,
) -> engine_pb2.SourcePosture:
    posture = engine_pb2.SourcePosture(project=project, running_sha=running_sha or "")
    if root is None or not root.is_dir():
        return posture
    head = git_text(root, "rev-parse", "HEAD")
    branch = git_text(root, "rev-parse", "--abbrev-ref", "HEAD")
    if branch == "HEAD":
        branch = ""
    upstream = git_text(root, "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    ahead = behind = 0
    if upstream:
        parts = git_text(root, "rev-list", "--left-right", "--count", f"{upstream}...HEAD").split()
        if len(parts) == 2 and all(part.isdigit() for part in parts):
            behind, ahead = int(parts[0]), int(parts[1])
    posture.checkout = str(root)
    posture.branch = branch
    posture.head = head
    posture.dirty = bool(git_text(root, "status", "--porcelain", "--untracked-files=no"))
    posture.upstream = upstream
    posture.ahead = ahead
    posture.behind = behind
    names = _submodule_names(root)
    for raw in git_text(root, "submodule", "status").splitlines():
        parsed = _parse_submodule_line(raw)
        if parsed is None:
            continue
        prefix, checked_out, path = parsed
        pinned = ""
        ls = git_text(root, "ls-tree", "HEAD", path).split()
        if len(ls) >= 3 and ls[1] == "commit":
            pinned = ls[2]
        dirty = False
        if prefix != "-":
            dirty = bool(git_text(root / path, "status", "--porcelain", "--untracked-files=no"))
        posture.submodules.add(
            path=path,
            name=names.get(path, path),
            pinned_sha=pinned,
            checked_out_sha=checked_out,
            dirty=dirty,
        )
    for source, current, unapplied in migrations or []:
        posture.migrations.add(source=source, current=current, unapplied=list(unapplied))
    return posture


def named_remotes(root: Path | None) -> list[tuple[str, str]]:
    if root is None or not root.is_dir():
        return []
    out: list[tuple[str, str]] = []
    for line in git_text(root, "remote", "-v").splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[-1] == "(fetch)":
            out.append((parts[0], parts[1]))
    return out


def head_sha(root: Path | None) -> str:
    if root is None:
        return ""
    return git_text(root, "rev-parse", "HEAD")
