"""Regenerate Python stubs from signals-protocol and the vendored OIP proto.

The protos are the specification. Generated modules live under
``signals_engine.wire`` so a host project does not collide with a local
``zndx`` or ``inference`` tree until it chooses to import these.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WIRE = ROOT / "src" / "signals_engine" / "wire"
PROTOCOL = ROOT / "components" / "signals-protocol" / "proto"
OIP = ROOT / "proto" / "inference" / "open_inference_grpc.proto"
ENGINE = PROTOCOL / "zndx" / "engine" / "v1" / "engine.proto"

# grpc_tools emits absolute package imports. The wire tree is one package.
_IMPORT_FIXES = (
    (
        WIRE / "zndx" / "engine" / "v1" / "engine_pb2_grpc.py",
        "from zndx.engine.v1 import engine_pb2 as",
        "from . import engine_pb2 as",
    ),
    (
        WIRE / "inference" / "open_inference_grpc_pb2_grpc.py",
        "from inference import open_inference_grpc_pb2 as",
        "from . import open_inference_grpc_pb2 as",
    ),
)


def _markers() -> None:
    for rel in (
        "",
        "zndx",
        "zndx/engine",
        "zndx/engine/v1",
        "inference",
    ):
        path = WIRE / rel / "__init__.py"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text('"""Generated wire package. Do not edit by hand."""\n')


def _fix_imports() -> None:
    for path, old, new in _IMPORT_FIXES:
        text = path.read_text()
        if old not in text:
            raise SystemExit(f"codegen: expected import not in {path}: {old!r}")
        path.write_text(text.replace(old, new, 1))


def main() -> None:
    if not ENGINE.is_file():
        raise SystemExit(f"codegen: missing {ENGINE} (signals-protocol submodule)")
    if not OIP.is_file():
        raise SystemExit(f"codegen: missing {OIP}")
    WIRE.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{PROTOCOL}",
        f"-I{ROOT / 'proto'}",
        f"--python_out={WIRE}",
        f"--grpc_python_out={WIRE}",
        f"--pyi_out={WIRE}",
        str(ENGINE),
        str(OIP),
    ]
    subprocess.run(cmd, check=True)
    _markers()
    _fix_imports()
    print(f"codegen: wrote {WIRE}/zndx/engine/v1 and {WIRE}/inference")


if __name__ == "__main__":
    main()
