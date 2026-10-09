"""Protocol snapshot shipped with this engine.

``zndx`` protos have one text: the ``signals-protocol`` submodule. This
package pins one commit of that repo and ships the tree, including the
Open Inference proto, plus the generated Python. A peer such as Semantica
depends on ``signals-engine`` and imports this snapshot. It does not vendor
its own ``signals-protocol`` checkout.

Evolve the wire in the submodule. Stay additive inside a version; a breaking
change is a new package. Commit that repo on ``trunk``, point this repo's
gitlink at the commit, and run ``just gen``. The stubs, the packaged protos,
and ``PROTOCOL_SHA`` move in the same engine commit. ``OIP_SHA256`` names
``inference/open_inference_grpc.proto`` inside that commit.
"""

from pathlib import Path

from signals_engine.protocol_stamp import OIP_SHA256, PROTOCOL_SHA

__all__ = ["OIP_SHA256", "PROTOCOL_SHA", "include_dirs", "proto_root"]


def proto_root() -> Path:
    """Directory suitable for ``protoc -I``. Contains ``zndx/`` and ``inference/``."""
    return Path(__file__).resolve().parent / "proto"


def include_dirs() -> list[str]:
    """Include paths for a host proto that imports this snapshot."""
    return [str(proto_root())]
